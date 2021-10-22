#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import atexit
import logging.config
import os
import platform
import re
import threading
import time
import typing
import sys

from yaml import dump
from experimentlib.file.git import get_git_hash, GitError
from experimentlib.logging import basic_logging, get_logger, shutdown, DEBUG, INFO
from experimentlib.util.classes import instance_from_dict
from experimentlib.util.time import now

import experimentserver
import experimentserver.util.thread
from experimentserver.config import ConfigManager
from experimentserver.measurement import Measurement, MeasurementTarget
from experimentserver.database import setup_database
from experimentserver.hardware import device
from experimentserver.hardware.base.core import Hardware
from experimentserver.hardware.manager import HardwareManager
from experimentserver.server import start_server
from experimentserver.versions import dependencies, python_version_tested


# Timeout before ignoring running threads and exiting in release mode
THREAD_TIMEOUT = 60

# Lock file name, used to detect crashes
LOCK_FILENAME = './experimentserver.lock'


REGEX_DATABASE_URL = re.compile(r'^http([s]?):\/\/([^:\/]+):?([0-9]*)(.*)$')


def __exit_handler(exit_logger):
    exit_logger.critical('Stopped abnormally', notify=True, event=True)


def main(debug: bool, config_paths: typing.List[str], cli_tags: typing.Dict[str, str],
         procedure_path: typing.Optional[str] = None) -> int:
    """ Launch the experimentserver application.

    :param debug: if True application will launch in debug mode
    :param config_paths: path(s) to configuration file(s)
    :param cli_tags: dict containing additional metadata tags
    :param procedure_path:
    """
    time_startup = now()

    # Initial logging setup
    basic_logging(level=DEBUG if debug else INFO)

    # Get root logger
    root_logger = get_logger(experimentserver.__app_name__)

    # Create config directory if it doesn't exist
    try:
        os.mkdir(experimentserver.CONFIG_PATH)
    except FileExistsError:
        pass

    # Try to load configuration
    try:
        app_config = ConfigManager()

        for config_path in config_paths:
            config_path = os.path.abspath(config_path)
            root_logger.info(f"Loading configuration: {config_path}")

            app_config.load(config_path)
    except Exception:
        print("An error occurred while attempting to load application configuration.")
        raise

    # Dump configuration
    if debug:
        root_logger.debug(f"Configuration:\n{dump(app_config.dump())}")

    # Wrap launch in a finally block to ensure all threads get to complete, even those created for logging
    try:
        # Configure logging
        root_logger.warning('Reconfiguring logging, if this hangs there may be a configuration issue')
        logging.config.dictConfig(app_config['logging'])

        # Log module version
        root_logger.info(f"Launching: {experimentserver.__app_name__} v{experimentserver.__version__}")
        root_logger.info(f"Developers: {', '.join(experimentserver.__credits__)}")

        root_logger.info(f"Command: {' '.join(sys.argv)}")

        # Log platform version
        root_logger.info("Runtime: python {}".format(sys.version.replace('\n', ' ')))
        root_logger.info(f"Interpreter: {sys.executable}")
        root_logger.info(f"Platform: {platform.python_implementation()}")
        root_logger.info(f"Path: {';'.join(sys.path)}")

        if all((sys.version_info[:len(version)] != version for version in python_version_tested)):
            root_logger.warning(f"This version of Python has not been tested! Tested versions: "
                                f"{', '.join(['.'.join(map(str, version)) for version in python_version_tested])}")

        if platform.python_implementation() != 'CPython':
            root_logger.warning(f"This python interpreter has not been tested! Tested interpreters: CPython")

        # If procedure is specified, add to the configuration
        if procedure_path is not None:
            procedure_path = os.path.abspath(procedure_path)

            if not os.path.isfile(procedure_path):
                raise experimentserver.ApplicationException(f"Specified procedure file \"{procedure_path}\" not a "
                                                            f"valid file")

            app_config.set('startup.procedure', procedure_path)
            root_logger.info(f"Configured startup procedure: {procedure_path}")

        # Setup fixed metadata
        app_metadata = experimentserver.config.get_system_metadata()

        app_metadata.update({
            'launch_mode': 'debug' if debug else 'release'
        })

        # Append CLI tags
        app_metadata.update(cli_tags)

        # Add global metadata
        Measurement.add_global_tags(app_metadata)

        # Setup database connections
        for database_identifier, database_connect_args in app_config['database'].items():
            # Strip empty values
            database_connect_args = {k: v for k, v in database_connect_args.items() if v != ''}

            root_logger.info(f"Database connection: {database_identifier} (args: {database_connect_args})")
            setup_database(database_identifier, database_connect_args, debug)

        # Setup database remapping
        remap_config = app_config.get('remap', default={})

        if remap_config is not None:
            for exporter_source, database_target in remap_config.items():
                MeasurementTarget.measurement_target_remap(exporter_source, database_target)

        app_metadata_string = '\n'.join((f"    {k!s}: {v!s}" for k, v in app_metadata.items()))
        root_logger.info(f"Startup\nGlobal metadata:\n{app_metadata_string}")

        # Append git hash if available
        try:
            git_hash = get_git_hash()

            if git_hash:
                root_logger.info(f"git hash: {git_hash}")
            else:
                root_logger.info('git commit hash not available')
        except GitError:
            root_logger.info('git not available')

        # Dump requirements versions
        for module_name, module_version in dependencies.items():
            root_logger.info(f"Module: {module_name} {module_version}")

        root_logger.debug('Registering exit handler')
        atexit.register(__exit_handler, root_logger)

        # Create lock file
        if os.path.isfile(LOCK_FILENAME):
            root_logger.warning('Process did not shutdown cleanly on previous run')
        else:
            try:
                with open(LOCK_FILENAME, 'w') as lock_file:
                    lock_file.write('')
            except PermissionError:
                root_logger.warning(f"Unable to write lock file {os.path.abspath(LOCK_FILENAME)}")

        # Hardware and manager instances
        hardware_list: typing.Dict[str, Hardware] = {}
        manager_list: typing.Dict[str, HardwareManager] = {}

        root_logger.info('Initialization complete')

        try:
            # Setup hardware instances and managers
            for hardware_inst_config in app_config.get('hardware', default=[]):
                root_logger.info("Hardware config: {}".format(hardware_inst_config))

                try:
                    # Create hardware object
                    hardware_inst = instance_from_dict(hardware_inst_config, device)

                    root_logger.info(f"Loaded: {hardware_inst} (author: {hardware_inst.get_author()})")

                    hardware_inst = typing.cast(Hardware, hardware_inst)

                    hardware_list[hardware_inst.get_hardware_identifier()] = hardware_inst

                    # Create and start manager
                    manager_inst = HardwareManager(hardware_inst)
                    manager_inst.thread_start()

                    manager_list[hardware_inst.get_hardware_identifier()] = manager_inst
                except Exception as exc:
                    raise experimentserver.ApplicationException(
                        f"Failed to initialise hardware using configuration: {hardware_inst_config!r}") \
                        from exc

            root_logger.info('Startup OK')

            user_metadata = {}

            # If URL is available then provide it now
            grafana_url = app_config.get('url.grafana')

            if grafana_url is not None and len(grafana_url) > 0:
                url = app_config.get('url.grafana').format(**{
                    'from': int(1000 * time_startup.timestamp()),
                    'to': 'now'
                })

                root_logger.info(f"Runtime Grafana URL {url}", notify=True)

                user_metadata['grafana_url'] = url

            # Run main application
            start_server(app_config, app_metadata, user_metadata)

            root_logger.info(f"Stopped, total runtime: {now() - time_startup!s}")

            if grafana_url is not None and len(grafana_url) > 0:
                url = app_config.get('url.grafana').format(**{
                    'from': int(1000 * time_startup.timestamp()),
                    'to': int(1000 * time.time())
                })

                root_logger.info(f"Completed Grafana URL {url}", notify=True)

            return experimentserver.EXIT_SUCCESS
        finally:
            # Cleanup managers
            for manager_inst in manager_list.values():
                manager_inst.thread_stop()

            root_logger.debug('Unregistering exit handler')
            atexit.unregister(__exit_handler)

            # Remove lock file
            if os.path.isfile(LOCK_FILENAME):
                os.unlink(LOCK_FILENAME)
            else:
                root_logger.error('Lock file removed during run')
    except experimentserver.ApplicationException as exc:
        root_logger.exception(f"Application exception: {exc!s}")
        raise
    except Exception as exc:
        root_logger.exception(f"Unhandled exception: {exc!s}")
        raise
    finally:
        for thread in threading.enumerate():
            root_logger.info(f"Running thread: {thread!r}")

        # Ask all threads to stop
        if debug:
            experimentserver.util.thread.stop_all()
        else:
            experimentserver.util.thread.stop_all(timeout=THREAD_TIMEOUT)

        root_logger.info(f"Threads stopped")

        for thread in threading.enumerate():
            root_logger.warning(f"Remaining thread: {thread!r}")

        root_logger.info('Stopped, wait for 5 secs to allow threads to wrap up')
        time.sleep(5)
        shutdown()


""" A wrapper for the main function (mostly so we can exit if an argument error is found). """
config_default = os.path.abspath(os.path.join(experimentserver.APP_PATH, '..', 'config/experiment.yaml'))

parser = argparse.ArgumentParser(description=f"{experimentserver.__app_name__} {experimentserver.__version__}")

parser.add_argument('config', default=[config_default], help='YAML configuration file', nargs='*')
parser.add_argument('--debug', action='store_true', dest='debug', help='Enable debug mode')
parser.add_argument('-t', '--tag', action='append', dest='tag', help='Additional metadata ags (key=value)')

parser.add_argument('-p', '--procedure', dest='procedure', help='Load and validate specified procedure on startup')

app_args = parser.parse_args()

# Append CLI app_metadata
app_tags = {}

if app_args.tag is not None:
    for arg_tag in app_args.tag:
        arg_tag_split = arg_tag.split('=')

        if len(arg_tag_split) != 2:
            print(f"Invalid tag argument \"{arg_tag}\", should follow the format key=value")
            exit(experimentserver.EXIT_ERROR_ARGUMENTS)

        app_tags[arg_tag_split[0]] = arg_tag_split[1]

try:
    exit(main(app_args.debug, app_args.config, app_tags, app_args.procedure))
except Exception:
    raise
finally:
    exit(experimentserver.EXIT_ERROR_EXCEPTION)
