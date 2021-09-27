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
from datetime import datetime

import yaml

import experimentserver
import experimentserver.util.thread
from experimentserver.config import ConfigManager
from experimentserver.data.measurement import Measurement, MeasurementTarget, dynamic_field_time_delta
from experimentserver.database import setup_database
from experimentserver.hardware import Hardware, device, HardwareManager
from experimentserver.server import start_server
from experimentserver.util.constant import FORMAT_TIMESTAMP
from experimentserver.util.git import get_git_hash
from experimentserver.util.logging import get_logger
from experimentserver.util.logging.notify import PushoverNotificationHandler
from experimentserver.util.module import class_instance_from_dict
from experimentserver.util.uniqueid import hex_str
from experimentserver.versions import dependencies, python_version_tested

# Exit codes
EXIT_SUCCESS = 0
EXIT_ERROR_ARGUMENTS = -1


# Timeout before ignoring running threads and exiting in release mode
THREAD_TIMEOUT = 60

# Lock file name, used to detect crashes
LOCK_FILENAME = './experimentserver.lock'


REGEX_DATABASE_URL = re.compile(r'^http([s]?):\/\/([^:\/]+):?([0-9]*)(.*)$')


def __exit_handler(exit_logger):
    exit_logger.critical('Stopped abnormally', notify=True, event=True)


def main(debug: bool, enable_pushover: bool, config_paths: typing.List[str], cli_tags: typing.Dict[str, str],
         procedure_path: typing.Optional[str] = None) \
        -> typing.NoReturn:
    """ Launch the experimentserver application.

    :param debug: if True application will launch in debug mode
    :param enable_pushover: if True pushover notifications will be sent
    :param config_paths: path(s) to configuration file(s)
    :param cli_tags: dict containing additional metadata tags
    :param procedure_path:
    """
    time_startup = datetime.now()

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
            print(f"Loading configuration: {config_path}")

            app_config.load(config_path)
    except Exception:
        print("An error occurred while attempting to load application configuration.")
        raise

    # Get root logger
    root_logger = get_logger(experimentserver.__app_name__)

    # Wrap launch in a finally block to ensure all threads get to complete, even those created for logging
    try:
        # Configure logging
        logging.config.dictConfig(app_config['logging'])

        # Disable select loggers
        root_logger.disabled = False
        logging.getLogger("urllib3").setLevel(logging.INFO)
        logging.getLogger("pyvisa").setLevel(logging.INFO)
        logging.getLogger("transitions.core").setLevel(logging.INFO)
        logging.getLogger("pymodbus").setLevel(logging.INFO)

        # Setup pushover if enabled
        if enable_pushover:
            if 'pushover.api_token' in app_config:
                notify_title = app_config.get('pushover.title', default=experimentserver.__app_name__)

                notify_handler = PushoverNotificationHandler(notify_title, app_config.get('pushover.api_token', True),
                                                             app_config.get('pushover.user_key', True),
                                                             app_config.get('pushover.user_device'))

                root_logger.parent.addHandler(notify_handler)
            else:
                root_logger.info('No Pushover API key provided')

        # Log module version
        root_logger.info(f"Launching: {experimentserver.__app_name__} {experimentserver.__version__}")
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

        # Dump configuration
        root_logger.info(f"Configuration:\n{yaml.dump(app_config.dump())}")

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
            'launch_timestamp': time.time(),
            'launch_time': time.strftime(FORMAT_TIMESTAMP),
            'launch_mode': 'debug' if debug else 'release',
            'launch_uid': hex_str()
        })

        # Append CLI tags
        app_metadata.update(cli_tags)

        # Add global metadata
        Measurement.add_global_tags(app_metadata)

        # Add startup dynamic field
        Measurement.add_global_dynamic_field('time_delta_launch', dynamic_field_time_delta(time_startup))

        # Setup database connections
        for database_identifier, database_connect_args in app_config['database'].items():
            # Expand URL into individual settings
            if 'url' in database_connect_args:
                database_connect_url = database_connect_args.pop('url')

                database_connect_url_match = REGEX_DATABASE_URL.match(database_connect_url)

                if database_connect_url_match is None:
                    raise experimentserver.ApplicationException(f"Malformed database URL ({database_connect_url})")

                if len(database_connect_url_match[1]) > 0:
                    database_connect_args['ssl'] = True
                    database_connect_args['port'] = 443
                else:
                    database_connect_args['ssl'] = False
                    database_connect_args['port'] = 80

                if len(database_connect_url_match[3]) > 0:
                    database_connect_args['port'] = int(database_connect_url_match[3])

                database_connect_args['host'] = database_connect_url_match[2]
                database_connect_args['path'] = database_connect_url_match[4] or ''

                # Strip trailing slash
                if database_connect_args['path'].endswith('/'):
                    database_connect_args['path'] = database_connect_args['path'][:-1]

            if 'buffer_interval' in database_connect_args:
                buffer_interval = float(database_connect_args.pop('buffer_interval'))
            else:
                buffer_interval = None

            # Strip empty values
            database_connect_args = {k: v for k, v in database_connect_args.items() if v != ''}

            root_logger.info(f"Database connection: {database_identifier} (args: {database_connect_args})")
            setup_database(database_identifier, database_connect_args, buffer_interval)

        # Setup database remapping
        remap_config = app_config.get('remap', default={})

        if remap_config is not None:
            for exporter_source, database_target in remap_config.items():
                MeasurementTarget.measurement_target_remap(exporter_source, database_target)

        app_metadata_string = '\n'.join((f"    {k!s}: {v!s}" for k, v in app_metadata.items()))
        root_logger.info(f"Started\nMetadata:\n{app_metadata_string}", event=True, notify=True)

        # Append git hash if available
        try:
            git_hash = get_git_hash()

            if git_hash:
                root_logger.info(f"git hash: {git_hash}")
            else:
                root_logger.info('git commit hash not available')
        except OSError:
            root_logger.info('git not available')

        # Dump requirements versions
        for module_name, module_version in dependencies.items():
            root_logger.info(f"Library: {module_name} {module_version}")

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
                    hardware_inst = class_instance_from_dict(hardware_inst_config, device)

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
            url = None

            if 'url.grafana' in app_config and app_config['url.grafana'] is not None and \
                    len(app_config['url.grafana']) > 0:
                url = app_config.get('url.grafana').format(int(1000 * time_startup.timestamp()), 'now')
                root_logger.info(f"Runtime Grafana URL {url}", notify=True)

                user_metadata['grafana_url'] = url

            # Run main application
            start_server(app_config, app_metadata, user_metadata)

            root_logger.info(f"Stopped, total runtime: {datetime.now() - time_startup!s}", notify=True, event=True)

            if url is not None:
                root_logger.info(f"Completed Grafana URL "
                                 f"{url.format(int(1000 * time_startup.timestamp()), int(1000 * time.time()))}",
                                 notify=True)
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
            root_logger.warning(f"Remaining thread: {thread!r}", event=False)

        root_logger.info('Done')


""" A wrapper for the main function (mostly so we can exit if an argument error is found). """
config_default = os.path.abspath(os.path.join(experimentserver.APP_PATH, '..', 'config/experiment.yaml'))

parser = argparse.ArgumentParser(description=f"{experimentserver.__app_name__} {experimentserver.__version__}")

parser.add_argument('config', default=[config_default], help='YAML configuration file', nargs='*')
parser.add_argument('--debug', action='store_true', dest='debug', help='Enable debug mode')
parser.add_argument('--no-pushover', action='store_false', dest='pushover', help='Disable automatic Pushover '
                                                                                 'notifications')
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
            exit(EXIT_ERROR_ARGUMENTS)

        app_tags[arg_tag_split[0]] = arg_tag_split[1]

main(app_args.debug, app_args.pushover, app_args.config, app_tags, app_args.procedure)

exit(EXIT_SUCCESS)
