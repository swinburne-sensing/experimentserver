#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import atexit
import getpass
import logging.config
import os
import platform
import socket
import threading
import time
import sys
from datetime import datetime

import experimentserver.util.thread
from experimentserver.versions import dependencies, python_version_tested
from experimentserver.config import ConfigManager
from experimentserver.database import setup_database
from experimentserver.data.measurement import Measurement, MeasurementTarget, dynamic_field_time_delta
from experimentserver.observer import Observer
from experimentserver.server import start_server
from experimentserver.util.git import get_git_hash
from experimentserver.util.logging import get_logger


# Timeout before ignoring running threads and exiting in release mode
THREAD_TIMEOUT = 60

# Lock file name, used to detect crashes
LOCK_FILENAME = './experimentserver.lock'


def __exit_handler(exit_logger):
    exit_logger.critical('Stopped abnormally', notify=True, event=True)


if __name__ == '__main__':
    config_default = os.path.abspath(os.path.join(experimentserver.ROOT_PATH, '..', 'config/experiment.yaml'))

    time_startup = datetime.now()

    parser = argparse.ArgumentParser(description=f"{experimentserver.__app_name__} {experimentserver.__version__}")

    parser.add_argument('config', default=[config_default], help='YAML configuration file', nargs='*')
    parser.add_argument('--debug', action='store_true', default=False, dest='debug', help='Enable debug mode')
    parser.add_argument('-t', '--tag', action='append', dest='tag', help='Additional metadata tags (key=value)')

    app_args = parser.parse_args()

    app_debug = app_args.debug

    # Try to load configuration
    try:
        app_config = ConfigManager()

        for config_file in app_args.config:
            config_file = os.path.abspath(config_file)
            print(f"Loading configuration: {config_file}")

            app_config.load(config_file)
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

        root_logger.info(f"Command: {' '.join(sys.argv)}")

        # Log platform version
        root_logger.info("Runtime: python {}".format(sys.version.replace('\n', ' ')))
        root_logger.info(f"Interpreter: {sys.executable}")
        root_logger.info(f"Platform: {platform.python_implementation()}")

        if all((sys.version_info[:len(version)] != version for version in python_version_tested)):
            root_logger.warning(f"This version of Python has not been tested! Tested versions: "
                                f"{', '.join(['.'.join(map(str, version)) for version in python_version_tested])}")

        if platform.python_implementation() != 'CPython':
            root_logger.warning(f"This python interpreter has not been tested! Tested interpreters: CPython")

        # Log module version
        root_logger.info(f"Launching: {experimentserver.__app_name__} {experimentserver.__version__}")
        root_logger.info(f"Developers: {', '.join(experimentserver.__credits__)}")

        # Dump identifiers
        server_version = list(map(int, experimentserver.__version__.split('.')))

        app_metadata = {
            'launch_hostname': socket.getfqdn(),
            'launch_username': getpass.getuser(),
            'launch_timestamp': time.time(),
            'launch_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'app_mode': 'debug' if app_debug else 'release',
            'app_name': experimentserver.__app_name__,
            'app_version': experimentserver.__version__,
            'app_version_major': server_version[0],
            'app_version_minor': server_version[1],
            'app_version_revision': server_version[2]
        }

        # Append CLI metadata
        if app_args.tag is not None:
            for arg_tag in app_args.tag:
                arg_tag_split = arg_tag.split('=')

                if len(arg_tag_split) != 2:
                    root_logger.warning(f"Invalid tag argument \"{arg_tag}\", should follow the format key=value")
                    continue

                app_metadata[arg_tag_split[0]] = arg_tag_split[1]

        # Add global metadata
        Measurement.add_tags(app_metadata)

        # Add startup dynamic field
        Measurement.add_dynamic_field('time_delta_launch', dynamic_field_time_delta(time_startup))

        # Setup database connections
        for database_identifier, database_connect_args in app_config['database'].items():
            root_logger.info(f"Database connection: {database_identifier} (args: {database_connect_args})")
            db = setup_database(database_identifier, database_connect_args)

        # Setup database remapping
        for exporter_source, database_target in app_config['remap'].items():
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
            with open(LOCK_FILENAME, 'w') as lock_file:
                lock_file.write('')

        # Create observer
        observer = Observer()

        root_logger.info('Initialization complete')

        try:
            observer.start()
            root_logger.debug(f"Observer running (pid: {observer.get_pid()})")

            root_logger.info('Startup OK')

            # If URL is available then provide it now
            url = None

            if 'url.grafana' in app_config:
                url = app_config.get('url.grafana').format(int(1000 * time_startup.timestamp()), 'now')
                root_logger.info(f"Runtime Grafana URL {url}", notify=True)

            # Run main application
            start_server(app_config, app_metadata)

            root_logger.info(f"Stopped, total runtime: {datetime.now() - time_startup!s}", notify=True, event=True)

            if url is not None:
                root_logger.info(f"Completed Grafana URL "
                                 f"{url.format(int(1000 * time_startup.timestamp()), int(1000 * time.time()))}", notify=True)
        finally:
            # Stop observer
            observer.stop()

            root_logger.info(f"Observer stopped")

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
            root_logger.info(f"Running thread: {thread!r}", notify=False, event=False)

        # Ask all threads to stop
        if app_debug:
            experimentserver.util.thread.stop_all()
        else:
            experimentserver.util.thread.stop_all(timeout=THREAD_TIMEOUT)

        root_logger.info(f"Threads stopped")

        for thread in threading.enumerate():
            root_logger.warning(f"Remaining thread: {thread!r}", notify=False, event=False)

        root_logger.info('Done')
