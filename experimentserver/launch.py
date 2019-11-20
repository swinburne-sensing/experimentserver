#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import atexit
import datetime
import getpass
import logging.config
import os
import socket
import time
import sys

import experimentserver
from experimentserver.config import ConfigManager
from experimentserver.data.database import setup_database
from experimentserver.data.export import dynamic_field_time_delta
from experimentserver.observer import Observer
from experimentserver.server import main
from experimentserver.util.git import get_git_hash
from experimentserver.util.logging import get_logger
from experimentserver.util.thread import QueueThread


LOCK_FILENAME = './experimentserver.lock'


def __exit_handler(exit_logger):
    exit_logger.critical('Stopped abnormally', notify=True, event=True)


if __name__ == '__main__':
    time_startup = time.time()

    parser = argparse.ArgumentParser()

    parser.add_argument('config', default=['./config/experiment.yaml'], help='YAML configuration file', nargs='*')
    parser.add_argument('--debug', action='store_true', default=False, dest='debug', help='Enable debug mode')

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

    # Wrap launch in a finally block to ensure all threads get to complete, even those created for logging
    try:
        # Configure logging
        logging.config.dictConfig(app_config['logging'])

        # Get root logger
        root_logger = get_logger(experimentserver.__app_name__)

        root_logger.info("Command: {}".format(' '.join(sys.argv)))

        # Log platform version
        root_logger.info("Runtime: python {}".format(sys.version.replace('\n', ' ')))

        if all((sys.version_info[:len(version)] != version for version in experimentserver.python_version_tested)):
            root_logger.warning("This version of Python has not been tested! Tested versions: {}".format(
                ', '.join(('.'.join([str(x) for x in version])) for version in experimentserver.python_version_tested)
            ))

        # Dump identifiers
        app_metadata = {
            'hostname': socket.getfqdn(),
            'username': getpass.getuser(),
            'startup_timestamp': time.time(),
            'startup_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'server_name': experimentserver.__app_name__,
            'server_version': experimentserver.__version__,
            'mode': 'debug' if app_debug else 'release'
        }

        app_metadata_string = '\n'.join((f"    {k!s}: {v!s}" for k, v in app_metadata.items()))

        root_logger.info(f"Started\nMetadata:\n{app_metadata_string}", notify=True)

        root_logger.info(f"System hostname: {app_metadata['hostname']}")
        root_logger.info(f"System username: {app_metadata['username']}")

        # Log module version
        root_logger.info(f"Launching: {experimentserver.__app_name__} {experimentserver.__version__}")
        root_logger.info(f"Developers: {', '.join(experimentserver.__credits__)}")

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
        for module_name, module_version in experimentserver.__dependencies__.items():
            root_logger.info(f"Library: {module_name} {module_version}")

        # Launch application
        root_logger.debug('Registering exit handler')
        atexit.register(__exit_handler, root_logger)

        # Create lock file
        if os.path.isfile(LOCK_FILENAME):
            root_logger.error('Process did not shutdown cleanly on previous run')
        else:
            with open(LOCK_FILENAME, 'w') as lock_file:
                lock_file.write('')

        # Create observer
        observer = Observer()

        root_logger.info('Initialization complete')

        try:
            observer.start()
            root_logger.debug(f"Observer running (pid: {observer.get_pid()})")

            # Setup database connections
            for database_identifier, database_connect_args in app_config['database'].items():
                root_logger.info(f"Database connection: {database_identifier} (args: {database_connect_args})")
                db = setup_database(database_identifier, database_connect_args)

                # Add global metadata to database client
                db.add_tags(app_metadata)

                # Add startup dynamic field
                db.add_dynamic_field('time_delta_startup', dynamic_field_time_delta(time_startup))

            root_logger.info('Startup OK', event=True)

            main(app_config, app_metadata)

            runtime = str(datetime.timedelta(seconds=time.time() - time_startup))
            root_logger.info(f"Stopped cleanly, total runtime: {runtime}", notify=True, event=True)
        except Exception as exc:
            root_logger.exception('Unhandled exception during runtime')
            raise
        finally:
            root_logger.debug('Unregistering exit handler')
            atexit.unregister(__exit_handler)

            root_logger.info(f"Threads stopped")

            # Stop observer
            observer.stop()

            # Remove lock file
            if os.path.isfile(LOCK_FILENAME):
                os.unlink(LOCK_FILENAME)
            else:
                root_logger.error('Lock file removed during run')
    finally:
        # Ask all threads to stop
        QueueThread.stop_all()
