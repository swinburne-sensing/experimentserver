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

import experimentserver.util.thread
from experimentserver.versions import dependencies, python_version_tested
from experimentserver.config import ConfigManager
from experimentserver.data.database import setup_database
from experimentserver.data.export import dynamic_field_time_delta, add_tags, add_dynamic_field, exporter_remap
from experimentserver.observer import Observer
from experimentserver.server import main
from experimentserver.util.git import get_git_hash
from experimentserver.util.logging import get_logger


LOCK_FILENAME = './experimentserver.lock'


def __exit_handler(exit_logger):
    exit_logger.critical('Stopped abnormally', notify=True, event=True)


if __name__ == '__main__':
    root_path = os.path.dirname(os.path.abspath(__file__))
    config_default = os.path.abspath(os.path.join(root_path, '..', 'config/experiment.yaml'))

    time_startup = time.time()

    parser = argparse.ArgumentParser()

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

        root_logger.info(f"Command: {' '.join(sys.argv)}")

        # Log platform version
        root_logger.info("Runtime: python {}".format(sys.version.replace('\n', ' ')))
        root_logger.info(f"Interpreter: {sys.executable}")

        if all((sys.version_info[:len(version)] != version for version in python_version_tested)):
            root_logger.warning(f"This version of Python has not been tested! Tested versions: "
                                f"{', '.join(['.'.join(map(str, version)) for version in python_version_tested])}")

        # Log module version
        root_logger.info(f"Launching: {experimentserver.__app_name__} {experimentserver.__version__}")
        root_logger.info(f"Developers: {', '.join(experimentserver.__credits__)}")

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

        # Append CLI metadata
        for arg_tag in app_args.tag:
            arg_tag_split = arg_tag.split('=')

            if len(arg_tag_split) != 2:
                root_logger.warning(f"Invalid tag argument \"{arg_tag}\", should follow the format key=value")
                continue

            app_metadata[arg_tag_split[0]] = arg_tag_split[1]

        # Add global metadata
        add_tags(app_metadata)

        # Add startup dynamic field
        add_dynamic_field('time_delta_startup', dynamic_field_time_delta(time_startup))

        app_metadata_string = '\n'.join((f"    {k!s}: {v!s}" for k, v in app_metadata.items()))

        # Setup database connections
        for database_identifier, database_connect_args in app_config['database'].items():
            root_logger.info(f"Database connection: {database_identifier} (args: {database_connect_args})")
            db = setup_database(database_identifier, database_connect_args)

        # Setup database remapping
        for exporter_source, database_target in app_config['exporter'].items():
            exporter_remap(exporter_source, database_target)

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

            root_logger.info('Startup OK')

            # Run main application
            main(app_config, app_metadata)

            runtime = str(datetime.timedelta(seconds=time.time() - time_startup))
            root_logger.info(f"Stopped, total runtime: {runtime}", notify=True, event=True)
        except Exception as exc:
            root_logger.exception('Unhandled exception during runtime')
            raise
        finally:
            root_logger.debug('Unregistering exit handler')
            atexit.unregister(__exit_handler)

            # Stop observer
            observer.stop()

            root_logger.info(f"Observer stopped")

            # Remove lock file
            if os.path.isfile(LOCK_FILENAME):
                os.unlink(LOCK_FILENAME)
            else:
                root_logger.error('Lock file removed during run')
    finally:
        # Ask all threads to stop
        experimentserver.util.thread.stop_all()

        root_logger.info(f"Threads stopped")

        root_logger.info('Done')
