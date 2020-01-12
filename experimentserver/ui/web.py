import logging
import time
import typing
from datetime import datetime

import flask
from flask import request, jsonify
from transitions import MachineError

from experimentserver.data import MeasurementGroup
from experimentserver.data.measurement import Measurement, MeasurementSource, MeasurementTarget, TYPE_TAG_DICT
from experimentserver.config import ConfigManager
from experimentserver.hardware import Hardware
from experimentserver.hardware.error import HardwareError
from experimentserver.hardware.state.manager import StateManager
from experimentserver.hardware.state.transition import HardwareTransition
from experimentserver.util.module import get_all_subclasses
from experimentserver.util.thread import CallbackThread
from experimentserver.util.logging import LoggerObject, get_logger
from experimentserver.util.logging.event import EventBufferHandler


class WebUI(LoggerObject, MeasurementSource):
    """  """

    def __init__(self, config: ConfigManager, metadata: TYPE_TAG_DICT,
                 manager_list: typing.Dict[str, StateManager], procedure):
        """

        :param config:
        :param metadata:
        :param manager_list:
        """
        super().__init__()

        self._config = config
        self._metadata = metadata
        self._manager_list = manager_list
        self._procedure = procedure

        # Inject event buffer into logger
        self._event_buffer = EventBufferHandler(enable_filter=False)
        self._event_buffer.setLevel(logging.INFO)
        get_logger().addHandler(self._event_buffer)

        # Create flask instance
        self._app = flask.Flask(__name__)

        # Register methods
        self._register_json()
        self._register_page()

        # Thread for hosting
        self._thread = CallbackThread(name='WebUI', callback=self._app.run, callback_kwargs={
            'host': self._config.get('server.host'), 'port': self._config.get('server.port'),
            'debug': self._config.get('server.debug', default=True), 'use_reloader': False
        }, thread_daemon=True)

    def get_export_source_name(self) -> str:
        return 'server'

    def run(self):
        count = 0

        self._thread.start()

        while True:
            # Check web interface is still running
            if not self._thread.is_alive():
                self._logger.debug('Web interface stopped')
                return

            # Check all managers are still running
            for manager in self._manager_list.values():
                if not manager.is_alive():
                    self._logger.warning(
                        f"Hardware manager for {manager.get_hardware().get_hardware_identifier()} crashed",
                        event=True, notify=True)
                    return

            # Wait before checking again, break if wait is interrupted
            try:
                time.sleep(5)

                MeasurementTarget.record(Measurement(self, MeasurementGroup.STATUS,
                                                     {'status': 'running', 'count': count}))

                count += 1
            except KeyboardInterrupt:
                continue

    def _return_ok(self, msg):
        self._logger.info(msg)

        return jsonify({'success': True, 'message': msg})

    def _return_error(self, msg):
        self._logger.warning(msg)

        return jsonify({'success': False, 'message': msg})

    def _register_json(self):
        @self._app.route('/json/event')
        def json_event():
            records = []

            for record in self._event_buffer.get_events():
                records.insert(0, {
                    'time': datetime.fromtimestamp(record.created).isoformat(),
                    'level': record.levelname,
                    'thread': record.threadName,
                    'message': record.message
                })

            return jsonify(records)

        @self._app.route('/json/event/clear')
        def json_event_clear():
            self._event_buffer.clear()

            return self._return_ok('Events cleared')

        @self._app.route('/json/hardware/class')
        def json_hardware_class():
            hardware_class_list = []

            # Get list of hardware state managers
            for hardware_class in get_all_subclasses(Hardware):
                class_fqn = hardware_class.__module__ + '.' + hardware_class.__qualname__

                # Strip module name
                class_fqn = class_fqn.split('.')
                class_fqn = class_fqn[2:]
                class_fqn = '.'.join(class_fqn)

                try:
                    parameter_list = hardware_class.get_hardware_parameters()
                    measurement_list = hardware_class.get_hardware_measurements()

                    hardware_class_list.append({
                        'class': class_fqn,
                        'author': hardware_class.get_author(),
                        'description': hardware_class.get_hardware_description(),
                        'parameter': {k: v.description for k, v in parameter_list.items()
                                      if not k.endswith('hardware_measurement')},
                        'measurement': {k: v.description for k, v in measurement_list.items()}
                    })
                except NotImplementedError:
                    self._logger.warning(f"Hardware class {class_fqn} lacks authorship information")

            return jsonify(hardware_class_list)

        @self._app.route('/json/hardware/state')
        def json_hardware_state():
            manager_state = []

            # Get list of hardware state managers
            for identifier, manager in self._manager_list.items():
                manager_error = manager.get_error()

                manager_state.append({
                    'identifier': identifier,
                    'state': manager.get_state().value,
                    'message': 'None' if manager_error is None else str(manager_error)
                })

            return jsonify(manager_state)

        @self._app.route('/cmd/parameter', methods=['GET', 'POST'])
        def command_hardware_parameter():
            identifier = request.args.get('id')
            parameter = request.args.get('param')
            arguments = request.args.get('args')

            if identifier is None or parameter is None:
                return self._return_error('Missing identifier or parameter')

            if identifier not in self._manager_list:
                return self._return_error(f"Invalid hardware identifier: {identifier}")

            # Fetch hardware state
            manager = self._manager_list[identifier]

            manager.queue_parameter({
                parameter: arguments
            }, timeout=5)

            return self._return_ok('OK')

        @self._app.route('/cmd/shutdown')
        def command_shutdown():
            # Ask app thread to stop
            self._thread.stop()

            # Fetch server shutdown method
            shutdown_func = request.environ.get('werkzeug.server.shutdown')

            if shutdown_func is None:
                return self._return_error('Not running with the Werkzeug Server, shutdown request ignored')

            # Call shutdown method
            self._logger.info('Shutdown requested via web interface', event=True, notify=True)
            shutdown_func()

            return self._return_ok('Shutting down, this page may now be closed.')

        @self._app.route('/cmd/state', methods=['GET', 'POST'])
        def command_state():
            identifier = request.args.get('identifier')
            transition = request.args.get('transition')

            if identifier is None or transition is None:
                return self._return_error('Missing identifier or transition')

            if identifier not in self._manager_list:
                return self._return_error(f"Invalid hardware identifier: {identifier}")

            # Fetch hardware state
            manager = self._manager_list[identifier]

            try:
                transition = HardwareTransition(transition)
            except (KeyError, ValueError):
                return self._return_error(f"Invalid state transition: {transition}")

            try:
                # Attempt to process transition
                manager.queue_transition(transition)
            except HardwareError as exc:
                return self._return_error(f"Hardware error occurred during transition: {exc}")
            except MachineError as exc:
                return self._return_error(f"State machine error occurred during transition: {exc}")

            return self._return_ok(f"{manager.get_hardware().get_hardware_identifier()} transitioned to state "
                                   f"{manager.get_state().value}")

    def _register_page(self):
        @self._app.route('/')
        def page_index():
            return flask.render_template('index.html', metadata=self._metadata)

        @self._app.route('/debug/state')
        def page_state():
            manager_list = []

            # Get list of hardware state managers
            for identifier, manager in self._manager_list.items():
                parameter_list = manager.get_hardware().get_hardware_parameters()
                measurement_list = manager.get_hardware().get_hardware_measurements()

                manager_list.append({
                    'identifier': identifier,
                    'state': manager.get_state().value,
                    'parameter': parameter_list,
                    'measurement_group': measurement_list
                })

            return flask.render_template('state.html', metadata=self._metadata, managers=manager_list,
                                         transitions=[x.value for x in HardwareTransition])
