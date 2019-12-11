import time
import typing
from datetime import datetime

import flask
from flask import request, jsonify

from experimentserver.data import MeasurementGroup
from experimentserver.data.export import ExporterSource, record_measurement, TYPING_TAG
from experimentserver.config import ConfigManager
from experimentserver.hardware.manager import Hardware, HardwareStateManager, HardwareStateException, \
    HardwareStateTransition
from experimentserver.util.module import get_all_subclasses
from experimentserver.util.thread import CallbackThread
from experimentserver.util.logging import LoggerObject, get_logger
from experimentserver.util.logging.event import EventBufferHandler


class WebUI(LoggerObject, ExporterSource):
    def __init__(self, config: ConfigManager, metadata: TYPING_TAG, manager_list: typing.Dict[str, HardwareStateManager]):
        super().__init__()

        self._config = config
        self._metadata = metadata
        self._manager_list = manager_list

        # Inject event buffer into logger
        self._event_buffer = EventBufferHandler(False)
        get_logger().addHandler(self._event_buffer)

        # Create flask instance
        self._app = flask.Flask(__name__)

        # self._app.add_url_rule('/favicon.ico', redirect_to=flask.url_for('static', filename='favicon.ico'))

        # Register methods
        self._register_json()
        self._register_route()

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

                record_measurement(datetime.now(), self, MeasurementGroup.STATUS, {'status': 'running', 'count': count})

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
                records.append({
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
                    parameter_list = hardware_class.get_parameter_meta()
                    measurement_list = hardware_class.get_measurement_meta()

                    hardware_class_list.append({
                        'class': class_fqn,
                        'author': hardware_class.get_author(),
                        'description': hardware_class.get_hardware_description(),
                        'parameter': {k: v['description'] for k, v in parameter_list.items()
                                      if not k.endswith('_measurement')},
                        'measurement': {k: v['description'] for k, v in measurement_list.items()}
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
            identifier = request.args.get('identifier')
            parameter = request.args.get('parameter')

            if identifier is None or parameter is None:
                return self._return_error('Missing identifier or parameter')

            if identifier not in self._manager_list:
                return self._return_error(f"Invalid hardware identifier: {identifier}")

            # Fetch hardware manager
            manager = self._manager_list[identifier]

            try:
                manager.get_hardware().set_parameter(parameter)
            except:
                pass

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

            # Fetch hardware manager
            manager = self._manager_list[identifier]

            try:
                transition = HardwareStateTransition(transition)
            except (KeyError, ValueError):
                return self._return_error(f"Invalid state transition: {transition}")

            try:
                # Attempt to process transition
                manager.queue_transition(transition)
            except HardwareStateException as exc:
                return self._return_error(f"Exception occurred during transition: {exc.__cause__}")

            return self._return_ok(f"{manager.get_hardware().get_hardware_identifier()} transitioned to state "
                                   f"{manager.get_state().value}")

    def _register_route(self):
        @self._app.route('/')
        def page_index():
            return flask.render_template('index.html', metadata=self._metadata)

        @self._app.route('/debug/state')
        def page_state():
            manager_list = []

            # Get list of hardware state managers
            for identifier, manager in self._manager_list.items():
                parameter_list = manager.get_hardware().get_parameter_meta()
                measurement_list = manager.get_hardware().get_measurement_meta()

                manager_list.append({
                    'identifier': identifier,
                    'state': manager.get_state().value,
                    'parameter': parameter_list,
                    'measurement': measurement_list
                })

            return flask.render_template('state.html', metadata=self._metadata, managers=manager_list,
                                         transitions=[x.value for x in HardwareStateTransition])
