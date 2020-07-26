import time

import flask

import experimentserver
from experimentserver.data.measurement import Measurement, MeasurementSource, MeasurementTarget, TYPE_TAG_DICT, \
    MeasurementGroup
from experimentserver.config import ConfigManager
from experimentserver.hardware.manager import HardwareError, HardwareManager
from experimentserver.protocol import Delay, Procedure, ProcedureConfigurationError, ProcedureTransition, Setup
from experimentserver.util.thread import CallbackThread
from experimentserver.util.logging import LoggerObject, get_logger, INFO
from experimentserver.util.logging.event import EventBufferHandler


# Suppress werkzeug logging for status updates
def _filter_werkzeug(record):
    return '/server/state' not in record.msg


_werkzeug_logger = get_logger('werkzeug')
_werkzeug_logger.addFilter(_filter_werkzeug)


class UserInterfaceError(experimentserver.ApplicationException):
    pass


class WebServer(LoggerObject, MeasurementSource):
    """  """

    def __init__(self, config: ConfigManager, app_metadata: TYPE_TAG_DICT, user_metadata: TYPE_TAG_DICT):
        """

        :param config:
        :param app_metadata:
        :param user_metadata:
        """
        super().__init__()

        self._config = config
        self._app_metadata = app_metadata
        self._user_metadata = user_metadata

        # Initial procedure using default configuration
        procedure_config = config.get('procedure', default=ConfigManager())

        self._procedure = Procedure(config=procedure_config)
        self._procedure.thread_start()

        # Inject event buffer into logger
        self._event_buffer = EventBufferHandler(enable_filter=False)
        self._event_buffer.setLevel(INFO)
        get_logger().addHandler(self._event_buffer)

        # Create flask instance
        self.app = flask.Flask(__name__)

        @self.app.route('/ping')
        def ping():
            return ''

        # Thread for hosting
        self._thread = CallbackThread(name='WebUI', callback=self.app.run, callback_kwargs={
            'host': self._config.get('server.host'), 'port': self._config.get('server.port'),
            'debug': self._config.get('server.debug', default=True), 'use_reloader': False
        }, thread_daemon=True)

    def get_export_source_name(self) -> str:
        return 'server'

    def get_config(self):
        return self._config

    def get_metadata(self):
        metadata = self._app_metadata
        metadata.update(self._user_metadata)

        return metadata

    def get_procedure(self) -> Procedure:
        return self._procedure

    def set_procedure(self, procedure: Procedure):
        if self._procedure.get_state().is_valid():
            raise ProcedureConfigurationError('Procedure currently validated or running, stop before attempting import')

        self.get_logger().info(f"Unloaded procedure {self._procedure.get_uid()}", event=True)

        # Update procedure
        self._procedure = procedure
        self._procedure.thread_start()

        self.get_logger().info(f"Loaded procedure {self._procedure.get_uid()}", event=True)

    def get_event_buffer(self):
        return self._event_buffer

    def get_thread(self):
        return self._thread

    def run(self):
        count = 0

        self._thread.thread_start()

        self.get_logger().info(f"{experimentserver.__app_name__} {experimentserver.__version__} ready", notify=True)

        while True:
            # Check web interface is still running
            if not self._thread.is_thread_alive():
                self.get_logger().info('Web interface thread stopped')

                break

            if not self._procedure.is_thread_alive():
                self.get_logger().error('Procedure thread stopped unexpectedly')

                break

            # Check all managers are still running
            run = True

            for manager in HardwareManager.get_all_instances().values():
                try:
                    manager.check_watchdog()
                except HardwareError:
                    self.get_logger().error(
                        f"Hardware manager for {manager.get_hardware().get_hardware_identifier()} crashed")

                    run = False

                    break

            if not run:
                break

            # Wait before checking again, break if wait is interrupted
            try:
                time.sleep(5)

                MeasurementTarget.record(Measurement(self, MeasurementGroup.STATUS, {'status': 'running',
                                                                                     'count': count}))

                count += 1
            except KeyboardInterrupt:
                break

        # Attempt to stop procedure if running
        self._procedure.queue_transition(ProcedureTransition.STOP, raise_exception=False)


