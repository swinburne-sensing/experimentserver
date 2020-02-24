import time

import flask

import experimentserver
from experimentserver.data.measurement import Measurement, MeasurementSource, MeasurementTarget, TYPE_TAG_DICT, \
    MeasurementGroup
from experimentserver.config import ConfigManager
from experimentserver.experiment.procedure import Procedure
from experimentserver.experiment.control import ProcedureTransition
from experimentserver.hardware.manager import HardwareManager
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

    def __init__(self, config: ConfigManager, metadata: TYPE_TAG_DICT, procedure: Procedure):
        """

        :param config:
        :param metadata:
        :param procedure:
        """
        super().__init__()

        self._config = config
        self._metadata = metadata
        self._procedure = procedure

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
        return self._metadata

    def get_procedure(self) -> Procedure:
        return self._procedure

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
                self.get_logger().info('Web interface stopped')
                break

            if not self._procedure.is_thread_alive():
                self.get_logger().error('Experimental procedure stopped unexpectedly')
                break

            # Check all managers are still running
            for manager in HardwareManager.get_all_instances().values():
                if not manager.is_thread_alive():
                    self.get_logger().error(
                        f"Hardware manager for {manager.get_hardware().get_hardware_identifier()} crashed",
                        event=True, notify=True)
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
