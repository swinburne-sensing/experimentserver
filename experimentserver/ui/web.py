import time

import flask
from experimentlib.logging import get_logger, INFO
from experimentlib.logging.classes import LoggedAbstract
from experimentlib.logging.filters import only_event_factory
from experimentlib.logging.handlers import BufferedHandler

import experimentserver
from experimentserver.data.measurement import MeasurementSource, TYPE_TAG_DICT, MeasurementTarget, Measurement, \
    MeasurementGroup
from experimentserver.config import ConfigManager
from experimentserver.hardware.manager import HardwareError, HardwareManager
from experimentserver.protocol import Procedure, ProcedureLoadError, ProcedureTransition, ProcedureState
from experimentserver.util.thread import CallbackThread


# Suppress werkzeug logging for status updates
def _filter_werkzeug(record):
    return '/server/state' not in record.msg


_werkzeug_logger = get_logger('werkzeug')
_werkzeug_logger.addFilter(_filter_werkzeug)


class UserInterfaceError(experimentserver.ApplicationException):
    pass


class WebServer(LoggedAbstract, MeasurementSource):
    """  """

    def __init__(self, config: ConfigManager, app_metadata: TYPE_TAG_DICT, user_metadata: TYPE_TAG_DICT):
        """

        :param config:
        :param app_metadata:
        :param user_metadata:
        """
        LoggedAbstract.__init__(self)
        MeasurementSource.__init__(self)

        self._config = config
        self._app_metadata = app_metadata
        self._user_metadata = user_metadata

        # Initial procedure using default configuration
        procedure_metadata = config.get('procedure_metadata', default=ConfigManager())

        self._procedure = Procedure(metadata=procedure_metadata, stages=[])
        self._procedure.thread_start()

        # Inject event buffer into logger
        self._event_buffer = BufferedHandler()
        self._event_buffer.addFilter(only_event_factory())

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
            raise ProcedureLoadError('Procedure currently validated or running, stop before attempting import')

        self.logger().info(f"Unloaded procedure {self._procedure.get_uid()}")

        # Update procedure
        self._procedure = procedure
        self._procedure.thread_start()

        self.logger().info(f"Loaded procedure {self._procedure.get_uid()}")

    def get_event_buffer(self) -> BufferedHandler:
        return self._event_buffer

    def get_thread(self):
        return self._thread

    def run(self):
        self._thread.thread_start()

        self.logger().info(f"{experimentserver.__app_name__} {experimentserver.__version__} ready", notify=True)

        # Load startup procedure if specified
        startup_procedure_path = self._config.get('startup.procedure')

        if startup_procedure_path is not None:
            with open(startup_procedure_path, 'r') as startup_procedure_file:
                startup_procedure = Procedure.procedure_import(startup_procedure_file.read())

            self.set_procedure(startup_procedure)

            # Queue validation
            self.get_procedure().queue_transition(ProcedureTransition.VALIDATE)

        while True:
            # Check web interface is still running
            if not self._thread.is_thread_alive():
                self.logger().info('Web interface thread stopped')
                break

            if not self._procedure.is_thread_alive():
                self.logger().error('Procedure thread stopped unexpectedly')
                break

            # Check all managers are still running
            run = True

            for manager in HardwareManager.get_all_instances().values():
                try:
                    manager.check_watchdog()
                except HardwareError:
                    self.logger().error(
                        f"Hardware manager for {manager.get_hardware().get_hardware_identifier()} crashed")

                    run = False

                    break

            if not run:
                break

            # Wait before checking again, break if wait is interrupted
            try:
                time.sleep(5)

                procedure_state = self._procedure.get_state()

                if procedure_state in (ProcedureState.RUNNING, ProcedureState.PAUSED):
                    MeasurementTarget.record(
                        Measurement(
                            self,
                            MeasurementGroup.STATUS,
                            {
                                'state': procedure_state.name
                            }
                        )
                    )
            except KeyboardInterrupt:
                break

        # Attempt to stop procedure if running
        self._procedure.queue_transition(ProcedureTransition.STOP, raise_exception=False)
