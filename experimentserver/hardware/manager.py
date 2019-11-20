import enum
import threading

from . import Hardware
from experimentserver.data.export import Exporter
from experimentserver.util.logging import LoggerObject


class HardwareState(enum.Enum):
    IDLE = 'idle'
    READY = 'ready'
    RUNNING = 'running'
    PAUSED = 'paused'
    ERROR = 'error'


class HardwareStateManager(LoggerObject):
    def __init__(self, hardware: Hardware, exporter: Exporter):
        super().__init__(logger_append=':' + hardware.get_hardware_identifier())

        # References to managed objects
        self._hardware = hardware
        self._exporter = exporter

        # Initial state
        self._state = HardwareState.IDLE

        # Thread for management operations
        self._thread = threading.Thread(name=self.__class__.__name__ + ':' + hardware.get_hardware_identifier(),
                                        target=self._thread_run)

    def change_state(self, target: HardwareState, wait: bool = False):
        pass

    def _thread_run(self):
        pass
