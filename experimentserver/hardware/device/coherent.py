import typing

from transitions import EventData

from ..base.serial import SerialHardware
from ...data import Measurement, MeasurementGroup, to_unit


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class FieldMasterGSPowerMeter(SerialHardware):
    def __init__(self, *args, termination: str = '\r\n', **kwargs):
        super(FieldMasterGSPowerMeter, self).__init__(*args, serial_args={
            'baudrate': 9600
        }, **kwargs)

        self._termination = termination

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Coherent FieldMaster GS Optical Power Meter'

    @SerialHardware.register_parameter(description='Configure wavelength')
    def set_wavelength(self, wavelength: float):
        wavelength = to_unit(wavelength, 'nm', magnitude=True)

        # Configure wavelength
        with self._serial_lock.lock():
            self._serial_port.write(f"wv {wavelength:e}{self._termination}".encode())

    @SerialHardware.register_measurement(description='Optical power', force=True)
    def get_power(self) -> Measurement:
        with self._serial_lock.lock():
            self._serial_port.write(f"pw?{self._termination}".encode())
            power = self._serial_port.read_until(self._termination.encode()).strip()

            self._serial_port.write(f"wv?{self._termination}".encode())
            wavelength = self._serial_port.read_until(self._termination.encode()).strip()

        power = to_unit(power, 'W')
        wavelength = to_unit(wavelength, 'nm')

        return Measurement(self, MeasurementGroup.POWER, {
            'power': power
        }, tags={
            'wavelength': wavelength
        })

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_connect(event)

        with self._serial_lock.lock():
            self._serial_port.write(f"*ind{self._termination}".encode())
            identity = self._serial_port.read_until(self._termination.encode()).strip()

            self._serial_port.write(f"v{self._termination}".encode())
            version = self._serial_port.read_until(self._termination.encode()).strip()

            self._serial_port.write(f"*dt?{self._termination}".encode())
            detector = self._serial_port.read_until(self._termination.encode()).strip()

        self.get_logger().info(f"System: {identity}")
        self.get_logger().info(f"Version: {version}")
        self.get_logger().info(f"Detector: {detector}")

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(FieldMasterPowerMeter, self).transition_configure(event)

        # Reset
        with self._serial_lock.lock():
            self._serial_port.write(f"*rst{self._termination}".encode())

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(FieldMasterPowerMeter, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(FieldMasterPowerMeter, self).transition_error(event)
