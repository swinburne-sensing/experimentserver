import typing

from experimentlib.data.unit import registry, parse
from transitions import EventData

from ..base.serial import SerialHardware
from experimentserver.measurement import Measurement, MeasurementGroup


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class FieldMasterGSPowerMeter(SerialHardware):
    def __init__(self, *args: typing.Any, termination: str = '\r\n', **kwargs: typing.Any):
        if 'serial_args' not in kwargs:
            kwargs['serial_args'] = {
                'baudrate': 9600
            }

        super(FieldMasterGSPowerMeter, self).__init__(
            *args,
            **kwargs
        )

        self._termination = termination

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Coherent FieldMaster GS Optical Power Meter'

    @SerialHardware.register_parameter(description='Configure wavelength')
    def set_wavelength(self, wavelength: float) -> None:
        wavelength = parse(wavelength, registry.nm).to(registry.m).magnitude

        # Configure wavelength
        with self._serial_lock.lock():
            self.serial_port.write(f"wv {wavelength:e}{self._termination}".encode())

    @SerialHardware.register_measurement(description='Optical power', force=True)
    def get_power(self) -> Measurement:
        with self._serial_lock.lock():
            self.serial_port.write(f"pw?{self._termination}".encode())
            power = self.serial_port.read_until(self._termination.encode()).decode().strip()

            self.serial_port.write(f"wv?{self._termination}".encode())
            wavelength = self.serial_port.read_until(self._termination.encode()).decode().strip()

        self.logger().debug(f"Read power: {power}")
        self.logger().debug(f"Read wavelength: {wavelength}")

        power = parse(power, registry.W)
        wavelength = parse(wavelength, registry.m)

        self.logger().debug(f"Parsed power: {power}")
        self.logger().debug(f"Parsed wavelength: {wavelength}")

        return Measurement(self, MeasurementGroup.POWER, {
            'power': power
        }, tags={
            'wavelength': f"{wavelength.to('nm').magnitude:g} nm"
        })

    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_connect(event)

        with self._serial_lock.lock(reason='connect'):
            self.serial_port.write(f"*ind{self._termination}".encode())
            identity = self.serial_port.read_until(self._termination.encode()).decode().strip()

            self.serial_port.write(f"v{self._termination}".encode())
            version = self.serial_port.read_until(self._termination.encode()).decode().strip()

            self.serial_port.write(f"*dt?{self._termination}".encode())
            detector = self.serial_port.read_until(self._termination.encode()).decode().strip()

        self.logger().info(f"System: {identity}")
        self.logger().info(f"Version: {version}")
        self.logger().info(f"Detector: {detector}")

    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_configure(event)

        # Reset
        with self._serial_lock.lock(reason='configure'):
            self.serial_port.write(f"*rst{self._termination}".encode())

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_error(event)
