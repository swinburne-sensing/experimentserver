import typing
from datetime import datetime, timedelta

from experimentlib.data.gas import GasProperties, registry
from experimentlib.util.time import now
from transitions import EventData

from ..base.serial import SerialStringHardware
from ..metadata import TYPE_PARAMETER_DICT
from experimentserver.measurement import MeasurementGroup, Measurement

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class GasAlanyzer(SerialStringHardware):
    _ZERO_PERIOD = timedelta(seconds=60)

    _CHANNEL_PROPERTIES: typing.List[typing.Tuple[GasProperties, float, str]] = [
        (registry['carbon-monoxide'], 0.01, 'pct'),
        (registry['hexane'], 1e-6, 'ppm'),
        (registry['carbon-dioxide'], 0.01, 'pct'),
        (registry['oxygen'], 0.01, 'pct'),
        (registry['nitric-oxides'], 1e-6, 'ppm')
    ]

    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(GasAlanyzer, self).__init__(identifier, port, parameters, {
            'baudrate': 9600
        })

        self._zero_timeout: typing.Optional[datetime] = None

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Bridge Exhaust Gas Analyzer'

    def command_data_start(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'!DA1\r')

    def command_data_stop(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'!DA0\r')

    def command_zero(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'!ZRO\r')

    def command_zero_calibration(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'!ZCL\r')

    def command_zero_fast(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'!ZFS\r')

    def command_gas_hexane(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'#GAS,2.1\r')

    def command_gas_propane(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'#GAS,2.2\r')

    def command_gas_methane(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'#GAS,2.3\r')

    def command_gas_query(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'?GAS,2.1\r')

    def command_serial_number_query(self) -> None:
        with self._serial_lock.lock():
            self.serial_port.write(b'?S/N\r')

    @SerialStringHardware.register_parameter(description='Trigger zero')
    def zero(self) -> None:
        self.logger().info('Zeroing', event=True)

        self.command_zero()

        self._zero_timeout = now()

    @SerialStringHardware.register_parameter(description='Trigger fast zero')
    def zero_fast(self) -> None:
        self.command_zero_fast()

        self._zero_timeout = now()

    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_connect(event)

    # Do nothing during transitions
    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
        super(GasAlanyzer, self).transition_configure(event)

    def transition_start(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_start(event)

        # Start acquisition
        self.command_data_start()

    def transition_stop(self, event: typing.Optional[EventData] = None) -> None:
        # Stop acquisition
        self.command_data_stop()

        super().transition_stop(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        super(GasAlanyzer, self).transition_configure(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        super(GasAlanyzer, self).transition_configure(event)

    def _handle_payload(self, payload: str, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        measurements = []

        # Generate tags
        zero_flag = self._zero_timeout is not None and now() <= (self._zero_timeout + self._ZERO_PERIOD)

        tags = {
            'sensor_operation': 'zero' if zero_flag else 'normal'
        }

        if payload.startswith('#DAT'):
            payload_fields = payload.split(',')

            if len(payload_fields) == 1 + len(self._CHANNEL_PROPERTIES) + 4:
                # Drop start field
                payload_fields.pop(0)

                # Parse gas concentrations
                for channel_prop in self._CHANNEL_PROPERTIES:
                    # Parse float and apply scaling
                    concentration = float(payload_fields.pop(0)) * channel_prop[1]

                    gas_tag = {
                        'gas_label': channel_prop[0].name,
                        'gas_formula': channel_prop[0].symbol,
                        'sensor_scale': channel_prop[2]
                    }

                    gas_tag.update(tags)

                    measurements.append(Measurement(self, MeasurementGroup.GAS, {
                        'concentration': concentration
                    }, received, tags=gas_tag))

                # Parse additional metadata fields
                internal_temp = float(payload_fields.pop(0))

                measurements.append(Measurement(self, MeasurementGroup.TEMPERATURE, {
                    'internal': internal_temp
                }, received, tags=tags))

                # Parse combustion data
                comb_lambda = float(payload_fields.pop(0))
                comb_afr = float(payload_fields.pop(0))
                comb_efficiency = float(payload_fields.pop(0)) * 1e-2

                measurements.append(Measurement(self, MeasurementGroup.COMBUSTION, {
                    'lambda': comb_lambda,
                    'afr': comb_afr,
                    'efficiency': comb_efficiency
                }, received, tags=tags))
            else:
                self.logger().warning(f"Expected 10 fields in Bridge Analyzer data, received {len(payload_fields)}")

        return measurements
