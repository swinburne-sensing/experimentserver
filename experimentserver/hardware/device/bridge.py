import typing
import queue
from datetime import datetime, timedelta

from transitions import EventData

from ..base.serial import SerialStringHardware
from ..metadata import TYPE_PARAMETER_DICT
from ...data import TYPE_FIELD_DICT, MeasurementGroup, Measurement
from ...data.gas import get_gas

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class GasAlanyzer(SerialStringHardware):
    _ZERO_PERIOD = timedelta(seconds=60)

    _GAS_CHANNELS = [
        (get_gas('carbon-monoxide'), 0.01, 'percent'),
        (get_gas('hexane'), 1e-6, 'ppm'),
        (get_gas('carbon-dioxide'), 0.01, 'percent'),
        (get_gas('oxygen'), 0.01, 'percent'),
        (get_gas('nitric-oxides'), 1e-6, 'ppm')
    ]

    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(GasAlanyzer, self).__init__(identifier, port, parameters, {
            'baudrate': 9600
        })

        self._zero_timeout: typing.Optional[datetime] = None

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Bridge Exhaust Gas Analyzer'

    def command_data_start(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'!DA1\r')

    def command_data_stop(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'!DA0\r')

    def command_zero(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'!ZRO\r')

    def command_zero_calibration(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'!ZCL\r')

    def command_zero_fast(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'!ZFS\r')

    def command_gas_hexane(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'#GAS,2.1\r')

    def command_gas_propane(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'#GAS,2.2\r')

    def command_gas_methane(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'#GAS,2.3\r')

    def command_gas_query(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'?GAS,2.1\r')

    def command_serial_number_query(self):
        with self._serial_lock.lock():
            self._serial_port.write(b'?S/N\r')

    @SerialStringHardware.register_parameter(description='Trigger zero')
    def zero(self):
        self.get_logger().info('Zeroing', event=True)

        self.command_zero()

        self._zero_timeout = datetime.now()

    @SerialStringHardware.register_parameter(description='Trigger fast zero')
    def zero_fast(self):
        self.command_zero_fast()

        self._zero_timeout = datetime.now()

    @SerialStringHardware.register_measurement(description='Gas concentration', measurement_group=MeasurementGroup.GAS,
                                               force=True)
    def get_concentration(self) -> TYPE_FIELD_DICT:
        # Fetch measurements
        pass

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_connect(event)

        # Fetch serial number
        with self._serial_lock.lock():
            pass

    # Do nothing during transitions
    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GasAlanyzer, self).transition_configure(event)

    def transition_start(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_start(event)

        # Start acquisition
        self.command_data_start()

    def transition_stop(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Stop acquisition
        self.command_data_stop()

        super().transition_stop(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GasAlanyzer, self).transition_configure(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GasAlanyzer, self).transition_configure(event)

    def _handle_payload(self, payload: str, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        measurements = []

        # Generate tags
        zero_flag = self._zero_timeout is not None and datetime.now() <= (self._zero_timeout + self._ZERO_PERIOD)

        tags = {
            'mode': 'zero' if zero_flag else 'normal'
        }

        if payload.startswith('#DAT'):
            payload_fields = payload.split(',')

            if len(payload_fields) == 1 + len(self._GAS_CHANNELS) + 4:
                # Drop start field
                payload_fields.pop(0)

                # Parse gas concentrations
                for gas in self._GAS_CHANNELS:
                    # Parse float and apply scaling
                    concentration = float(payload_fields.pop(0)) * gas[1]

                    gas_tag = {
                        'label': gas[0].label,
                        'formula': gas[0].formula,
                        'scale': gas[2]
                    }

                    gas_tag.update(tags)

                    measurements.append(Measurement(self, MeasurementGroup.GAS, {
                        'concentration': concentration
                    }, received, gas_tag))

                # Parse additional metadata fields
                internal_temp = float(payload_fields.pop(0))

                measurements.append(Measurement(self, MeasurementGroup.TEMPERATURE, {
                    'internal': internal_temp
                }, received, tags))

                # Parse combustion data
                comb_lambda = float(payload_fields.pop(0))
                comb_afr = float(payload_fields.pop(0))
                comb_efficiency = float(payload_fields.pop(0)) * 1e-2

                measurements.append(Measurement(self, MeasurementGroup.COMBUSTION, {
                    'lambda': comb_lambda,
                    'afr': comb_afr,
                    'efficiency': comb_efficiency
                }, received, tags))
            else:
                self.get_logger().warning(f"Expected 10 fields in Bridge Analyzer data, received {len(payload_fields)}")

        return measurements
