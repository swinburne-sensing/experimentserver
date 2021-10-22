import enum
import re
import typing

from experimentlib.data.unit import registry, parse
from transitions import EventData
from pyvisa import constants

from ..error import CommunicationError, MeasurementUnavailable, MeasurementError
from ..base.scpi import SCPIHardware
from ..base.visa import VISAHardware, TYPE_ERROR
from ..base.enum import HardwareEnum, TYPE_ENUM_CAST
from experimentserver.measurement import Measurement, MeasurementGroup


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class HPMultimeterFunction(HardwareEnum):
    VOLTAGE_DC = enum.auto()
    VOLTAGE_AC = enum.auto()
    CURRENT_DC = enum.auto()
    CURRENT_AC = enum.auto()
    RESISTANCE_2WIRE = enum.auto()
    RESISTANCE_4WIRE = enum.auto()
    DIODE = enum.auto()
    FREQUENCY = enum.auto()
    PERIOD = enum.auto()

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.List[str]]]:
        return {
            cls.VOLTAGE_DC: ['v', 'volt', 'volts', 'voltage'],
            cls.CURRENT_DC: ['a', 'i', 'amp', 'amps', 'amperage'],
            cls.RESISTANCE_2WIRE: ['r', '2r', '2w', 'ohm', 'res', '2res', 'resistance'],
            cls.RESISTANCE_4WIRE: ['4r', '4w', '4w_ohm', '4w_res', 'kelvin']
        }

    @classmethod
    def _get_description_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.VOLTAGE_DC: 'Voltage DC',
            cls.VOLTAGE_AC: 'Voltage AC',
            cls.CURRENT_DC: 'Current DC',
            cls.CURRENT_AC: 'Current AC',
            cls.RESISTANCE_2WIRE: '2-Wire Resistance',
            cls.RESISTANCE_4WIRE: '4-Wire Resistance',
            cls.DIODE: 'Diode',
            cls.FREQUENCY: 'Frequency',
            cls.PERIOD: 'Period'
        }

    @classmethod
    def _get_command_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.VOLTAGE_DC: 'VOLT',
            cls.VOLTAGE_AC: 'VOLT:AC',
            cls.CURRENT_DC: 'CURR',
            cls.CURRENT_AC: 'VOLT:AC',
            cls.RESISTANCE_2WIRE: 'RES',
            cls.RESISTANCE_4WIRE: 'FRES',
            cls.DIODE: 'DIOD',
            cls.FREQUENCY: 'FREQ',
            cls.PERIOD: 'PER'
        }


class HP34401AMultimeter(SCPIHardware):
    _RE_ERROR = re.compile(r'([+-][0-9]+),"([^\"]+)"')

    def __init__(self, *args, **kwargs):
        """ Create Hardware instance for a HP 34401A Multimeter.

        :param args:
        :param kwargs:
        """
        super().__init__(*args, visa_open_args={
            'baud_rate': 9600,
            'flow_control': constants.VI_ASRL_FLOW_DTR_DSR,
            'read_termination': '\r\n',
            'write_termination': '\r\n'
        }, visa_timeout=5, visa_rate_limit=0.1, visa_delay=0.1, **kwargs)

        self._function = HPMultimeterFunction.VOLTAGE_DC

    @classmethod
    def scpi_display(cls, transaction: VISAHardware.VISATransaction, msg: typing.Optional[str] = None):
        # Clear display
        transaction.write('DISP:TEXT:CLE')

        if msg is not None:
            # Truncate long messages
            if len(msg) > 20:
                cls.logger().warning(f"Truncating message to 20 characters (original: {msg})")
                msg = msg[:20]

            transaction.write('DISP:TEXT {}', msg)

    def _scpi_reset_pre(self, transaction: VISAHardware.VISATransaction):
        self.sleep(0.1, 'flow control hack')

        transaction.flush()

        self.sleep(0.1, 'flow control hack')

    def _scpi_reset_post(self, transaction: VISAHardware.VISATransaction):
        # Delay before running next command
        self.sleep(1, 'visa reset')

        # Place in remote mode
        transaction.write('SYST:REM')

        self.sleep(0.1, 'remote switch')

    @classmethod
    def _get_visa_error(cls, transaction: VISAHardware.VISATransaction) -> typing.Optional[TYPE_ERROR]:
        error = transaction.query('SYST:ERR?')
        error_match = cls._RE_ERROR.search(error)

        if error_match is None:
            raise CommunicationError(f"Error message did not match expected format (response: {error})")

        error_code = int(error_match[1])

        if error_code == 0:
            return None
        else:
            return error_code, error_match[2]

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'HP 34401A Multimeter'

    def transition_disconnect(self, event: typing.Optional[EventData] = None):
        # Return to local control
        with self.visa_transaction() as transaction:
            transaction.write('SYST:LOC')

        super().transition_disconnect(event)

    @SCPIHardware.register_parameter(description='Configure measurement function')
    def set_mode(self, func: typing.Union[TYPE_ENUM_CAST, HPMultimeterFunction]):
        func = HPMultimeterFunction.from_input(func)

        self._function = func

        with self.visa_transaction() as transaction:
            transaction.write('CONF:{}', func)

            # Delay after mode switch
            self.sleep(0.05, 'mode switch')

    @SCPIHardware.register_parameter(description='Set number of power line cycles per measurement')
    def set_nplc(self, nplc: typing.Union[str, float]):
        with self.visa_transaction() as transaction:
            transaction.write(f"{{}}:NPLC {nplc}", self._function)

            # Delay after mode switch
            self.sleep(0.05, 'nplc switch')

    @SCPIHardware.register_measurement(description='Trigger and get reading', force=True)
    def get_reading(self) -> Measurement:
        with self.visa_transaction() as transaction:
            measurement_value = transaction.query('READ?')

        if self._function in (HPMultimeterFunction.VOLTAGE_DC, HPMultimeterFunction.VOLTAGE_AC,
                              HPMultimeterFunction.DIODE):
            return Measurement(self, MeasurementGroup.VOLTAGE, {
                'voltage': parse(measurement_value, registry.V)
            })
        elif self._function in (HPMultimeterFunction.CURRENT_DC, HPMultimeterFunction.CURRENT_AC):
            return Measurement(self, MeasurementGroup.CURRENT, {
                'current': parse(measurement_value, registry.A)
            })
        elif self._function in (HPMultimeterFunction.RESISTANCE_2WIRE, HPMultimeterFunction.RESISTANCE_4WIRE):
            resistance = parse(measurement_value, registry.ohm)

            if resistance.m_as('ohm') > 1e37:
                raise MeasurementUnavailable('Open circuit')

            return Measurement(self, MeasurementGroup.RESISTANCE, {
                'resistance': resistance
            })
        elif self._function == HPMultimeterFunction.FREQUENCY:
            return Measurement(self, MeasurementGroup.FREQUENCY, {
                'frequency': parse(measurement_value, registry.Hz)
            })
        else:
            raise MeasurementError(f"Unknown measurement for configured function {self._function!s}")

    def transition_connect(self, event: typing.Optional[EventData] = None):
        for attempt in range(3):
            # noinspection PyBroadException
            try:
                super().transition_connect(event)
                break
            except Exception:
                self.logger().exception(f"Exception on connect attempt {attempt + 1}")
