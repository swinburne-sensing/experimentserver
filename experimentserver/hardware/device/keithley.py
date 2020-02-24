import abc
import enum
import re
import typing

from transitions import EventData

from experimentserver.data.unit import to_unit
from experimentserver.data.measurement import TYPE_FIELD_DICT, TYPE_MEASUREMENT_LIST, MeasurementGroup
from ..error import CommunicationError, MeasurementUnavailable
from ..base.scpi import SCPIHardware
from ..base.visa import VISAHardware, TYPE_ERROR
from ..base.enum import HardwareEnum

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class DAQChannelFunction(HardwareEnum):
    VOLTAGE_DC = enum.auto()
    VOLTAGE_AC = enum.auto()
    CURRENT_DC = enum.auto()
    CURRENT_AC = enum.auto()
    RESISTANCE_2WIRE = enum.auto()
    RESISTANCE_4WIRE = enum.auto()
    DIODE = enum.auto()
    CAPACITANCE = enum.auto()
    TEMPERATURE = enum.auto()
    FREQUENCY = enum.auto()
    PERIOD = enum.auto()

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.List[str]]]:
        return {
            cls.VOLTAGE_DC: ['v', 'volt', 'volts', 'voltage'],
            cls.CURRENT_DC: ['a', 'i', 'amp', 'amps', 'amperage'],
            cls.RESISTANCE_2WIRE: ['r', 'ohm', 'res', 'resistance']
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
            cls.CAPACITANCE: 'Capacitance',
            cls.TEMPERATURE: 'Temperature',
            cls.FREQUENCY: 'Frequency',
            cls.PERIOD: 'Period'
        }

    @classmethod
    def _get_command_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.VOLTAGE_DC: '\"VOLT\"',
            cls.VOLTAGE_AC: '\"VOLT:AC\"',
            cls.CURRENT_DC: '\"CURR\"',
            cls.CURRENT_AC: '\"VOLT:AC\"',
            cls.RESISTANCE_2WIRE: '\"RES\"',
            cls.RESISTANCE_4WIRE: '\"FRES\"',
            cls.DIODE: '\"DIOD\"',
            cls.CAPACITANCE: '\"CAP\"',
            cls.TEMPERATURE: '\"TEMP\"',
            cls.FREQUENCY: '\"FREQ\"',
            cls.PERIOD: '\"PER\"'
        }


class _KeithleyInstrument(SCPIHardware, metaclass=abc.ABCMeta):
    """  """

    _RE_ERROR = re.compile(r'([0-9]+),"([^\"]+)"')

    def __init__(self, *args, **kwargs):
        super(_KeithleyInstrument, self).__init__(*args, **kwargs)

    @classmethod
    def _get_visa_error(cls, transaction: VISAHardware._VISATransaction) -> typing.Optional[TYPE_ERROR]:
        error = transaction.query(':SYST:ERR?')
        error_match = cls._RE_ERROR.search(error)

        if error_match is None:
            raise CommunicationError(f"Error message did not match expected format (response: {error})")

        error_code = int(error_match[1])

        if error_code == 0:
            return None
        else:
            return error_code, error_match[2]


class MultimeterDAQ6510(_KeithleyInstrument):
    """  """

    _SLOTS = [1, 2]
    _CHANNELS = []

    def __init__(self, *args, **kwargs):
        """ Create Hardware instance for a Keithley DAQ6510.

        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)

        # Occupied slots
        self._slot_module: typing.Dict[int, typing.Optional[str]] = {slot: None for slot in self._SLOTS}

        # Channel scan settings
        self._channel_scan: typing.List[int] = []

    @classmethod
    def scpi_display(cls, transaction: VISAHardware._VISATransaction,
                     msg: typing.Optional[str] = None) -> typing.NoReturn:
        # Clear display
        transaction.write(':DISP:CLE')

        if msg is not None:
            # Truncate long messages
            if len(msg) > 20:
                cls.get_class_logger().warning(f"Truncating message to 20 characters (original: {msg})")
                msg = msg[:20]

            transaction.write(':DISP:USER1:TEXT {}', msg)
            transaction.write(':DISP:SCR SWIPE_USER')
        else:
            transaction.write(':DISP:SCR HOME_LARG')

    # Hardware implementation
    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Keithley DAQ6510 Data Acquisition/Multimeter System'

    # SCPI overrides
    def transition_connect(self, event: typing.Optional[EventData] = None):
        super().transition_connect(event)

        with self.visa_transaction() as transaction:
            # Check for available modules
            for slot in self._SLOTS:
                card_idn = transaction.query(':SYST:CARD{}:IDN?', slot)
                self.get_logger().info(f"Card {slot}: {card_idn}")

                if card_idn.lower().startswith('empty slot'):
                    self._slot_module[slot] = None
                else:
                    self._slot_module[slot] = card_idn

    def transition_disconnect(self, event: typing.Optional[EventData] = None):
        # Clear available slots
        for slot in self._slot_module.keys():
            self._slot_module[slot] = None

        super().transition_disconnect(event)

    # Instrument commands
    def is_front_panel(self) -> bool:
        """ Test if front or rear terminals are connected for measurements.

        :return: if True front panel is selected, if False rear panel is selected
        :raises CommunicationError: when instrument returns unexpected response
        """
        with self.visa_transaction() as transaction:
            terminals = transaction.query(':ROUT:TERM?')

        if terminals == 'FRON':
            return True
        elif terminals == 'REAR':
            return False
        else:
            raise CommunicationError(f"Unexpected response to terminal status query: {terminals}")

    # Measurements
    @SCPIHardware.register_measurement(description='DC current (front terminals)',
                                       measurement_group=MeasurementGroup.CURRENT)
    def get_current(self) -> TYPE_FIELD_DICT:
        with self.visa_transaction() as transaction:
            return {
                'current': to_unit(transaction.query(':MEAS:CURR?'), 'amp')
            }

    @SCPIHardware.register_measurement(description='AC current (front terminals)',
                                       measurement_group=MeasurementGroup.CURRENT)
    def get_current_ac(self) -> TYPE_FIELD_DICT:
        # Switching to AC measurements can be very slow
        with self.visa_transaction(timeout=6) as transaction:
            return {
                'current_ac': to_unit(transaction.query(':MEAS:CURR:AC?'), 'amp')
            }

    @SCPIHardware.register_measurement(description='2-wire resistance (front terminals)',
                                       measurement_group=MeasurementGroup.RESISTANCE)
    def get_resistance(self) -> TYPE_FIELD_DICT:
        with self.visa_transaction() as transaction:
            resistance = to_unit(transaction.query(':MEAS:RES?'), 'ohm')

        if resistance.magnitude > 1e37:
            raise MeasurementUnavailable('Open circuit')

        return {
            'resistance': resistance
        }

    @SCPIHardware.register_measurement(description='4-wire resistance (front terminals)',
                                       measurement_group=MeasurementGroup.RESISTANCE)
    def get_resistance_kelvin(self) -> TYPE_FIELD_DICT:
        with self.visa_transaction() as transaction:
            resistance = to_unit(transaction.query(':MEAS:FRES?'), 'ohm')

        if resistance.magnitude > 1e37:
            raise MeasurementUnavailable('Open circuit')

        return {
            'resistance': resistance
        }

    @SCPIHardware.register_measurement(description='DC voltage (front terminals)',
                                       measurement_group=MeasurementGroup.VOLTAGE, default=True)
    def get_voltage(self) -> TYPE_FIELD_DICT:
        with self.visa_transaction() as transaction:
            return {
                'voltage': to_unit(transaction.query(':MEAS:VOLT?'), 'volt')
            }

    @SCPIHardware.register_measurement(description='AC voltage (front terminals)',
                                       measurement_group=MeasurementGroup.VOLTAGE)
    def get_voltage_ac(self) -> TYPE_FIELD_DICT:
        # Switching to AC measurements can be very slow
        with self.visa_transaction(timeout=6) as transaction:
            return {
                'voltage_ac': to_unit(transaction.query(':MEAS:VOLT:AC?'), 'volt')
            }

    @SCPIHardware.register_measurement(description='Multi-channel scan (rear terminals)')
    def get_channel_scan(self) -> TYPE_MEASUREMENT_LIST:
        pass


class Picoammeter6487(_KeithleyInstrument):
    """  """

    def __init__(self, *args, use_rs232: bool = True, **kwargs):
        """

        :param args:
        :param use_rs232:
        :param kwargs:
        """
        self._use_rs232 = use_rs232

        if self._use_rs232:
            super(Picoammeter6487, self).__init__(*args, visa_open_args={
                'baud_rate': 9600,
                'read_termination': '\r'
            }, **kwargs)
        else:
            super(Picoammeter6487, self).__init__(*args, **kwargs)

    @classmethod
    def scpi_display(cls, transaction: VISAHardware._VISATransaction,
                     msg: typing.Optional[str] = None) -> typing.NoReturn:
        if msg is None:
            # Disable message mode
            transaction.write(':DISP:TEXT:STAT OFF')
        else:
            # Truncate long messages
            if len(msg) > 12:
                cls.get_class_logger().warning(f"Truncating message to 12 characters (original: {msg})")
                msg = msg[:12]

            # Set message and enable message mode
            transaction.write(':DISP:TEXT {}', msg)
            transaction.write(':DISP:TEXT:STAT ON')

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Keithley 6487 Picoammeter'

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_configure(event)

        # Set remote operation
        if self._use_rs232:
            with self.visa_transaction() as transaction:
                transaction.write(':SYST:REM')
                transaction.write(':SYST:RWL')

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        if self._use_rs232:
            with self.visa_transaction() as transaction:
                transaction.write(':SYST:LOC')

        super().transition_cleanup(event)
