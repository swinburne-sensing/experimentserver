import enum
import re
import typing

from transitions import EventData

from experimentserver.data import MeasurementGroup, to_unit, TYPE_FIELD_DICT
from . import CommunicationError, HardwareError, MeasurementUnavailable
from .scpi import SCPIHardware, TYPE_ERROR
from .visa import VISAEnum


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class DAQChannelFunction(VISAEnum):
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
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[VISAEnum, typing.List[str]]]:
        return {
            cls.VOLTAGE_DC: ['v', 'volt', 'volts', 'voltage'],
            cls.CURRENT_DC: ['a', 'i', 'amp', 'amps', 'amperage'],
            cls.RESISTANCE_2WIRE: ['r', 'ohm', 'res', 'resistance']
        }

    @classmethod
    def _get_description_map(cls) -> typing.Dict[VISAEnum, str]:
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
    def _get_visa_map(cls) -> typing.Dict[VISAEnum, str]:
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


class DAQ6510Multimeter(SCPIHardware):
    _RE_ERROR = re.compile(r'([0-9]+),"([^\"]+)"')
    _SLOTS = [1, 2]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Occupied slots
        self._slot_module: typing.Dict[int, typing.Optional[str]] = {slot: None for slot in self._SLOTS}

    # SCPI implementation
    def set_scpi_display(self, msg: typing.Optional[str] = None) -> typing.NoReturn:
        with self.get_visa_transaction_lock(error_ignore=True):
            # Clear display
            self.visa_write(':DISP:CLE')

            if msg is not None:
                # Truncate long messages
                if len(msg) > 20:
                    self._logger.warning(f"Truncating message to 20 characters (original: {msg})")
                    msg = msg[:20]

                self.visa_write(':DISP:USER1:TEXT {}', msg)
                self.visa_write(':DISP:SCR SWIPE_USER')
            else:
                self.visa_write(':DISP:SCR HOME_LARG')

    def get_scpi_error(self) -> TYPE_ERROR:
        error = self.visa_query(':SYST:ERR?')
        error_match = self._RE_ERROR.search(error)

        if error_match is None:
            raise CommunicationError(f"Error message did not match expected format (response: {error})")

        error_code = int(error_match[1])

        if error_code == 0:
            return None
        else:
            return error_code, error_match[2]

    # Hardware implementation
    @staticmethod
    def get_hardware_description() -> str:
        return 'Keithley DAQ6510 Data Acquisition/Multimeter System'

    # SCPI overrides
    def handle_connect(self, event: typing.Optional[EventData] = None):
        super().handle_connect(event)

        # Check for available modules
        for slot in self._SLOTS:
            card_idn = self.visa_query(':SYST:CARD{}:IDN?', slot)
            self._logger.info(f"Card {slot}: {card_idn}")

            if card_idn.lower().startswith('empty slot'):
                self._slot_module[slot] = None
            else:
                self._slot_module[slot] = card_idn

    def handle_disconnect(self, event: typing.Optional[EventData] = None):
        # Clear available slots
        for slot in self._slot_module.keys():
            self._slot_module[slot] = None

        super().handle_disconnect(event)

    # Instrument state
    def is_front_panel(self) -> bool:
        with self.get_hardware_lock():
            terminals = self.visa_query(':ROUT:TERM?')

        if terminals == 'FRON':
            return True
        elif terminals == 'REAR':
            return False
        else:
            raise CommunicationError(f"Unexpected response to terminal status query: {terminals}")

    # Measurements
    @SCPIHardware.register_measurement(description='2-wire resistance', measurement_group=MeasurementGroup.RESISTANCE)
    def get_resistance(self) -> TYPE_FIELD_DICT:
        resistance = to_unit(self.visa_query(':MEAS:RES?'), 'ohm')

        if resistance.magnitude > 1e37:
            raise MeasurementUnavailable('Open circuit')

        return {
            'resistance': resistance
        }

    @SCPIHardware.register_measurement(description='AC voltage', measurement_group=MeasurementGroup.VOLTAGE)
    def get_voltage_ac(self) -> TYPE_FIELD_DICT:
        return {
            'voltage_ac': to_unit(self.visa_query(':MEAS:VOLT:AC?'), 'volt')
        }

    @SCPIHardware.register_measurement(description='DC voltage', measurement_group=MeasurementGroup.VOLTAGE,
                                       default=True)
    def get_voltage(self) -> TYPE_FIELD_DICT:
        return {
            'voltage': to_unit(self.visa_query(':MEAS:VOLT?'), 'volt')
        }
