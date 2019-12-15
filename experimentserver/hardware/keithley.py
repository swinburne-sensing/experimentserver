import enum
import re
import typing

from transitions import EventData

from . import HardwareException, MeasurementNotReady
from .scpi import SCPIHardware, SCPIException
from .visa import VISAException, VISAEnum
from experimentserver.data import MeasurementGroup, to_unit, TYPE_FIELD_DICT


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class KeithleyException(SCPIException):
    pass


class DAQException(KeithleyException):
    pass


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

    @staticmethod
    def get_hardware_description() -> str:
        return 'Keithley DAQ6510 Data Acquisition/Multimeter System'

    def _get_error(self) -> typing.Union[None, str, typing.Tuple[int, str]]:
        msg = self.visa_query(':SYST:ERR?', remember_last=False)

        msg_match = self._RE_ERROR.search(msg)

        if msg_match is None:
            raise HardwareException(f"Error message did not match expected format (response: {msg})")

        error_code = int(msg_match[1])

        if error_code == 0:
            return None
        else:
            return error_code, msg_match[2]

    def display_msg(self, msg: str = None) -> typing.NoReturn:
        with self.get_hardware_lock():
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

    # def get_reading(self):
    #     # Clear existing buffer
    #     self.visa_write(':TRAC:CLE "defbuffer1"')
    #
    #     # Trigger
    #     self.visa_write(':INIT')
    #     self.scpi_wait()
    #
    #     # Get buffer size
    #     index = int(self.visa_query(':TRAC:ACT:END? "defbuffer1"'))
    #
    #     readings = self.visa_query(':TRAC:DATA? 1, {}, "defbuffer1", REL, READ, UNIT', index)
    #     readings = readings.split(',')
    #
    #     return readings

    # @SCPIHardware.register_parameter(description={'function': 'Channel Function', 'channel': 'Channel(s) to configure',
    #                                               'nplc': 'Number of power line cycles per measurement'})
    # def set_function(self, function: typing.Union[str, DAQChannelFunction],
    #                  channel: typing.Union[None, int, typing.List[int]] = None,
    #                  nplc: typing.Union[None, float, str] = None):
    #     if type(function) is str:
    #         function = DAQChannelFunction.from_str(function)
    #
    #     if nplc is not None:
    #         if function not in [DAQChannelFunction.VOLTAGE_DC, DAQChannelFunction.CURRENT_DC,
    #                             DAQChannelFunction.RESISTANCE_2WIRE, DAQChannelFunction.RESISTANCE_4WIRE,
    #                             DAQChannelFunction.DIODE, DAQChannelFunction.TEMPERATURE]:
    #             self._logger.error(f"NPLC cannot be configured when using {function}")
    #             return
    #
    #         if type(nplc) is str:
    #             nplc = nplc.upper()
    #
    #             if nplc not in ['DEFAULT', 'DEF', 'MAXIMUM', 'MAX', 'MINIMUM', 'MIN']:
    #                 pass
    #
    #     # Convert channel list
    #     if channel is not None:
    #         if type(channel) is list:
    #             channel = ','.join(map(str, channel))
    #
    #     with self.get_hardware_lock():
    #         # Configure function
    #         if channel is None:
    #             self.visa_write(':FUNC {}', function)
    #         else:
    #             self.visa_write(':FUNC {}, @({})', function, channel)
    #
    #         # If provided configure NPLC
    #         if nplc is not None:
    #             if channel is None:
    #                 self.visa_write(':{}:NPLC {}', function, nplc)
    #             else:
    #                 self.visa_write(':{}:NPLC {}, @({})', function, nplc, channel)

    @SCPIHardware.register_measurement(description='2-wire resistance', measurement_group=MeasurementGroup.RESISTANCE)
    def get_resistance(self) -> TYPE_FIELD_DICT:
        resistance = to_unit(self.visa_query(':MEAS:RES?'), 'ohm')

        if resistance.magnitude > 1e37:
            raise MeasurementNotReady('Open circuit')

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

    def is_front_panel(self) -> bool:
        with self.get_hardware_lock():
            terminals = self.visa_query(':ROUT:TERM?')

        if terminals == 'FRON':
            return True
        elif terminals == 'REAR':
            return False
        else:
            raise HardwareException(f"Unexpected response to terminal status query: {terminals}")

    def handle_setup(self, event: EventData):
        super(DAQ6510Multimeter, self).handle_setup(event)

        with self.get_hardware_lock():
            # Check for available modules
            for slot in self._SLOTS:
                card_idn = self.visa_query(':SYST:CARD{}:IDN?', slot)
                self._logger.info(f"Card {slot}: {card_idn}")

                if card_idn.lower().startswith('empty slot'):
                    self._slot_module[slot] = None
                else:
                    self._slot_module[slot] = card_idn

    def handle_start(self, event: EventData):
        super(DAQ6510Multimeter, self).handle_start(event)

    def handle_pause(self, event: EventData):
        super(DAQ6510Multimeter, self).handle_pause(event)

        # Pause trigger model
        # self.visa_write(':TRIG:PAUS')

    def handle_resume(self, event: EventData):
        super(DAQ6510Multimeter, self).handle_resume(event)

        # Resume trigger model
        # self.visa_write(':TRIG:RES')

    def handle_stop(self, event: EventData):
        super(DAQ6510Multimeter, self).handle_stop(event)

        # Halt trigger model
        self.visa_write(':ABOR')

    def handle_cleanup(self, event: EventData):
        # Clear slots
        for slot in self._slot_module.keys():
            self._slot_module[slot] = None

        super(DAQ6510Multimeter, self).handle_cleanup(event)

    def handle_error(self, event: EventData):
        super(DAQ6510Multimeter, self).handle_error(event)

        if self._visa_resource is not None:
            try:
                # Halt trigger model
                self.visa_write(':ABOR')
            except VISAException:
                self._logger.exception('VISA exception occurred while attempting to halt instrument')


class DAQ6510MultimeterMultichannel(DAQ6510Multimeter):
    def __init__(self, *args, **kwargs):
        super(DAQ6510MultimeterMultichannel, self).__init__(*args, **kwargs)

        self._slot_channels: typing.Optional[typing.List[int]] = None
        self._enabled_channels: typing.Optional[typing.List[int]] = None

    @staticmethod
    def get_hardware_description() -> str:
        return 'Keithley DAQ6510 Data Acquisition/Multimeter System with 7700 20-channel Differential Multiplexer ' \
               'Module'

    @staticmethod
    def _get_channel_ids(slot, n):
        """ Generate channel IDs given a slot ID and number of channels.

        :param slot:
        :param n:
        :return:
        """
        return [100 * slot + x + 1 for x in range(n)]

    def get_available_channels(self):
        if self._slot_channels is None:
            raise DAQException('Available channel list not available in disconnected state')

        return self._slot_channels

    def get_enabled_channels(self):
        if self._enabled_channels is None:
            raise DAQException('Enabled channel list not available in disconnected state')

        return self._enabled_channels

    def get_relay_cycle_count(self):
        # Get channel cycle counts
        relay_cycles = {}

        # Check for channels on any enables slots
        for slot, slot_module in self._slot_module.items():
            # Ignore empty slots
            if slot_module is None:
                continue

            cycle_count = list(map(int, self.visa_query(':ROUT:CLOS:COUN? (@SLOT{})', slot).split(',')))

            relay_cycles.update(dict(zip(self._get_channel_ids(slot, len(cycle_count)), cycle_count)))

        return relay_cycles

    def enable_channel(self):
        pass

    def disable_channel(self):
        pass

    def enable_channels(self):
        pass

    def disable_channels(self):
        pass

    def close_channel(self, channel: int):
        with self.get_hardware_lock():
            self.visa_write(':ROUT:CLOS (@{})', channel)

    def open_channel(self, channel: typing.Optional[int] = None):
        with self.get_hardware_lock():
            if channel is None:
                # Open all channels
                self.visa_write(':ROUT:OPEN:ALL')
            else:
                self.visa_write(':ROUT:OPEN (@{})', channel)

    def handle_setup(self, event: EventData):
        super().handle_setup(event)

        # Get relay cycle count
        cycle_count = ', '.join([f"{ch}: {count}" for ch, count in self.get_relay_cycle_count().items()])
        self._logger.info(f"Relay cycle count: {cycle_count}")

        if self.is_front_panel():
            raise DAQException('Rear panel terminals must be selected for multi-channel operation')

        # Get available channels


