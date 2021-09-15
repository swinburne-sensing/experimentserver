import abc
import enum
import re
import typing
from datetime import timedelta

from transitions import EventData

from ...data import TYPE_FIELD_DICT, MeasurementGroup, to_unit, TYPE_UNIT, Quantity, units, Measurement, to_timedelta,\
    TYPE_TIME
from ...data.measurement import TYPE_MEASUREMENT_LIST
from ..error import CommunicationError, ParameterError, MeasurementUnavailable, MeasurementError
from ..base.scpi import SCPIHardware
from ..base.visa import VISAHardware, TYPE_ERROR
from ..base.enum import HardwareEnum

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class DAQChannelFunction(HardwareEnum):
    OPEN = enum.auto()
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
    # DISTANCE = enum.auto()

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.List[str]]]:
        return {
            cls.OPEN: ['open', 'na', 'none'],
            cls.VOLTAGE_DC: ['v', 'volt', 'volts', 'voltage'],
            cls.CURRENT_DC: ['a', 'i', 'amp', 'amps', 'amperage'],
            cls.RESISTANCE_2WIRE: ['r', 'ohm', 'res', 'resistance'],
            # cls.DISTANCE: ['m', 'um', 'length', 'distance']
        }

    @classmethod
    def _get_description_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.OPEN: 'Open Circuit (external)',
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
            cls.PERIOD: 'Period',
            # cls.DISTANCE: 'Distance (via Keyence amp.)'
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
            cls.PERIOD: '\"PER\"',
            # cls.DISTANCE: '\"VOLT\"'
        }


class _KeithleyInstrument(SCPIHardware, metaclass=abc.ABCMeta):
    """  """

    _RE_ERROR = re.compile(r'([0-9]+),"([^\"]+)"')

    def __init__(self, *args, **kwargs):
        super(_KeithleyInstrument, self).__init__(*args, **kwargs)

    @classmethod
    def _get_visa_error(cls, transaction: VISAHardware.VISATransaction) -> typing.Optional[TYPE_ERROR]:
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

    _CHANNEL_INTERNAL = [23, 24, 25]

    def __init__(self, *args, **kwargs):
        """ Create Hardware instance for a Keithley DAQ6510.

        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)

        # Occupied slots
        self._slot_module: typing.Dict[int, typing.Optional[str]] = {slot: None for slot in self._SLOTS}

        # Channel scan settings
        # self._channel_scan: typing.Dict[int, DAQChannelFunction] = {}

    @classmethod
    def scpi_display(cls, transaction: VISAHardware.VISATransaction,
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

    # Parameters
    # @SCPIHardware.register_parameter(description='Configure a measurement channel and add to scan list')
    # def configure_channel(self, channel: int, mode: DAQChannelFunction):
    #     mode = DAQChannelFunction.from_input(mode)
    #
    #     with self.visa_transaction() as transaction:
    #         # Configure channel mode
    #         transaction.write("SENS:FUNC {}, (@{})", mode, channel)
    #
    #     self._channel_scan[channel] = mode
    #
    # @SCPIHardware.register_parameter(description='Remove measurement channel from scan list')
    # def remove_channel(self, channel: int):
    #     if channel in self._channel_scan:
    #         self._channel_scan.pop(channel)
    #
    # @SCPIHardware.register_parameter(description='Update channel scan configuration')
    # def setup_channel_scan(self):
    #     with self.visa_transaction() as transaction:
    #         # Create scan list
    #         transaction.write("ROUT:SCAN:CRE (@{})", ','.join(map(str, self._channel_scan)))

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

    # @SCPIHardware.register_measurement(description='Displacement via Keyence Laser Displacment',
    #                                    measurement_group=MeasurementGroup.POSITION)
    # def get_distance(self) -> TYPE_FIELD_DICT:
    #     # Get DC volts and convert
    #     with self.visa_transaction(timeout=6) as transaction:
    #         voltage = to_unit(transaction.query(':MEAS:VOLT?'), 'volt')
    #
    #     distance = to_unit(voltage.magnitude, 'mm')
    #
    #     if distance.magnitude > 5.5:
    #         raise MeasurementUnavailable('Outside measurement range')
    #
    #     return {
    #         'distance': distance
    #     }


class Picoammeter6487(_KeithleyInstrument):
    """ Hardware interface for communication with Keithley 6487 Picoammeter. """

    def __init__(self, *args, use_rs232: bool = True, settling_time: TYPE_TIME = timedelta(), **kwargs):
        """

        :param args:
        :param use_rs232: if True VISA resource arguments are configured to support operation over RS232
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

        # Settling time for sweep measurements
        self._settling_time = to_timedelta(settling_time)

        # Interlock enabled when source voltage is configured to over 10V
        self._check_interlock = False

        # Save mode to correctly handle returned readings
        self._expect_ohms = False

        # Used for generation of I-V sweeps
        self._source_sweep_range: typing.Optional[typing.Sequence[Quantity]] = None

    @classmethod
    def scpi_display(cls, transaction: VISAHardware.VISATransaction,
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
    def _fetch_interlock_state(transaction: VISAHardware.VISATransaction) -> bool:
        return not (transaction.query(':SOUR:VOLT:INT:FAIL?') == '1')

    @SCPIHardware.register_parameter(description='Measurement range', order=40)
    def set_measure_range(self, measure_range: TYPE_UNIT):
        if type(measure_range) is str and measure_range.lower() == 'auto':
            with self.visa_transaction() as transaction:
                transaction.write(':RANG:AUTO')
        else:
            measure_range = to_unit(measure_range, 'amp', magnitude=True)

            with self.visa_transaction() as transaction:
                transaction.write(":RANG {}", measure_range)

    @SCPIHardware.register_parameter(description='Measurement rate', order=40)
    def set_measure_rate(self, rate: TYPE_UNIT):
        rate = float(rate)

        with self.visa_transaction() as transaction:
            transaction.write(":NPLC {}", rate)

    @SCPIHardware.register_parameter(description='Enable resistance measurement')
    def set_measure_ohms(self, enable: typing.Union[bool, str]):
        if type(enable) is str:
            enable = enable.strip().lower() in ['true', '1', 'on']

        with self.visa_transaction() as transaction:
            if enable:
                self._expect_ohms = True

                # Set ohms mode
                transaction.write(':CONF')
                transaction.write(':OHMS ON')
            else:
                self._expect_ohms = False

                # Set current mode
                transaction.write(':CONF:CURR:DC')
                transaction.write(':OHMS OFF')

    @SCPIHardware.register_parameter(description='Source current limit', order=40)
    def set_source_current(self, current: TYPE_UNIT):
        current = to_unit(current, 'amp', magnitude=True)

        with self.visa_transaction() as transaction:
            transaction.write(":SOUR:VOLT:ILIM {}", current)

    @SCPIHardware.register_parameter(description='Source voltage')
    def set_source_voltage(self, voltage: TYPE_UNIT):
        voltage = to_unit(voltage, 'volt', magnitude=True)

        with self.visa_transaction() as transaction:
            # Check output range
            if voltage <= 10:
                transaction.write(':SOUR:VOLT:RANG 10')
                self._check_interlock = False
            else:
                if not self._fetch_interlock_state(transaction):
                    raise ParameterError("Interlock asserted")

                if voltage <= 50:
                    transaction.write(':SOUR:VOLT:RANG 50')
                else:
                    transaction.write(':SOUR:VOLT:RANG 500')

                self._check_interlock = True

            # Set output voltage
            transaction.write(":SOUR:VOLT {}", voltage)

    @SCPIHardware.register_parameter(description='Source output enable', order=60)
    def set_source_enable(self, enable: typing.Union[bool, str]):
        if type(enable) is str:
            enable = enable.strip().lower() in ['true', '1', 'on']

        with self.visa_transaction() as transaction:
            transaction.write(":SOUR:VOLT:STAT {}", enable)

    @SCPIHardware.register_parameter(description='Source sweep range start, step and stop', order=30)
    def set_source_sweep(self, start: TYPE_UNIT, step: TYPE_UNIT, stop: TYPE_UNIT):
        start = to_unit(start, 'volt')
        step = to_unit(step, 'volt')
        stop = to_unit(stop, 'volt')

        self._source_sweep_range = []
        voltage = start

        # Generate sweep range
        while voltage <= stop:
            self._source_sweep_range.append(voltage)
            voltage += step

        self.get_logger().info(f"Sweep values: {', '.join(map(str, self._source_sweep_range))}")

    @SCPIHardware.register_parameter(description='Source sweep measurement delay')
    def set_source_sweep_settling(self, settling_time: TYPE_TIME):
        # Settling time for sweep measurements
        self._settling_time = to_timedelta(settling_time)

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Keithley 6487 Picoammeter'

    @SCPIHardware.register_measurement(description='Measure current/resistance', default=True)
    def get_reading(self) -> Measurement:
        # Fetch reading from instrument
        with self.visa_transaction() as transaction:
            reading = transaction.query(':READ?')
            source_voltage = to_unit(transaction.query(':SOUR:VOLT?'), 'volt')
            source_enabled = int(transaction.query(':SOUR:VOLT:STAT?'))

        # Get reading
        reading = reading.split(',', 1)
        reading = reading[0]

        # If ohms reading then convert case
        if 'ohm' in reading.lower():
            reading = reading.lower()

        # Parse unit
        reading = to_unit(reading)

        if self._expect_ohms != (reading.units == units.ohm):
            raise MeasurementError('Measurement returned wrong unit')

        if abs(reading.magnitude) > pow(10, 36):
            raise MeasurementUnavailable('Open circuit')

        if self._expect_ohms:
            return Measurement(self, MeasurementGroup.RESISTANCE, {
                'resistance': reading,
                'voltage': source_voltage
            }, tags={
                'source_voltage': source_voltage,
                'source_enabled': 'ON' if source_enabled else 'OFF'
            })
        else:
            return Measurement(self, MeasurementGroup.CONDUCTOMETRIC_IV, {
                'current': reading,
                'voltage': source_voltage
            }, tags={
                'source_voltage': source_voltage,
                'source_enabled': 'ON' if source_enabled else 'OFF'
            })

    @SCPIHardware.register_measurement(description='Measure current/resistance across range of source voltages')
    def get_sweep(self) -> TYPE_MEASUREMENT_LIST:
        measurement_list = []

        settling_time = self._settling_time.total_seconds()

        if self._source_sweep_range is None:
            raise MeasurementUnavailable('Sweep range not configured')

        for voltage in self._source_sweep_range:
            self.set_source_voltage(voltage)

            # Take measurement after settling time
            if settling_time > 0:
                self.sleep(settling_time, 'settling time')

            # Get reading
            try:
                measurement_list.append(self.get_reading())
            except MeasurementUnavailable:
                pass

        return measurement_list

    def transition_connect(self, event: typing.Optional[EventData] = None):
        super().transition_connect(event)

        # Enable remote operation
        if self._use_rs232:
            with self.visa_transaction() as transaction:
                transaction.write(':SYST:REM')
                transaction.write(':SYST:RWL')

        # On reset unit defaults to current mode
        self._expect_ohms = False

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Disable voltage source
        self.set_source_enable(False)

        if self._use_rs232:
            with self.visa_transaction() as transaction:
                transaction.write(':SYST:LOC')

        super().transition_cleanup(event)
