import enum
import re
import typing

from transitions import EventData

from ..error import CommunicationError, ExternalError
from ..base.enum import HardwareEnum, TYPE_ENUM_CAST
from ..base.scpi import SCPIHardware
from ..base.visa import TYPE_ERROR, VISAHardware
from ...data.unit import to_unit, TYPE_UNIT, TYPE_UNIT_OPTIONAL
from experimentserver.data.measurement import Measurement, MeasurementGroup

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class RigolError(ExternalError):
    pass


class PowerSupplyChannel(HardwareEnum):
    CHANNEL1 = enum.auto()
    CHANNEL2 = enum.auto()
    CHANNEL3 = enum.auto()

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.List[typing.Any]]]:
        return {
            cls.CHANNEL1: ['channel1', 'channel 1', 'ch1', 'ch 1', 1],
            cls.CHANNEL2: ['channel2', 'channel 2', 'ch2', 'ch 2', 2],
            cls.CHANNEL3: ['channel3', 'channel 3', 'ch3', 'ch 3', 3]
        }

    @classmethod
    def _get_description_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.CHANNEL1: 'Channel 1',
            cls.CHANNEL2: 'Channel 2',
            cls.CHANNEL3: 'Channel 3'
        }

    @classmethod
    def _get_command_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.CHANNEL1: 'CH1',
            cls.CHANNEL2: 'CH2',
            cls.CHANNEL3: 'CH3'
        }

    @staticmethod
    def get_tag_name() -> str:
        return 'channel'

    @classmethod
    def _get_tag_map(cls) -> typing.Dict[HardwareEnum, typing.Any]:
        return {
            cls.CHANNEL1: 1,
            cls.CHANNEL2: 2,
            cls.CHANNEL3: 3
        }


class DP832PowerSupply(SCPIHardware):
    _RE_ERROR = re.compile(r'([0-9]+),"([\w\d\; ]+)"')

    def __init__(self, *args, output_failsafe: bool = True, **kwargs):
        super(DP832PowerSupply, self).__init__(*args, **kwargs)

        # On error attempt to disable output channels
        self._output_failsafe = output_failsafe

    @classmethod
    def scpi_display(cls, transaction: VISAHardware.VISATransaction,
                     msg: typing.Optional[str] = None) -> typing.NoReturn:
        if msg is not None:
            if len(msg) > 45:
                cls.get_class_logger().warning("Truncating message to 45 characters (original: {})".format(msg))
                msg = msg[:45]

            transaction.write(":DISP:TEXT \"{}\"".format(msg))
        else:
            transaction.write(':DISP:TEXT:CLE')

    @classmethod
    def _get_visa_error(cls, transaction: VISAHardware.VISATransaction) -> typing.Optional[TYPE_ERROR]:
        error = transaction.query(':SYST:ERR?')

        error_match = cls._RE_ERROR.search(error)

        if error_match is None:
            raise RigolError("Error message did not match expected format (response: {})".format(error))

        error_code = int(error_match[1])

        if error_code == 0:
            return None
        else:
            return error_code, error_match[2]

    # Hardware implementation
    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Rigol DP832 3-Channel Power Supply'

    # Instrument manager
    def get_ocp_alarm(self, channel: PowerSupplyChannel):
        with self.visa_transaction() as transaction:
            alarm = transaction.query(':OUTP:OCP:ALAR? {}', channel)

        if alarm == 'YES':
            return True
        elif alarm == 'NO':
            return False
        else:
            raise RigolError(f"Unexpected response to OCP alarm query: {alarm!r}")

    def get_ovp_alarm(self, channel: PowerSupplyChannel):
        with self.visa_transaction() as transaction:
            alarm = transaction.query(':OUTP:OVP:ALAR? {}', channel)

        if alarm == 'YES':
            return True
        elif alarm == 'NO':
            return False
        else:
            raise RigolError(f"Unexpected response to OVP alarm query: {alarm!r}")

    @SCPIHardware.register_parameter(description='Enable/disable output channel', order=90, primary_key=['channel'])
    def set_output(self, channel: typing.Union[TYPE_ENUM_CAST, PowerSupplyChannel], enable: typing.Union[bool, str]):
        channel = PowerSupplyChannel.from_input(channel)

        if type(enable) is str:
            enable = enable.strip().lower() in ['true', '1', 'on']

        with self.visa_transaction() as transaction:
            transaction.write(':OUTP {},{}', channel, enable)

    @SCPIHardware.register_parameter(description='Set output current limit', order=40, primary_key=['channel'])
    def set_ocp(self, channel: typing.Union[TYPE_ENUM_CAST, PowerSupplyChannel], current: TYPE_UNIT_OPTIONAL = None):
        channel = PowerSupplyChannel.from_input(channel)
        current = to_unit(current, 'amp', magnitude=True)

        with self.visa_transaction() as transaction:
            if current is not None:
                transaction.write(':OUTP:OCP {},ON', channel)
                transaction.write(':OUTP:OCP:VAL {},{}', channel, current)
            else:
                transaction.write(':OUTP:OCP {},OFF', channel)

    @SCPIHardware.register_parameter(description='Set output voltage limit', order=40, primary_key=['channel'])
    def set_ovp(self, channel: typing.Union[TYPE_ENUM_CAST, PowerSupplyChannel], voltage: TYPE_UNIT_OPTIONAL = None):
        channel = PowerSupplyChannel.from_input(channel)
        voltage = to_unit(voltage, 'volt', magnitude=True)

        with self.visa_transaction() as transaction:
            if voltage is not None:
                transaction.write(':OUTP:OVP {},ON', channel)
                transaction.write(':OUTP:OVP:VAL {},{}', channel, voltage)
            else:
                transaction.write(':OUTP:OVP {},OFF', channel)

    @SCPIHardware.register_parameter(description='Set output voltage', primary_key=['channel'])
    def set_voltage(self, channel: typing.Union[TYPE_ENUM_CAST, PowerSupplyChannel], voltage: TYPE_UNIT):
        channel = PowerSupplyChannel.from_input(channel)
        voltage = to_unit(voltage, 'volt', magnitude=True)

        with self.visa_transaction() as transaction:
            transaction.write(':SOUR{}:VOLT {}', channel, voltage)

    @SCPIHardware.register_parameter(description='Set output voltage', primary_key=['channel'])
    def set_current(self, channel: typing.Union[TYPE_ENUM_CAST, PowerSupplyChannel], current: TYPE_UNIT):
        channel = PowerSupplyChannel.from_input(channel)
        current = to_unit(current, 'amp', magnitude=True)

        with self.visa_transaction() as transaction:
            transaction.write(':SOUR{}:CURR {}', channel, current)

    @SCPIHardware.register_measurement(description='Output manager, voltage and current',
                                       measurement_group=MeasurementGroup.SUPPLY, force=True)
    def get_supply_state(self) -> typing.Sequence[Measurement]:
        # Delay slightly to allow for new data
        self.sleep(1, 'rate limit, infrequent update')

        status = []

        for channel in PowerSupplyChannel:
            with self.visa_transaction() as transaction:
                # Query over voltage and current alarms
                if self.get_ocp_alarm(channel):
                    self.get_logger().error(f"Over current alarm triggered on channel {channel}")

                if self.get_ovp_alarm(channel):
                    self.get_logger().error(f"Over voltage alarm triggered on channel {channel}")

                channel_ocp = transaction.query(':OUTP:OCP:VAL?', channel)
                channel_ovp = transaction.query(':OUTP:OVP:VAL?', channel)

                # Get channel measurements and mode
                channel_setting = transaction.query(':APPL? {}', channel)
                channel_status = transaction.query(':MEAS:ALL? {}', channel)
                channel_mode = transaction.query(':OUTP:MODE? {}', channel)

            channel_setting_split = channel_setting.split(',')

            if len(channel_setting_split) != 3:
                raise RigolError(f"Unexpected response to channel setting query: {channel_setting!r}")

            channel_status_split = channel_status.split(',')

            if len(channel_status_split) != 3:
                raise RigolError(f"Unexpected response to channel status query: {channel_status!r}")

            if channel_mode not in ('CV', 'CC', 'UR'):
                raise RigolError(f"Unexpected response to channel mode query: {channel_mode}")

            status.append(Measurement(
                self,
                MeasurementGroup.SUPPLY,
                {
                    'voltage': to_unit(channel_status_split[0], 'volt', apply_round=3),
                    'voltage_setpoint': to_unit(channel_setting_split[1], 'volt', apply_round=3),
                    'voltage_limit': to_unit(channel_ovp, 'volt', apply_round=3),
                    'current': to_unit(channel_status_split[1], 'amp', apply_round=3),
                    'current_setpoint': to_unit(channel_setting_split[2], 'amp', apply_round=3),
                    'current_limit': to_unit(channel_ocp, 'volt', apply_round=3),
                    'power': to_unit(channel_status_split[2], 'watt', apply_round=3),
                    'mode': channel_mode
                },
                tags={
                    PowerSupplyChannel.get_tag_name(): channel.get_tag_value()
                })
            )

        return status

    # Event handling
    def transition_connect(self, event: typing.Optional[EventData] = None):
        super().transition_connect(event)

        # Enable remote mode
        with self.visa_transaction() as transaction:
            transaction.write(':SYST:REM')

    def transition_disconnect(self, event: typing.Optional[EventData] = None):
        # Return to local control
        with self.visa_transaction() as transaction:
            transaction.write(':SYST:LOC')

        super().transition_disconnect(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        if self._output_failsafe:
            for channel in PowerSupplyChannel:
                self.set_output(channel, False)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Disconnect all output channels
        if self._output_failsafe:
            if self._visa_resource is not None:
                with self.visa_transaction(error_raise=False) as transaction:
                    for channel in PowerSupplyChannel:
                        try:
                            transaction.write(':OUTP {},OFF', channel)
                        except (CommunicationError, ExternalError):
                            self.get_logger().warning(f"Failed to disable output channel {channel} after error",
                                                      notify=True)

        super().transition_error(event)
