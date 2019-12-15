import enum
import re
import typing
from datetime import datetime

from transitions import EventData

from . import TYPE_MEASUREMENT_TUPLE_LIST
from .visa import VISAEnum, VISAException
from .scpi import SCPIHardware, SCPIException

from experimentserver.data import MeasurementGroup, to_unit, TYPE_UNIT_OPTIONAL


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class RigolException(SCPIException):
    pass


class PowerSupplyChannel(VISAEnum):
    CHANNEL1 = enum.auto()
    CHANNEL2 = enum.auto()
    CHANNEL3 = enum.auto()

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[VISAEnum, typing.List[str]]]:
        return {
            cls.CHANNEL1: ['channel1', 'channel 1', 'ch1', 'ch 1', 1],
            cls.CHANNEL2: ['channel2', 'channel 2', 'ch2', 'ch 2', 2],
            cls.CHANNEL3: ['channel3', 'channel 3', 'ch3', 'ch 3', 3]
        }

    @classmethod
    def _get_description_map(cls) -> typing.Dict[VISAEnum, str]:
        return {
            cls.CHANNEL1: 'Channel 1',
            cls.CHANNEL2: 'Channel 2',
            cls.CHANNEL3: 'Channel 3'
        }

    @classmethod
    def _get_visa_map(cls) -> typing.Dict[VISAEnum, str]:
        return {
            cls.CHANNEL1: 'CH1',
            cls.CHANNEL2: 'CH2',
            cls.CHANNEL3: 'CH3'
        }

    @staticmethod
    def get_tag_name() -> str:
        return 'channel'

    @classmethod
    def _get_tag_map(cls) -> typing.Dict[VISAEnum, typing.Any]:
        return {
            cls.CHANNEL1: 1,
            cls.CHANNEL2: 2,
            cls.CHANNEL3: 3
        }


class DP832PowerSupply(SCPIHardware):
    _RE_ERROR = re.compile(r'([0-9]+),"([\w\d\; ]+)"')

    def __init__(self, *args, ovp_channel_1: TYPE_UNIT_OPTIONAL= None, ovp_channel_2: TYPE_UNIT_OPTIONAL = None,
                 ovp_channel_3: TYPE_UNIT_OPTIONAL = None, ocp_channel_1: TYPE_UNIT_OPTIONAL = None,
                 ocp_channel_2: TYPE_UNIT_OPTIONAL = None, ocp_channel_3: TYPE_UNIT_OPTIONAL = None,
                 output_failsafe: bool = True, **kwargs):
        super(DP832PowerSupply, self).__init__(*args, **kwargs)

        # Save over voltage/current limits if provided
        self._ovp = {
            PowerSupplyChannel.CHANNEL1: to_unit(ovp_channel_1, 'volt'),
            PowerSupplyChannel.CHANNEL2: to_unit(ovp_channel_2, 'volt'),
            PowerSupplyChannel.CHANNEL3: to_unit(ovp_channel_3, 'volt')
        }

        self._ocp = {
            PowerSupplyChannel.CHANNEL1: to_unit(ocp_channel_1, 'amp'),
            PowerSupplyChannel.CHANNEL2: to_unit(ocp_channel_2, 'amp'),
            PowerSupplyChannel.CHANNEL3: to_unit(ocp_channel_3, 'amp')
        }

        # On error attempt to disable output channels
        self._output_failsafe = output_failsafe

    def _get_error(self) -> typing.Union[None, str, typing.Tuple[int, str]]:
        msg = self.visa_query(':SYST:ERR?')

        msg_match = self._RE_ERROR.search(msg)

        if msg_match is None:
            raise RigolException(self, "Error message did not match expected format (response: {})".format(msg))

        error_code = int(msg_match[1])

        if error_code == 0:
            return None
        else:
            return error_code, msg_match[2]

    def display_msg(self, msg: str = None) -> typing.NoReturn:
        with self.get_hardware_lock():
            if msg is not None:
                if len(msg) > 45:
                    self._logger.warning("Truncating message to 45 characters (original: {})".format(msg))
                    msg = msg[:45]

                self.visa_write(":DISP:TEXT \"{}\"".format(msg))
            else:
                self.visa_write(':DISP:TEXT:CLE')

    @staticmethod
    def get_hardware_description() -> str:
        return 'Rigol DP832 3-Channel Power Supply'

    def get_ocp_alarm(self, channel: PowerSupplyChannel):
        with self.get_hardware_lock():
            alarm = self.visa_query(':OUTP:OCP:ALAR? {}', channel)

            if alarm == 'YES':
                return True
            elif alarm == 'NO':
                return False
            else:
                raise RigolException(f"Unexpected response to OCP alarm query: {alarm!r}")

    def get_ovp_alarm(self, channel: PowerSupplyChannel):
        with self.get_hardware_lock():
            alarm = self.visa_query(':OUTP:OVP:ALAR? {}', channel)

            if alarm == 'YES':
                return True
            elif alarm == 'NO':
                return False
            else:
                raise RigolException(f"Unexpected response to OVP alarm query: {alarm!r}")

    @SCPIHardware.register_parameter(description='Enable output channel')
    def enable_output(self, channel: typing.Optional[PowerSupplyChannel] = None):
        with self.get_hardware_lock():
            if channel is None:
                self.visa_write(':OUTP ON')
            else:
                self.visa_write(':OUTP {} ON', channel)

    @SCPIHardware.register_parameter(description='Disable output channel')
    def disable_output(self, channel: typing.Optional[PowerSupplyChannel] = None):
        with self.get_hardware_lock():
            if channel is None:
                self.visa_write(':OUTP OFF')
            else:
                self.visa_write(':OUTP {} OFF', channel)

    @SCPIHardware.register_measurement(description='Output state, voltage and current',
                                       measurement_group=MeasurementGroup.SUPPLY, force=True)
    def get_supply_state(self) -> TYPE_MEASUREMENT_TUPLE_LIST:
        status = []

        for channel in PowerSupplyChannel:
            with self.get_hardware_lock():
                # Query over voltage and current alarms
                if self.get_ocp_alarm(channel):
                    raise RigolException(f"Over current alarm triggered on channel {channel}")

                if self.get_ovp_alarm(channel):
                    raise RigolException(f"Over voltage alarm triggered on channel {channel}")

                channel_ocp = self.visa_query(':OUTP:OCP:VAL?', channel)
                channel_ovp = self.visa_query(':OUTP:OVP:VAL?', channel)

                # Get channel measurements and mode
                channel_setting = self.visa_query(':APPL? {}', channel)
                channel_status = self.visa_query(':MEAS:ALL? {}', channel)
                channel_mode = self.visa_query(':OUTP:MODE? {}', channel)

            channel_setting_split = channel_setting.split(',')

            if len(channel_setting_split) != 3:
                raise RigolException(f"Unexpected response to channel setting query: {channel_setting!r}")

            channel_status_split = channel_status.split(',')

            if len(channel_status_split) != 3:
                raise RigolException(f"Unexpected response to channel status query: {channel_status!r}")

            if channel_mode not in ('CV', 'CC', 'UR'):
                raise RigolException(f"Unexpected response to channel mode query: {channel_mode}")

            status.append((
                datetime.now(),
                MeasurementGroup.SUPPLY,
                {
                    'voltage': to_unit(channel_status_split[0], 'volt'),
                    'voltage_setpoint': to_unit(channel_setting_split[1], 'volt'),
                    'voltage_limit': to_unit(channel_ovp, 'volt'),
                    'current': to_unit(channel_status_split[1], 'amp'),
                    'current_setpoint': to_unit(channel_setting_split[2], 'amp'),
                    'current_limit': to_unit(channel_ocp, 'volt'),
                    'power': to_unit(channel_status_split[2], 'watt'),
                    'mode': channel_mode
                },
                {
                    PowerSupplyChannel.get_tag_name(): channel.get_tag_value()
                }
            ))

        return status

    def handle_setup(self, event: EventData):
        super(DP832PowerSupply, self).handle_setup(event)

        # Enable remote mode
        self.visa_write(':SYST:REM')

        # Apply over voltage/current limits to channels
        for channel, limit in self._ovp.items():
            if limit is not None:
                self.visa_write(':OUTP:OVP {},ON', channel)
                self.visa_write(':OUTP:OVP:VAL {},{}', channel, limit.magnitude)
            else:
                self.visa_write(':OUTP:OVP {},OFF', channel)

        for channel, limit in self._ocp.items():
            if limit is not None:
                self.visa_write(':OUTP:OCP {},ON', channel)
                self.visa_write(':OUTP:OCP:VAL {},{}', channel, limit.magnitude)
            else:
                self.visa_write(':OUTP:OCP {},OFF', channel)

    def handle_start(self, event: EventData):
        super(DP832PowerSupply, self).handle_start(event)

    def handle_pause(self, event: EventData):
        super(DP832PowerSupply, self).handle_pause(event)

    def handle_resume(self, event: EventData):
        super(DP832PowerSupply, self).handle_resume(event)

    def handle_stop(self, event: EventData):
        super(DP832PowerSupply, self).handle_stop(event)

    def handle_cleanup(self, event: EventData):
        self.visa_write(':SYST:LOC')

        super(DP832PowerSupply, self).handle_cleanup(event)

    def handle_error(self, event: EventData):
        # Disconnect all output channels
        if self._output_failsafe:
            if self._visa_resource is not None:
                for channel in PowerSupplyChannel:
                    try:
                        self.visa_write(':OUTP {} OFF', channel)
                    except VISAException:
                        self._logger.warning(f"Failed to disable output channel {channel} after error", notify=True)

        super(DP832PowerSupply, self).handle_error(event)
