import enum
import re
import typing

from transitions import EventData

from .visa import VISAEnum, VISAException
from .scpi import SCPIHardware, SCPIException

from experimentserver.data import MeasurementGroup, to_unit, TYPING_UNIT_OPTIONAL


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


class DP832PowerSupply(SCPIHardware):
    _RE_ERROR = re.compile(r'([0-9]+),"([\w\d\; ]+)"')

    def __init__(self, *args, ovp_channel_1: TYPING_UNIT_OPTIONAL= None, ovp_channel_2: TYPING_UNIT_OPTIONAL= None,
                 ovp_channel_3: TYPING_UNIT_OPTIONAL= None, ocp_channel_1: TYPING_UNIT_OPTIONAL= None,
                 ocp_channel_2: TYPING_UNIT_OPTIONAL= None, ocp_channel_3: TYPING_UNIT_OPTIONAL= None,
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

    def handle_setup(self, event: EventData):
        super(DP832PowerSupply, self).handle_setup(event)

        # Apply over voltage/current limits to channels
        for channel, limit in self._ovp.items():
            if limit is not None:
                self.visa_write(':OUTP:OVP:VAL {},{}', channel, limit)
            else:
                self.visa_write(':OUTP:OVP:VAL {},MAX', channel)

        for channel, limit in self._ocp.items():
            if limit is not None:
                self.visa_write(':OUTP:OCP ')
                self.visa_write(':OUTP:OCP:VAL {},{}', channel, limit)
            else:
                self.visa_write(':OUTP:OCP:VAL {},MAX', channel)

    def handle_start(self, event: EventData):
        super(DP832PowerSupply, self).handle_start(event)

    def handle_pause(self, event: EventData):
        super(DP832PowerSupply, self).handle_pause(event)

    def handle_resume(self, event: EventData):
        super(DP832PowerSupply, self).handle_resume(event)

    def handle_stop(self, event: EventData):
        super(DP832PowerSupply, self).handle_stop(event)

    def handle_cleanup(self, event: EventData):
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
