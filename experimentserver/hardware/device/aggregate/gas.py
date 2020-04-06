import typing

from transitions import EventData

from ...base.core import Hardware
from ...metadata import TYPE_PARAMETER_DICT


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class GasController(Hardware):
    def __init__(self, identifier: str, mfcs: typing.List[str], parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 valves: typing.Optional[typing.Dict[str, typing.List[str]]] = None):
        super(GasController, self).__init__(identifier, parameters)

        self._gas_mfcs = mfcs
        self._gas_valves = valves

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Gas Controller'

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing.
        pass

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing.
        pass

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing.
        pass

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing.
        pass

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing.
        pass
