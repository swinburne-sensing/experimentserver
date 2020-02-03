import typing

from transitions import EventData

from ..base.core import Hardware
from experimentserver.data.gas import _GAS_CONSTANTS


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class GE50MassFlowController(Hardware):
    def __init__(self, identifier: str, address: str, balance: str = 'nitrogen',
                 composition: typing.Optional[typing.Dict[str, str]] = None):
        super(GE50MassFlowController, self).__init__(identifier)

        self._address = address

        # Gas properties
        self._balance = balance
        self._composition = composition

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'MKS GE50A Mass Flow Controller'

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        pass

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        pass

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        pass

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        pass

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        pass
