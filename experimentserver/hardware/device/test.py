import typing

from transitions import EventData

from .. import Hardware

# Author tag for support purposes
__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class TestHardware(Hardware):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Test Hardware'

    def transition_connect(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).transition_connect(event)

    def transition_disconnect(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).transition_error(event)
