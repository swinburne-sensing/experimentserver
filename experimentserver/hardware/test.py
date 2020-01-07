import typing

from transitions import EventData

from . import Hardware


# Author tag for support purposes
__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class TestHardware(Hardware):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_hardware_description() -> str:
        return 'Test Hardware'

    def handle_connect(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).handle_connect(event)

    def handle_disconnect(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).handle_disconnect(event)

    def handle_configure(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).handle_configure(event)

    def handle_cleanup(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).handle_cleanup(event)

    def handle_error(self, event: typing.Optional[EventData] = None):
        super(TestHardware, self).handle_error(event)
