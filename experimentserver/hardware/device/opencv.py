import typing

# import cv2
from transitions import EventData

from ..base.core import Hardware
from ..metadata import TYPE_PARAMETER_DICT


class ImageCapture(Hardware):
    def __init__(self, identifier: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super().__init__(identifier, parameters)

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'OpenCV Image Capture'

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

    @Hardware.register_parameter(description='Trigger capture')
    def trigger(self):
        pass


class VideoCapture(Hardware):
    def __init__(self, identifier: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super().__init__(identifier, parameters)

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'OpenCV Video Capture'

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

    @Hardware.register_parameter(description='Trigger capture')
    def trigger(self):
        pass

