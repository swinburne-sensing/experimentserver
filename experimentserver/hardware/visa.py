import abc

from . import Hardware


class VISAHardware(Hardware, metaclass=abc.ABCMeta):
    def __init__(self, *args, visa_address: str, **kwargs):
        super().__init__(*args, **kwargs)

        self._visa_address = visa_address
