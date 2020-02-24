# -*- coding: utf-8 -*-
import os.path
import typing

__app_name__ = 'experimentserver'
__version__ = '0.2.4'

__author__ = 'Chris Harrison'
__credits__ = ['Chris Harrison']

__maintainer__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'

__status__ = 'Development'


# Application root path
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))


class ApplicationException(Exception):
    """ Base exception for all custom application exceptions that occur during runtime. """
    def get_user_str(self, separator: str = '\n') -> str:
        exc = self
        exc_str = []

        while exc is not None:
            exc_str.append(str(exc))
            exc = exc.__cause__

        return separator.join(exc_str)


class MultipleException(Exception):
    """ Base exception wrapper for multiple exceptions thrown in critical section. """

    def __init__(self, msg: str, exception_list: typing.Sequence[Exception]):
        super(MultipleException, self).__init__(msg)

        self.exception_list = exception_list

    def __str__(self) -> str:
        return f"Multiple exceptions: {', '.join(map(str, self.exception_list))}"

    def __repr__(self) -> str:
        return f"[{', '.join(map(repr, self.exception_list))}]"
