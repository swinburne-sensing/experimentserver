# -*- coding: utf-8 -*-

"""
experimentserver is a tool to facilitate the running of gas sensing (among other) experiments. It is comprised of
several key modules for data handling (data), hardware interaction (hardware), interfacing to external libraries
(interface), experimental protocol (protocol), and user interface (ui).
"""

import os.path
import typing


__app_name__ = 'experimentserver'
__version__ = '1.2.0'

__author__ = 'Chris Harrison'
__credits__ = ['Chris Harrison']

__maintainer__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'

__status__ = 'Production'


# Exit codes
EXIT_SUCCESS = 0
EXIT_ERROR_ARGUMENTS = -1
EXIT_ERROR_EXCEPTION = -2


# Application root path
APP_PATH = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(os.path.expanduser('~'), f".{__app_name__}")

CONFIG_DEFAULT = os.path.abspath(os.path.join(APP_PATH, '..', 'config/experiment.yaml'))


class ApplicationException(Exception):
    def __init__(self, *args, fatal: bool = False):
        super(ApplicationException, self).__init__(*args)

        self.fatal = fatal

    """ Base exception for all custom application exceptions that occur during runtime. """
    def get_user_str(self, separator: str = '\n') -> str:
        current_exc: typing.Optional[BaseException] = self
        exc_str = []

        while current_exc is not None:
            exc_str.append(str(current_exc))
            current_exc = current_exc.__cause__

        return separator.join(exc_str)


class MultipleException(ApplicationException):
    """ Base exception wrapper for multiple exceptions thrown in critical section. """

    def __init__(self, msg: str, exception_list: typing.Sequence[Exception]):
        super(MultipleException, self).__init__(msg)

        self.exception_list = exception_list

    def __str__(self) -> str:
        return f"Multiple exceptions: {', '.join(map(str, self.exception_list))}"

    def __repr__(self) -> str:
        return f"[{', '.join(map(repr, self.exception_list))}]"
