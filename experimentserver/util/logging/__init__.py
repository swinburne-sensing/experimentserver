import logging
import sys
import typing


# Shortcuts
from typing import Tuple, Optional

NOTSET = logging.NOTSET
DEBUG_LOCK = logging.DEBUG - 2
DEBUG_TRANS = logging.DEBUG - 1
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

# New logging levels
logging.addLevelName(DEBUG_LOCK, 'LOCK')
logging.addLevelName(DEBUG_TRANS, 'TRANS')


class _ModifiedLogger(logging.Logger):
    # Fixes bug where base logger considers the wrapper class as the base for all function calls
    def findCaller(self, stack_info: bool = ..., stacklevel: int = ...) -> Tuple[str, int, str, Optional[str]]:
        # Fetch frame and code
        # noinspection PyProtectedMember
        f = sys._getframe(4)
        co = f.f_code

        return co.co_filename, f.f_lineno, co.co_name, None

    @staticmethod
    def _update_args(notify, event, kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}

        if notify:
            kwargs['extra'].update({'notify': True})
        else:
            kwargs['extra'].update({'notify': False})

        if event:
            kwargs['extra'].update({'event': True})
        else:
            kwargs['extra'].update({'event': False})

    def debug_lock(self, msg, *args, notify: bool = False, event: bool = False, **kwargs) -> typing.NoReturn:
        """ Debug for lock/threading level events, normally not needed unless something is very wrong.

        :param msg: passed to log
        :param args: passed to log
        :param notify: passed to log
        :param event: passed to log
        :param kwargs: passed to log
        """
        self._update_args(notify, event, kwargs)

        if self.isEnabledFor(DEBUG_LOCK):
            self.log(DEBUG_TRANS, msg, *args, **kwargs)

    def debug_transaction(self, msg, *args, notify: bool = False, event: bool = False, **kwargs) -> typing.NoReturn:
        """ Debug for transaction level events, normally not needed unless something is very wrong.

        :param msg: passed to log
        :param args: passed to log
        :param notify: passed to log
        :param event: passed to log
        :param kwargs: passed to log
        """
        self._update_args(notify, event, kwargs)

        if self.isEnabledFor(DEBUG_TRANS):
            self.log(DEBUG_TRANS, msg, *args, **kwargs)

    def debug(self, msg, *args, notify: bool = False, event: bool = False, **kwargs):
        self._update_args(notify, event, kwargs)
        super(_ModifiedLogger, self).debug(msg, *args, **kwargs)

    def info(self, msg, *args, notify: bool = False, event: bool = False, **kwargs):
        self._update_args(notify, event, kwargs)
        super(_ModifiedLogger, self).info(msg, *args, **kwargs)

    def warning(self, msg, *args, notify: bool = False, event: bool = True, **kwargs):
        self._update_args(notify, event, kwargs)
        super(_ModifiedLogger, self).warning(msg, *args, **kwargs)

    def error(self, msg, *args, notify: bool = True, event: bool = True, **kwargs):
        self._update_args(notify, event, kwargs)
        super(_ModifiedLogger, self).error(msg, *args, **kwargs)

    def exception(self, msg, *args, notify: bool = True, event: bool = True, exc_info=True, **kwargs):
        self._update_args(notify, event, kwargs)
        super(_ModifiedLogger, self).exception(msg, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg, *args, notify: bool = True, event: bool = True, **kwargs):
        self._update_args(notify, event, kwargs)
        super(_ModifiedLogger, self).critical(msg, *args, **kwargs)

    def log(self, level, msg, *args, notify: bool = False, event: bool = False, **kwargs):
        self._update_args(notify, event, kwargs)
        super(_ModifiedLogger, self).log(level, msg, *args, **kwargs)


# Use derived logger class
logging.setLoggerClass(_ModifiedLogger)


def get_logger(name: typing.Optional[str] = None) -> _ModifiedLogger:
    """ Get a wrapped Logging object.

    :param name: name of the Logger to get from the logging library
    :return: logger
    """
    logger = logging.getLogger(name)
    logger = typing.cast(_ModifiedLogger, logger)

    # Ensure logger is enabled
    logger.disabled = False

    return logger


class LoggerClass(object):
    """ Base class for classes that needs to access to a logger. """
    __class_logger = None

    @classmethod
    def get_class_logger(cls):
        if cls.__class_logger is None:
            cls.__class_logger = get_logger(cls.__name__)

        return cls.__class_logger


class LoggerObject(LoggerClass):
    """ Base class for objects that needs to access to a logger. """
    def __init__(self, logger_name_prefix: typing.Optional[str] = None, logger_name: typing.Optional[str] = None,
                 logger_name_postfix: typing.Optional[str] = None):
        """
        Creates a new object that has a logging capability. An optional custom name and/or a custom name may be
        appended to the fetched logger.

        :param logger_name: string to use as the logger name, defaults to the class name
        :param logger_name_postfix: string to append to the logger name
        """
        LoggerClass.__init__(self)

        # If no name is provided then used the class name as the logger name
        logger_name = (logger_name_prefix or '') + (logger_name or self.__class__.__name__) + \
                      (logger_name_postfix or '')

        # Get a logger based upon this name
        self.__logger = get_logger(logger_name)

    def get_logger(self) -> _ModifiedLogger:
        return self.__logger
