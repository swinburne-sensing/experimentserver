import logging
import sys

from wrapt import ObjectProxy


class _ModifiedLogger(logging.Logger):
    # Fixes bug where logger considered the wrapper class as the base of the call (so everything came from this file)
    def findCaller(self, stack_info=False):
        # Fetch frame and code
        f = sys._getframe(4)
        co = f.f_code

        return co.co_filename, f.f_lineno, co.co_name, None


# noinspection PyAbstractClass
class __LoggerProxy(ObjectProxy):
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

    def debug(self, msg, *args, notify: bool = False, event: bool = False, **kwargs):
        self._update_args(notify, event, kwargs)
        self.__wrapped__.disabled = False
        self.__wrapped__.debug(msg, *args, **kwargs)

    def info(self, msg, *args, notify: bool = False, event: bool = False, **kwargs):
        self._update_args(notify, event, kwargs)
        self.__wrapped__.disabled = False
        self.__wrapped__.info(msg, *args, **kwargs)

    def warning(self, msg, *args, notify: bool = True, event: bool = True, **kwargs):
        self._update_args(notify, event, kwargs)
        self.__wrapped__.disabled = False
        self.__wrapped__.warning(msg, *args, **kwargs)

    def error(self, msg, *args, notify: bool = True, event: bool = True, **kwargs):
        self._update_args(notify, event, kwargs)
        self.__wrapped__.disabled = False
        self.__wrapped__.error(msg, *args, **kwargs)

    def exception(self, msg, *args, notify: bool = True, event: bool = True, exc_info=True, **kwargs):
        self._update_args(notify, event, kwargs)
        self.__wrapped__.disabled = False
        self.__wrapped__.exception(msg, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg, *args, notify: bool = True, event: bool = True, **kwargs):
        self._update_args(notify, event, kwargs)
        self.__wrapped__.disabled = False
        self.__wrapped__.critical(msg, *args, **kwargs)

    def log(self, level, msg, *args, notify: bool = False, event: bool = False, **kwargs):
        self._update_args(notify, event, kwargs)
        self.__wrapped__.disabled = False
        self.__wrapped__.log(level, msg, *args, **kwargs)


def get_logger(name: str) -> __LoggerProxy:
    """ Get a wrapped Logging object.

    :param name: name of the Logger to get from the logging library
    :return: LoggerWrapper
    """
    logger = logging.getLogger(name)

    # Ensure logger is enabled
    logger.disabled = False

    # Change to derived class
    logger.__class__ = _ModifiedLogger

    return __LoggerProxy(logger)


class LoggerObject(object):
    """ Base class for objects that needs to access to a logger. """
    __class_logger = None

    def __init__(self, logger_name: str = None, logger_append: str = None):
        """
        Creates a new object that has a logging capability. An optional custom name and/or a custom name may be
        appended to the fetched logger.

        :param logger_name: string to use as the logger name, defaults to the class name
        :param logger_append: string to append to the logger name
        """
        super().__init__()

        # If no name is provided then used the class name as the logger name
        if logger_name is None:
            logger_name = self.__class__.__name__
            # logger_name = self.__class__.__module__ + '.' + self.__class__.__qualname__

        if logger_append is not None:
            logger_name += logger_append

        # Get a logger based upon this name
        self._logger = get_logger(logger_name)

    @classmethod
    def _get_class_logger(cls):
        if cls.__class_logger is None:
            cls.__class_logger = get_logger(cls.__name__)

        return cls.__class_logger
