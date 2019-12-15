from __future__ import annotations

import abc
import enum
import functools
import time
import threading
import typing

import pyvisa
import pyvisa.constants
import pyvisa.errors
import pyvisa.resources.messagebased as messageresource
from transitions import EventData

from . import Hardware, HardwareException


class VISAException(HardwareException):
    """ Wrapper for VISA I/O errors that occur during transactions. """
    pass


class VISAEnum(enum.Enum):
    """ An enum base class useful for VISA fields with a fixes number of valid values. """

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[VISAEnum, typing.List[str]]]:
        return None

    @classmethod
    def _get_description_map(cls) -> typing.Dict[VISAEnum, str]:
        raise NotImplementedError()

    def get_description(self) -> str:
        """ Get the user readable description for a VISAEnum value.

        :param x: VISAEnum value to get the description for
        :return: string describing the value in a user readable form
        """
        description_map = self._get_description_map()

        return description_map[self]

    @staticmethod
    def get_tag_name() -> str:
        raise NotImplementedError()

    @classmethod
    def _get_tag_map(cls) -> typing.Dict[VISAEnum, typing.Any]:
        raise NotImplementedError()

    def get_tag_value(self) -> str:
        """

        :return:
        """
        tag_map = self._get_tag_map()

        return tag_map[self]

    @classmethod
    def _get_visa_map(cls) -> typing.Dict[VISAEnum, str]:
        raise NotImplementedError()

    def get_visa(self) -> str:
        """ Get the VISA string for a VISAEnum value.

        :param x: VISAEnum value to get the VISA string for
        :return: string representing the enum value in VISA transmissions
        """
        visa_map = self._get_visa_map()

        return visa_map[self]

    @classmethod
    def from_input(cls, x: typing.Any) -> VISAEnum:
        if type(x) is str:
            x = x.strip()

        try:
            return cls[x]
        except KeyError:
            # Test alias map if it exists
            alias_map = cls._get_alias_map()

            if type(x) is str:
                x = x.lower()

            if alias_map is not None:
                for key, value in alias_map.items():
                    if x in value:
                        return key

            # Test against VISA strings
            visa_map = cls._get_visa_map()

            for key, value in visa_map.items():
                if x == value.lower():
                    return key

        raise ValueError(f"Unknown VISA string {x}")

    def __str__(self):
        return self.get_description(self)


class VISAHardware(Hardware, metaclass=abc.ABCMeta):
    """ Base class for instruments relying upon the VISA communication layer. """
    __resource_manager = None
    __resource_manager_lock = threading.RLock()

    def __init__(self, identifier: str, visa_address: str, *args, visa_open_args: typing.Dict[str, typing.Any] = None,
                 visa_timeout: typing.Optional[float] = None, visa_rate_limit: typing.Optional[float] = None, **kwargs):
        """

        :param args:
        :param visa_address:
        :param visa_open_args:
        :param visa_timeout:
        :param visa_rate_limit:
        :param kwargs:
        """
        super().__init__(identifier)

        self._visa_address = visa_address
        self._visa_open_args = visa_open_args or {}
        self._visa_timeout = visa_timeout
        self._visa_rate_limit = visa_rate_limit

        # Timestamp for rate limiting
        self._visa_last_command = None
        self._visa_comm_timestamp = None

        # Instance of VISA resource
        self._visa_resource: typing.Optional[messageresource.MessageBasedResource] = None

        # Transaction lock
        self._visa_transaction_lock = threading.RLock()

    def get_visa_address(self) -> str:
        """
        Get the VISA address of this hardware instance.

        :return: string containing the target VISA address
        """
        return self._visa_address

    @classmethod
    def get_visa_resource_manager(cls):
        with cls.__resource_manager_lock:
            if cls.__resource_manager is None:
                cls.__resource_manager = pyvisa.ResourceManager()

                cls._get_class_logger().info(f"VISA library: {cls.__resource_manager.visalib}")

        return cls.__resource_manager

    @classmethod
    def get_visa_resources(cls) -> typing.List[str]:
        """
        Get a list of all available VISA resources.

        :return: list containing all available VISA resource addresses
        """
        return cls.get_visa_resource_manager().list_resources()

    def _comm_wrapper(func) -> typing.Callable:
        """ Decorator to wrap pyVISA exceptions in VISAException.

        :param func: method to decorate
        :return: decorated method
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Throw exception if instrument is not setup
            if self._visa_resource is None:
                raise VISAException(f"Not connected to VISA resource")

            # apply message rate limiting if enabled
            if self._visa_rate_limit is not None and self._visa_comm_timestamp is not None:
                # Delay for minimum write interval
                delay = self._visa_rate_limit - (self._visa_comm_timestamp - time.time())

                if delay > 0:
                    self._logger.debug(f"Rate limited, waiting {delay} s")
                    time.sleep(delay)

            try:
                result = func(self, *args, **kwargs)

                # Save timestamp for current operation
                self._visa_comm_timestamp = time.time()

                return result
            except pyvisa.errors.VisaIOError as exc:
                raise VISAException('VISA error occurred during communication') from exc

        return wrapper

    @staticmethod
    def cast_arg(x) -> str:
        """ Cast value to a VISA friendly string.

        :param x: input variable
        :return: string representation of that variable
        """
        if issubclass(type(x), VISAEnum):
            return x.get_visa()
        elif type(x) is bool:
            return 'ON' if x else 'OFF'
        elif type(x) is str:
            return f"\"{x}\""
        else:
            return x

    @classmethod
    def _format_command(cls, x: str, format_args, format_kwargs):
        """

        :param x:
        :param format_args:
        :param format_kwargs:
        :return:
        """
        # Cast parameters
        format_args = list(map(cls.cast_arg, format_args))
        format_kwargs = {k: cls.cast_arg(v) for k, v in format_kwargs.items()}

        return x.format(*format_args, **format_kwargs)

    @_comm_wrapper
    def visa_write(self, command: str, *format_args, visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                   remember_last: bool = True, **format_kwargs) -> int:
        """ Write command to VISA resource.

        :param command:
        :param format_args:
        :param visa_kwargs:
        :param remember_last:
        :param format_kwargs:
        :return:
        :raises VISAException: on VISA I/O error
        """
        command = self._format_command(command, format_args, format_kwargs)
        visa_kwargs = visa_kwargs or {}

        if len(visa_kwargs) == 0:
            self._logger.debug(f"Write {command!r}")
        else:
            self._logger.debug(f"Write {command!r} (args: {visa_kwargs!s})")

        if remember_last:
            self._visa_last_command = command

        return self._visa_resource.write(command, **visa_kwargs)

    @_comm_wrapper
    def visa_write_binary(self, command: str, payload: typing.Iterable, *format_args,
                          visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None, remember_last: bool = True,
                          **format_kwargs) -> int:
        """ Write binary payload to VISA resource.

        :param command:
        :param payload:
        :param format_args:
        :param visa_kwargs:
        :param remember_last:
        :param format_kwargs:
        :return:
        :raises VISAException: on VISA I/O error
        """
        command = self._format_command(command, format_args, format_kwargs)
        visa_kwargs = visa_kwargs or {}

        if len(visa_kwargs) == 0:
            self._logger.debug(f"Write binary {command!r} (payload: {payload!r})")
        else:
            self._logger.debug(f"Write binary {command!r} (payload: {payload!r}) (args: {visa_kwargs!s})")

        if remember_last:
            self._visa_last_command = command

        return self._visa_resource.write_binary_values(command, payload, **visa_kwargs)

    @_comm_wrapper
    def visa_query(self, command: typing.Optional[str], *format_args, binary: bool = False,
                   visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None, remember_last: bool = True,
                   **format_kwargs) -> str:
        """ Send query to VISA resource, return the string sent in reply.

        :param command:
        :param format_args:
        :param binary:
        :param visa_kwargs:
        :param remember_last:
        :param format_kwargs:
        :return: query response
        :raises VISAException: on VISA I/O error
        """
        visa_kwargs = visa_kwargs or {}

        if command is None:
            if not binary:
                response = self._visa_resource.read(**visa_kwargs).strip()
            else:
                response = self._visa_resource.read_binary_values(**visa_kwargs)

            if remember_last:
                self._visa_last_command = None
        else:
            command = self._format_command(command, format_args, format_kwargs)

            if len(visa_kwargs) == 0:
                self._logger.debug(f"Query: {command!r}")
            else:
                self._logger.debug(f"Query: {command!r} (args: {visa_kwargs!r})")

            if remember_last:
                self._visa_last_command = command

            if not binary:
                response = self._visa_resource.query(command, **visa_kwargs).strip()
            else:
                response = self._visa_resource.query_binary_values(command, **visa_kwargs)

        self._logger.debug(f"Response {response!r}")

        return response

    def _visa_connect(self):
        try:
            resource = self.get_visa_resource_manager().open_resource(self._visa_address, **self._visa_open_args)
        except pyvisa.errors.VisaIOError as exc:
            if exc.error_code == pyvisa.constants.VI_ERROR_TMO:
                raise VISAException(f"VISA communication timeout while connecting to {self._visa_address}") \
                    from exc
            elif exc.error_code == pyvisa.errors.StatusCode.error_resource_not_found:
                raise VISAException(f"VISA resource {self._visa_address} not found") from exc
            elif exc.error_code == pyvisa.errors.StatusCode.error_resource_busy:
                raise VISAException(f"VISA resource {self._visa_address} in use by another process") from exc
            else:
                raise VISAException(f"Unexpected VISA error occurred during acquisition of {self._visa_address}") \
                    from exc

        if not issubclass(type(resource), messageresource.MessageBasedResource):
            raise VISAException(self, 'Only message based VISA resources are supported')

        # Set optional timeout
        if self._visa_timeout is not None:
            resource.timeout = self._visa_timeout * 1000

        self._visa_resource = resource

        self._logger.info(f"Acquired VISA resource {self._visa_address}")

    def _visa_disconnect(self):
        # Close resource
        self._visa_resource.close()

        # Free resource
        self._visa_resource = None

        self._logger.info(f"Released VISA resource {self._visa_address}")

    @abc.abstractmethod
    def handle_setup(self, event: EventData):
        super().handle_setup(event)

        # Setup should not be called multiple times
        assert self._visa_resource is None

        # Connect to VISA resource
        self._visa_connect()

    @abc.abstractmethod
    def handle_start(self, event: EventData):
        super().handle_start(event)

        assert self._visa_resource is not None

    @abc.abstractmethod
    def handle_pause(self, event: EventData):
        super().handle_pause(event)

        assert self._visa_resource is not None

    @abc.abstractmethod
    def handle_resume(self, event: EventData):
        super().handle_resume(event)

        assert self._visa_resource is not None

    @abc.abstractmethod
    def handle_stop(self, event: EventData):
        super().handle_stop(event)

        assert self._visa_resource is not None

    @abc.abstractmethod
    def handle_cleanup(self, event: EventData):
        assert self._visa_resource is not None

        self._visa_disconnect()

        super().handle_cleanup(event)

    @abc.abstractmethod
    def handle_error(self, event: EventData):
        super().handle_error(event)

        assert self._visa_resource is not None

        # Disconnect instrument
        self._visa_disconnect()

    def get_hardware_instance_description(self) -> str:
        # Append VISA resource address to the class description
        return f"{self.get_hardware_description()} ({self.get_hardware_identifier(), self.get_visa_address()})"

    _comm_wrapper = staticmethod(_comm_wrapper)
