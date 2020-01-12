from __future__ import annotations

import abc
import contextlib
import enum
import functools
import time
import threading
import typing

import pyvisa
import pyvisa.constants
import pyvisa.errors
import pyvisa.resources.messagebased as messagebased
from transitions import EventData

from . import Hardware
from experimentserver.hardware.error import CommunicationError, HardwareError
from experimentserver.util.logging import LoggerObject


# Disable pyvisa logging (avoids duplication)
pyvisa.logger.disabled = True


# Error codes may be integers, strings or a combination of both
TYPE_ERROR = typing.Union[str, typing.Tuple[int, str]]


# Exceptions
class VISACommunicationError(CommunicationError):
    """ Wrapper for VISA I/O errors that occur during transactions. """
    pass


class VISAHardwareError(HardwareError):
    """ Wrapper for VISA command errors that occur during transactions. """
    pass


# Enums
class VISAEnum(enum.Enum):
    """ An enum base class useful for VISA _fields with a fixes number of valid values. """

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[VISAEnum, typing.List[typing.Any]]]:
        """ Get a mapping of VISAEnums to string aliases that might be used for recognition.

        Implementation of this method is optional.

        :return: dict
        """
        return None

    @classmethod
    def _get_description_map(cls) -> typing.Dict[VISAEnum, str]:
        """ Get a mapping of VISAEnums to description strings.

        :return: dict
        """
        raise NotImplementedError()

    def get_description(self) -> str:
        """ Get a user readable description of this VISAEnum.

        :return: str
        """
        description_map = self._get_description_map()

        return description_map[self]

    @staticmethod
    def get_tag_name() -> typing.Optional[str]:
        """ Provide an optional tag name so this VISAEnum can be used to supply export _tags.

        Implementation of this method is optional.

        :return:
        """
        return None

    @classmethod
    def _get_tag_map(cls) -> typing.Optional[typing.Dict[VISAEnum, typing.Any]]:
        """ Get a mapping of VISAEnums to tag values.

        Implementation of this method is optional.

        :return: dict
        """
        return None

    def get_tag_value(self) -> str:
        """ Get the tag value for this VISAEnum.

        :return: str, int, float
        """
        tag_map = self._get_tag_map()

        return tag_map[self]

    @classmethod
    def _get_visa_map(cls) -> typing.Dict[VISAEnum, str]:
        """ Get a mapping of VISAEnums to tag VISA strings. This is used for conversion to str in VISA calls and vice
        versa.

        :return: dict
        """
        raise NotImplementedError()

    def get_visa(self) -> str:
        """ Get the VISA string for this VISAEnum.

        :return: string representing the enum value in VISA transmissions
        """
        visa_map = self._get_visa_map()

        return visa_map[self]

    @classmethod
    def from_input(cls, x: typing.Any, allow_direct: bool = False, allow_alias: bool = True,
                   allow_visa: bool = True) -> VISAEnum:
        """ Cast from string to VISAEnum.

        :param x: str input
        :param allow_direct: if True then direct conversion will be attempted (defaults to False)
        :param allow_alias: if True then conversion from alias will be attempted (defaults to True)
        :param allow_visa: if True then conversion from VISA string will be attempted (defaults to True)
        :return: VISAEnum
        :raises ValueError: when no match is found
        """
        # Ignore existing enums
        if issubclass(type(x), VISAEnum):
            return x

        if allow_direct:
            try:
                return cls[x]
            except KeyError:
                pass

        if type(x) is str:
            x = x.strip()

            try:
                x = int(x)
            except ValueError:
                pass

        if allow_alias:
            # Test alias map if it exists
            alias_map = cls._get_alias_map()

            if type(x) is str:
                x = x.lower()

            if alias_map is not None:
                for key, value in alias_map.items():
                    if x in value:
                        return key

        if allow_visa:
            # Test against VISA strings
            visa_map = cls._get_visa_map()

            for key, value in visa_map.items():
                if x == value.lower():
                    return key

        raise ValueError(f"Unknown VISAEnum string {x}")

    def __str__(self):
        return self.get_description()


TYPE_ENUM = typing.TypeVar('TYPE_ENUM', bound=VISAEnum)
TYPE_ENUM_ARGUMENT = typing.Union[str, int, TYPE_ENUM]


# Classes
class VISAHardware(Hardware, metaclass=abc.ABCMeta):
    class _VISATransaction(LoggerObject):
        _transaction_number = 1
        _thansaction_number_lock = threading.RLock()

        def __init__(self, parent: VISAHardware, timeout: typing.Optional[float], error_check: bool, error_raise: bool):
            super().__init__()

            # Handle to parent object
            self._parent = parent

            self._timeout = timeout
            self._timeout_enter = None

            # Error check enabled flag
            self._error_check = error_check
            self._error_raise = error_raise

            # Unique identifier for transaction
            self._number = self._get_number()

            # Command cache for debugging
            self._command_history: typing.List[str] = []

        @classmethod
        def _get_number(cls):
            with cls._thansaction_number_lock:
                transaction_number = cls._transaction_number
                cls._transaction_number += 1

            return transaction_number

        def __enter__(self):
            # Get lock
            self._parent._hardware_lock.acquire()

            if self._parent._visa_resource is None:
                raise VISACommunicationError('VISA resource not available')

            self._logger.debug(f"VISA transaction {self._number} opened")

            # Override timeout
            if self._timeout is not None:
                self._timeout_enter = self._parent._visa_resource.timeout
                self._parent._visa_resource.timeout = 1000 * self._timeout

            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            # Release lock
            self._parent._hardware_lock.release()

            # Restore timeout
            if self._timeout_enter is not None:
                self._parent._visa_resource.timeout = self._timeout_enter

                self._timeout_enter = None

            command_history = ', '.join(self._command_history)

            if exc_type is not None:
                raise VISACommunicationError(f"Exception occurred during VISA transaction {self._number} (command "
                                             f"history: {command_history})")

            if len(self._command_history) == 0 and self._error_check:
                self._logger.debug(f"VISA transaction {self._number} error check")

                error_buffer = []
                error_message = None

                # Attempt to get error
                error = self._parent._get_visa_error(self)

                # Keep fetching errors if available
                while error is not None:
                    error_buffer.append(error)
                    error = self._parent._get_visa_error(self)

                if len(error_buffer) == 1:
                    error_message = f"VISA command resulted in error {error_buffer[0]} (command history: " \
                        f"{command_history})"
                elif len(error_buffer) > 1:
                    error_message = f"VISA command resulted in multiple multiple errors: {', '.join(error_buffer)} " \
                        f"(command history: {command_history})"

                if error_message is not None:
                    if self._error_raise:
                        raise VISAHardwareError(error_message)
                    else:
                        self._logger.warning(error_message, event=True)

            self._command_history = []

            self._logger.debug(f"VISA transaction {self._number} closed")

        # VISA command formatting
        @staticmethod
        def _visa_format_arg(x) -> str:
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
        def _visa_format_command(cls, x: str, format_args, format_kwargs):
            """

            :param x:
            :param format_args:
            :param format_kwargs:
            :return:
            """
            # Cast parameters
            format_args = list(map(cls._visa_format_arg, format_args))
            format_kwargs = {k: cls._visa_format_arg(v) for k, v in format_kwargs.items()}

            return x.format(*format_args, **format_kwargs)

        # VISA transaction operations
        def __visa_command_wrapper(func) -> typing.Callable:
            """ Decorator to wrap pyVISA exceptions in VISAException and provide rate limiting.

            :param func: method to decorate
            :return: decorated method
            """

            # noinspection PyProtectedMember
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                if self._parent._visa_rate_limit is not None and self._parent._visa_comm_timestamp is not None:
                    # Delay for minimum write interval
                    delay = self._visa_rate_limit - (self._visa_comm_timestamp - time.time())

                    if delay > 0:
                        # self._logger.debug(f"Rate limited, waiting {delay} s")
                        time.sleep(delay)
                try:
                    result = func(self, *args, **kwargs)

                    # Save timestamp for current operation
                    self._parent._visa_comm_timestamp = time.time()

                    return result
                except pyvisa.errors.Error as exc:
                    raise VISACommunicationError('VISA library error occurred during communication') from exc

            return wrapper

        @__visa_command_wrapper
        def write(self, command: str, *format_args, visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                  **format_kwargs) -> int:
            """ Write command to VISA resource.

            Transaction lock is always held when this method is called.

            :param command:
            :param format_args:
            :param visa_kwargs:
            :param format_kwargs:
            :return:
            :raises VISACommunicationError: on VISA I/O error
            """
            command = self._visa_format_command(command, format_args, format_kwargs)
            visa_kwargs = visa_kwargs or {}

            self._command_history.append(command)

            if len(visa_kwargs) == 0:
                self._logger.debug(f"VISA write {command!r}")
            else:
                self._logger.debug(f"VISA write {command!r} (args: {visa_kwargs!s})")

            return self._parent._visa_resource.write(command, **visa_kwargs)

        @__visa_command_wrapper
        def write_binary(self, command: str, payload: typing.Iterable, *format_args,
                         visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                         **format_kwargs) -> int:
            """ Write binary payload to VISA resource.

            Transaction lock is always held when this method is called.

            :param command:
            :param payload:
            :param format_args:
            :param visa_kwargs:
            :param format_kwargs:
            :return:
            :raises VISACommunicationError: on VISA I/O error
            """
            command = self._visa_format_command(command, format_args, format_kwargs)
            visa_kwargs = visa_kwargs or {}

            self._command_history.append(command)

            if len(visa_kwargs) == 0:
                self._logger.debug(f"VISA write binary {command!r} (payload: {payload!r})")
            else:
                self._logger.debug(f"VISA write binary {command!r} (payload: {payload!r}) (args: {visa_kwargs!s})")

            return self._parent._visa_resource.write_binary_values(command, payload, **visa_kwargs)

        @__visa_command_wrapper
        def query(self, command: typing.Optional[str], *format_args, binary: bool = False,
                  visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                  **format_kwargs) -> str:
            """ Send query to VISA resource, return the string sent in reply.

            Transaction lock is always held when this method is called.

            :param command:
            :param format_args:
            :param binary:
            :param visa_kwargs:
            :param format_kwargs:
            :return: query response
            :raises VISACommunicationError: on VISA I/O error
            """
            visa_kwargs = visa_kwargs or {}

            if command is None:
                if not binary:
                    response = self._parent._visa_resource.read(**visa_kwargs).strip()
                else:
                    response = self._parent._visa_resource.read_binary_values(**visa_kwargs)
            else:
                command = self._visa_format_command(command, format_args, format_kwargs)

                self._command_history.append(command)

                if len(visa_kwargs) == 0:
                    self._logger.debug(f"VISA query: {command!r}")
                else:
                    self._logger.debug(f"VISA query: {command!r} (args: {visa_kwargs!r})")

                if not binary:
                    response = self._parent._visa_resource.query(command, **visa_kwargs).strip()
                else:
                    response = self._parent._visa_resource.query_binary_values(command, **visa_kwargs)

            self._logger.debug(f"VISA response {response!r}")

            return response

        __visa_command_wrapper = staticmethod(__visa_command_wrapper)

    """ Base class for instruments relying upon the VISA communication layer. """
    # VISA transaction lock timeout
    _VISA_TRANSACTION_LOCK_TIMEOUT = 30

    # VISA resource state
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

        # Rate limiting time stamp
        self._visa_transaction_timestamp = None

        # Instance of VISA resource
        self._visa_resource: typing.Optional[messagebased.MessageBasedResource] = None

    def get_visa_address(self) -> str:
        """
        Get the VISA address of this hardware instance.

        :return: string containing the target VISA address
        """
        return self._visa_address

    @classmethod
    def get_visa_resource_manager(cls) -> pyvisa.ResourceManager:
        """ Get the VISA resource state. Typically only used by objects to connect to hardware.

        :return: ResourceManager
        """
        # Lock required otherwise memory access violations occur (although it's supposed to be thread safe)
        with cls.__resource_manager_lock:
            if cls.__resource_manager is None:
                cls.__resource_manager = pyvisa.ResourceManager()

                cls._get_class_logger().info(f"VISA library: {cls.__resource_manager.visalib}")

        return cls.__resource_manager

    @classmethod
    @abc.abstractmethod
    def _get_visa_error(cls, transaction: VISAHardware._VISATransaction) -> typing.Optional[TYPE_ERROR]:
        """

        :param transaction:
        :return:
        """
        pass

    def visa_transaction(self, timeout: typing.Optional[float] = None, error_check: bool = True,
                         error_raise: bool = True) -> VISAHardware._VISATransaction:
        """

        :param timeout:
        :param error_check:
        :param error_raise:
        :return:
        """
        return self._VISATransaction(self, timeout, error_check, error_raise)

    def _visa_disconnect(self):
        if self._visa_resource is None:
            self._logger.warning('VISA resource must be connected before requesting disconnect')
            return

        # Close resource
        self._visa_resource.close()

        # Free resource
        self._visa_resource = None

        self._logger.info(f"Released VISA resource {self._visa_address}")

    # State transition handling
    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(VISAHardware, self).transition_connect(event)

        if self._visa_resource is not None:
            self._visa_disconnect()

        try:
            resource = self.get_visa_resource_manager().open_resource(self._visa_address, **self._visa_open_args)
        except pyvisa.errors.VisaIOError as exc:
            if exc.error_code == pyvisa.constants.VI_ERROR_TMO:
                raise VISACommunicationError(f"VISA communication timeout while connecting to {self._visa_address}") \
                    from exc
            elif exc.error_code == pyvisa.errors.StatusCode.error_resource_not_found:
                raise VISACommunicationError(f"VISA resource {self._visa_address} not found") from exc
            elif exc.error_code == pyvisa.errors.StatusCode.error_resource_busy:
                raise VISACommunicationError(f"VISA resource {self._visa_address} in use by another process") from exc
            else:
                raise VISACommunicationError(f"Unexpected VISA error occurred during acquisition of "
                                             f"{self._visa_address}") from exc

        if not issubclass(type(resource), messagebased.MessageBasedResource):
            raise VISACommunicationError(self, 'Non-message based VISA resources are unsupported')

        # Clear existing data
        resource.clear()

        # Set optional timeout
        if self._visa_timeout is not None:
            resource.timeout = self._visa_timeout * 1000

        self._visa_resource = resource

        self._logger.info(f"Acquired VISA resource {self._visa_address}")

        # Clear existing errors
        with self.visa_transaction(error_check=False) as transaction:
            self._logger.debug('Clearing existing errors from queue')

            error = self._get_visa_error(transaction)

            while error is not None:
                self._logger.warning(f"Existing instrument errors found: {error}")
                error = self._get_visa_error(transaction)

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._visa_disconnect()

        super(VISAHardware, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(VISAHardware, self).transition_configure(event)

        assert self._visa_resource is not None

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        assert self._visa_resource is not None

        super(VISAHardware, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._visa_disconnect()

        super(VISAHardware, self).transition_error(event)

    # Hardware overrides
    def get_hardware_instance_description(self) -> str:
        # Append VISA resource address to the class description
        return f"{self.get_hardware_description()} ({self.get_hardware_identifier(), self.get_visa_address()})"
