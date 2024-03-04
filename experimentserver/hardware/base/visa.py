from __future__ import annotations

import abc
import functools
import time
import threading
import typing
from types import TracebackType

import pyvisa
import pyvisa.constants
import pyvisa.errors
import pyvisa.resources.messagebased
from experimentlib.data.unit import T_PARSE_UNIT, Quantity, parse
from experimentlib.logging.classes import Logged
from transitions import EventData

from .core import Hardware
from .enum import HardwareEnum
from ..error import CommunicationError, ExternalError
from ..metadata import TYPE_PARAMETER_DICT


# Disable pyvisa logging (avoids duplication)
pyvisa.logger.disabled = True


# Error codes may be integers, strings or a combination of both
TYPE_ERROR = typing.Union[str, typing.Tuple[int, str]]


# Decorator type hints
TParam = typing.ParamSpec('TParam')
TReturn = typing.TypeVar('TReturn')


# Exceptions
class VISACommunicationError(CommunicationError):
    """ Wrapper for VISA I/O errors that occur during transactions. """
    pass


class VISAExternalError(ExternalError):
    """ Wrapper for VISA command errors that occur during transactions. """
    pass


# Classes
class VISAHardware(Hardware, metaclass=abc.ABCMeta):
    """ Base class for instruments relying upon the VISA communication layer. """

    class VISATransaction(Logged):
        """
        Internal wrapper class for VISA transactions protected by a lock. Manages error checking and rate limiting.
        """

        _transaction_number = 1
        _transaction_number_lock = threading.RLock()

        def __init__(self, parent: VISAHardware, timeout: typing.Optional[float], error_check: bool, error_raise: bool):
            # Unique identifier for transaction
            self._transaction_number = self._get_number()

            Logged.__init__(self, f"{parent.get_hardware_identifier()}:{self._transaction_number}")

            # Handle to parent object
            self._parent = parent

            assert parent._visa_resource is not None
            self._visa_resource: pyvisa.resources.MessageBasedResource = parent._visa_resource
    
            self._timeout = timeout
            self._timeout_enter: typing.Optional[float] = None

            # Error check enabled flag
            self._error_check = error_check
            self._error_raise = error_raise

            # Command cache for debugging
            self._command_history: typing.List[str] = []

        @classmethod
        def _get_number(cls) -> int:
            with cls._transaction_number_lock:
                transaction_number = cls._transaction_number
                cls._transaction_number += 1

            return transaction_number

        def __enter__(self) -> VISAHardware.VISATransaction:
            # Get lock
            self._parent._hardware_lock.acquire()

            if self._parent._visa_resource is None:
                raise VISACommunicationError('Resource not available')

            self.logger().comm('Opened')

            # Override timeout
            if self._timeout is not None:
                self._timeout_enter = self._parent._visa_resource.timeout
                self._parent._visa_resource.timeout = 1000 * self._timeout

            return self

        def __exit__(self, exc_type: typing.Type[BaseException], exc_val: BaseException, exc_tb):
            # Release lock
            self._parent._hardware_lock.release()

            command_history = ', '.join(self._command_history)

            if exc_type is not None:
                # Restore timeout
                if self._timeout_enter is not None:
                    self._parent._visa_resource.timeout = self._timeout_enter  # type: ignore

                    self._timeout_enter = None

                raise VISACommunicationError(f"Exception in transaction #{self._transaction_number} (history: "
                                             f"{command_history})") from exc_val

            if len(self._command_history) > 0 and self._error_check:
                self.logger().comm('Error check')

                error_buffer = []
                error_message = None

                # Attempt to get error
                error = self._parent._get_visa_error(self)

                # Keep fetching errors if available
                while error is not None:
                    error_buffer.append(error)
                    error = self._parent._get_visa_error(self)

                if len(error_buffer) == 1:
                    error_message = f"Command resulted in error {error_buffer[0]} (history: " \
                        f"{command_history})"
                elif len(error_buffer) > 1:
                    error_message = f"Command resulted in multiple multiple errors: {', '.join(error_buffer)} " \
                        f"(history: {command_history})"

                if error_message is not None:
                    if self._error_raise:
                        raise VISAExternalError(error_message)
                    else:
                        self.logger().warning(error_message)

            # Restore timeout
            if self._timeout_enter is not None:
                self._parent._visa_resource.timeout = self._timeout_enter

                self._timeout_enter = None

            self._command_history = []

            self.logger().comm('Closed')

        # VISA command formatting
        @staticmethod
        def _visa_format_arg(x: typing.Any) -> str:
            """ Cast value to a VISA friendly string.

            :param x: input variable
            :return: string representation of that variable
            """
            if isinstance(x, HardwareEnum):
                return x.command_value
            elif isinstance(x, bool):
                return 'ON' if x else 'OFF'
            elif isinstance(x, str):
                return f"\"{x}\""
            else:
                return str(x)

        @classmethod
        def _visa_format_command(cls, x: str, format_args: typing.Tuple[typing.Any],
                                 format_kwargs: typing.Dict[str, typing.Any]) -> str:
            """

            :param x:
            :param format_args:
            :param format_kwargs:
            :return:
            """
            # Cast parameters
            format_args_list = (cls._visa_format_arg(x) for x in format_args)
            format_kwargs = {k: cls._visa_format_arg(v) for k, v in format_kwargs.items()}

            return x.format(*format_args_list, **format_kwargs)

        # VISA transaction operations
        def __visa_command_wrapper(func: typing.Callable[TParam, TReturn]) -> typing.Callable[TParam, TReturn]:
            """ Decorator to wrap pyVISA exceptions in VISAException and provide rate limiting.

            :param func: method to decorate
            :return: decorated method
            """

            # noinspection PyProtectedMember
            @functools.wraps(func)
            def wrapper(self: VISAHardware.VISATransaction, *args: typing.Any, **kwargs: typing.Any) -> TReturn:
                if self._parent._visa_rate_limit is not None and self._parent._visa_comm_timestamp is not None:
                    # Delay for minimum write interval
                    delay = self._parent._visa_rate_limit - (time.time() - self._parent._visa_comm_timestamp)

                    if delay > 0:
                        self.sleep(delay, 'VISA rate limit')
                try:
                    result = func(self, *args, **kwargs)

                    # Save timestamp for current operation
                    self._parent._visa_comm_timestamp = time.time()

                    return result
                except pyvisa.errors.Error as exc:
                    raise VISACommunicationError('VISA library error occurred during communication') from exc

            return wrapper  # type: ignore

        @__visa_command_wrapper
        def flush(self, timeout: float = 0.1) -> None:
            # Flush transaction
            flushed_buffer = bytearray()

            # Backup existing timeout
            original_timeout = self._visa_resource.timeout

            try:
                self._visa_resource.timeout = timeout * 1000

                while True:
                    try:
                        flushed_buffer.extend(self._visa_resource.read_raw(1))
                    except pyvisa.errors.VisaIOError:
                        break
            finally:
                # Restore timeout
                self._visa_resource.timeout = original_timeout

            if len(flushed_buffer) > 0:
                self.logger().comm(f"Flushed \"{flushed_buffer}\" from VISA resource read buffer")

        @__visa_command_wrapper
        def write(self, command: str, *format_args: typing.Any,
                  visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None, **format_kwargs: typing.Any
                  ) -> int:
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

            self._command_history.append(f"w:\"{command}\"")

            if len(visa_kwargs) == 0:
                self.logger().comm(f"Write {command!r}")
            else:
                self.logger().comm(f"Write {command!r} (args: {visa_kwargs!s})")

            return self._visa_resource.write(command, **visa_kwargs)

        @__visa_command_wrapper
        def write_binary(self, command: str, payload: typing.Sequence[typing.Any], *format_args: typing.Any,
                         visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                         **format_kwargs: typing.Any) -> int:
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

            self._command_history.append(f"wb:\"{command}\"")

            if len(visa_kwargs) == 0:
                self.logger().comm(f"Write binary {command!r} (payload: {payload!r})")
            else:
                self.logger().comm(f"Write binary {command!r} (payload: {payload!r}) (args: {visa_kwargs!s})")

            return int(self._visa_resource.write_binary_values(command, payload, **visa_kwargs))
        
        @typing.overload
        def query(self, command: typing.Optional[str], *format_args: typing.Any, binary: typing.Literal[False] = False, 
                  raw: typing.Literal[False] = False, visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                  **format_kwargs: typing.Any) -> str:
            ...

        @typing.overload
        def query(self, command: typing.Optional[str], *format_args: typing.Any, binary: typing.Literal[True],
                  raw: bool = False, visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                  **format_kwargs: typing.Any) -> typing.Sequence[typing.Union[int, float]]:
            ...
        
        @typing.overload
        def query(self, command: typing.Optional[str], *format_args: typing.Any, binary: typing.Literal[False] = False, 
                  raw: typing.Literal[True], visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                  **format_kwargs: typing.Any) -> bytes:
            ...

        @__visa_command_wrapper
        def query(self, command: typing.Optional[str], *format_args: typing.Any, binary: bool = False,
                  raw: bool = False, visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                  **format_kwargs: typing.Any) -> typing.Union[typing.Sequence[typing.Union[int, float]], bytes, str]:
            """ Send query to VISA resource, return the string sent in reply.

            Transaction lock is always held when this method is called.

            :param command:
            :param format_args:
            :param binary:
            :param raw:
            :param visa_kwargs:
            :param format_kwargs:
            :return: query response
            :raises VISACommunicationError: on VISA I/O error
            """
            visa_kwargs = visa_kwargs or {}

            if command is None:
                if binary:
                    cmd_type = 'rb'
                    cmd_description = 'Read binary'
                elif raw:
                    cmd_type = 'rr'
                    cmd_description = 'Read raw'
                else:
                    cmd_type = 'r'
                    cmd_description = 'Read'
            else:
                if binary:
                    cmd_type = 'qb'
                    cmd_description = 'Query binary'
                else:
                    cmd_type = 'q'
                    cmd_description = 'Query'

                command = self._visa_format_command(command, format_args, format_kwargs)

            if len(visa_kwargs) == 0:
                self.logger().comm(f"{cmd_description}: {command!r}")
            else:
                self.logger().comm(f"{cmd_description}: {command!r} (args: {visa_kwargs!r})")

            if command is None:
                # Read-only
                self._command_history.append(cmd_type)

                if binary:
                    response_bin = self._visa_resource.read_binary_values(**visa_kwargs)
                    self.logger().comm(f"Response (binary) {response_bin!r}")
                    return response_bin
                elif raw:
                    response_raw = self._visa_resource.read_raw(**visa_kwargs)
                    self.logger().comm(f"Response (raw) {response_raw!r}")
                    return response_raw
                else:
                    response_str = self._visa_resource.read(**visa_kwargs).strip()
                    self.logger().comm(f"Response {response_str!r}")
                    return response_str
            else:
                # Query
                self._command_history.append(f"{cmd_type}:\"{command}\"")

                if binary:
                    response_bin = self._visa_resource.query_binary_values(command, delay=self._parent._visa_delay,
                                                                               **visa_kwargs)
                    self.logger().comm(f"Response (binary) {response_bin!r}")
                    return response_bin
                else:
                    response_str = self._visa_resource.query(command, delay=self._parent._visa_delay).strip()
                    self.logger().comm(f"Response {response_str!r}")
                    return response_str

        def query_unit(self, command: typing.Optional[str], unit: T_PARSE_UNIT) -> Quantity:
            response = self.query(command)

            return parse(response, unit)

        __visa_command_wrapper = staticmethod(__visa_command_wrapper)

    # VISA transaction lock timeout
    _VISA_TRANSACTION_LOCK_TIMEOUT = 30

    # VISA resource manager
    __resource_manager: typing.Optional[pyvisa.ResourceManager] = None
    __resource_manager_lock = threading.RLock()

    def __init__(self, identifier: str, visa_address: str,
                 parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 visa_delay: typing.Optional[float] = None,
                 visa_open_args: typing.Optional[typing.Dict[str, typing.Any]] = None,
                 visa_timeout: typing.Optional[float] = None, visa_rate_limit: typing.Optional[float] = None,
                 visa_connect_delay: typing.Optional[float] = None):
        """

        :param visa_address:
        :param visa_open_args:
        :param visa_timeout:
        :param visa_rate_limit:
        """
        super().__init__(identifier, parameters)

        self._visa_address = visa_address
        self._visa_delay = visa_delay
        self._visa_open_args = visa_open_args or {}
        self._visa_timeout = visa_timeout
        self._visa_rate_limit = visa_rate_limit
        self._visa_connect_delay = visa_connect_delay

        # Rate limiting time stamp
        self._visa_comm_timestamp: typing.Optional[float] = None

        # Instance of VISA resource
        self._visa_resource: typing.Optional[pyvisa.resources.messagebased.MessageBasedResource] = None

    def get_visa_address(self) -> str:
        """
        Get the VISA address of this hardware instance.

        :return: string containing the target VISA address
        """
        return self._visa_address

    @classmethod
    def get_visa_resource_manager(cls) -> pyvisa.ResourceManager:
        """ Get the VISA resource manager. Typically only used by objects to connect to hardware.

        :return: ResourceManager
        """
        # Lock required otherwise memory access violations occur (although it's supposed to be thread safe)
        with cls.__resource_manager_lock:
            if cls.__resource_manager is None:
                cls.__resource_manager = pyvisa.ResourceManager()

                # time.sleep(1)

                cls.logger().info(f"VISA library: {cls.__resource_manager.visalib}")

        return cls.__resource_manager

    @classmethod
    @abc.abstractmethod
    def _get_visa_error(cls, transaction: VISAHardware.VISATransaction) -> typing.Optional[TYPE_ERROR]:
        """

        :param transaction:
        :return:
        """
        pass

    def visa_transaction(self, timeout: typing.Optional[float] = None, error_check: bool = True,
                         error_raise: bool = True) -> VISAHardware.VISATransaction:
        """

        :param timeout:
        :param error_check:
        :param error_raise:
        :return:
        """
        return self.VISATransaction(self, timeout, error_check, error_raise)

    def _visa_disconnect(self) -> None:
        if self._visa_resource is None:
            self.logger().warning('VISA resource must be connected before requesting disconnect')
            return

        # Close resource
        try:
            self._visa_resource.close()
        except pyvisa.errors.VisaIOError as exc:
            self.logger().warning('VISA IO error occurred during resource release', exc_info=exc)

        # Free resource
        self._visa_resource = None

        self.logger().info(f"Released VISA resource {self._visa_address}")

    @classmethod
    def _visa_resource_configure(cls, resource: pyvisa.resources.messagebased.MessageBasedResource) -> None:
        pass

    # State transition handling
    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super(VISAHardware, self).transition_connect(event)

        if self._visa_resource is not None:
            self.logger().warning('Resource already connected, disconnecting')
            self._visa_disconnect()

        self.logger().comm(f"Open resource \"{self._visa_address}\" (args: {self._visa_open_args!r})")

        try:
            resource = self.get_visa_resource_manager().open_resource(self._visa_address, **self._visa_open_args)

            if not isinstance(resource, pyvisa.resources.messagebased.MessageBasedResource):
                raise VISACommunicationError(self, 'Non-message based VISA resources are unsupported')

            self._visa_resource_configure(resource)
        except pyvisa.errors.VisaIOError as exc:
            if exc.error_code == pyvisa.constants.VI_ERROR_TMO:
                raise VISACommunicationError(f"VISA communication timeout while connecting to {self._visa_address}")
            elif exc.error_code == pyvisa.errors.StatusCode.error_resource_not_found:
                raise VISACommunicationError(f"VISA resource {self._visa_address} not found")
            elif exc.error_code == pyvisa.errors.StatusCode.error_resource_busy:
                raise VISACommunicationError(f"VISA resource {self._visa_address} in use by another process")
            else:
                raise VISACommunicationError(f"Unexpected VISA error occurred during acquisition of "
                                             f"{self._visa_address}") from exc

        if not isinstance(resource, pyvisa.resources.messagebased.MessageBasedResource):
            raise VISACommunicationError(self, 'Non-message based VISA resources are unsupported')

        # Set optional timeout
        if self._visa_timeout is not None:
            resource.timeout = self._visa_timeout * 1000

        self._visa_resource = resource

        self.logger().info(f"Acquired VISA resource {self._visa_address}")

        # Clear existing data
        self._visa_resource.clear()

        if self._visa_connect_delay is not None:
            self.sleep(self._visa_connect_delay, 'visa connect delay')

        # Clear existing errors
        with self.visa_transaction(error_check=False) as transaction:
            self.logger().comm('Clearing existing errors from queue')

            error = self._get_visa_error(transaction)

            while error is not None:
                self.logger().warning(f"Existing instrument errors found: {error}")
                error = self._get_visa_error(transaction)

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        self._visa_disconnect()

        super(VISAHardware, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
        super(VISAHardware, self).transition_configure(event)

        assert self._visa_resource is not None

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        assert self._visa_resource is not None

        super(VISAHardware, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        self._visa_disconnect()

        super(VISAHardware, self).transition_error(event)

    # Hardware overrides
    def get_hardware_instance_description(self) -> str:
        # Append VISA resource address to the class description
        return f"{self.get_hardware_class_description()} ({self.get_hardware_identifier()} at " \
               f"{self.get_visa_address()})"
