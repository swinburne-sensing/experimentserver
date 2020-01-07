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

from . import Hardware, CommunicationError, HardwareError
from experimentserver.util.thread import ThreadLock


# Disable pyvisa logging (avoids duplication)
pyvisa.logger.disabled = True


# Exceptions
class VISACommunicationError(CommunicationError):
    """ Wrapper for VISA I/O errors that occur during transactions. """
    pass


class VISAHardwareError(HardwareError):
    """ Wrapper for VISA I/O errors that occur during transactions. """
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
        if allow_direct:
            try:
                return cls[x]
            except KeyError:
                pass

        if type(x) is str:
            x = x.strip()

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


# Classes
class VISAHardware(Hardware, metaclass=abc.ABCMeta):
    """ Base class for instruments relying upon the VISA communication layer. """
    # VISA transaction lock timeout
    _VISA_TRANSACTION_LOCK_TIMEOUT = 30

    # VISA resource manager
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

        # Transaction lock
        self._visa_transaction_lock = ThreadLock(f"{self._hardware_identifier}:visa",
                                                 self._VISA_TRANSACTION_LOCK_TIMEOUT)

        # Transaction buffer for debugging
        self._visa_transaction_stack: typing.List[typing.List[str]] = []
        self._visa_error_check_flag = False

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
        """ Get the VISA resource manager. Typically only used by objects to connect to hardware.

        :return: ResourceManager
        """
        # Lock required otherwise memory access violations occur (although it's supposed to be thread safe)
        with cls.__resource_manager_lock:
            if cls.__resource_manager is None:
                cls.__resource_manager = pyvisa.ResourceManager()

                cls._get_class_logger().info(f"VISA library: {cls.__resource_manager.visalib}")

        return cls.__resource_manager

    @abc.abstractmethod
    def _visa_transaction_error_check(self) -> typing.NoReturn:
        """ Check for errors after VISA transaction.

        VISA transaction lock is held prior to calling this method.

        :raises VISACommunicationError: when VISA communication error occurs during check
        :raises VISAReportedError: when VISA transaction has resulted in an error state
        """
        pass

    @contextlib.contextmanager
    def get_visa_transaction_lock(self, error_check: typing.Optional[bool] = None, error_ignore: bool = False,
                                  timeout: typing.Optional[float] = None) -> int:
        """ Get an exclusive lock on VISA resource to perform transactions.

        Returns the depth of lock acquisition, eg. first acquisition returns 1, a further re-enterent lock acquisition
        returns 2, etc.

        :param error_check: if True an error check is performed when lock is released, if False error check is \
        explicitly not performed, default is perform check on exit from the first lock acquisition
        :param error_ignore:
        :param timeout:
        :return: int
        :raises VISACommunicationError: when not connected to VISA resource
        :raises HardwareError: when hardware reports error in response to processed commands
        """
        # Throw exception if instrument is not connected
        if self._visa_resource is None:
            raise VISACommunicationError('Not connected to VISA resource')

        with self._visa_transaction_lock.lock(timeout) as depth:
            try:
                # Push transaction buffer stack
                if not self._visa_error_check_flag:
                    self._visa_transaction_stack.append([])
                    self._logger.debug(f"Pushed transaction stack (depth: {len(self._visa_transaction_stack)})")

                # Perform transactions
                yield depth

                # Perform error check before returning if requested or not requested but releasing the last lock
                if not self._visa_error_check_flag and len(self._visa_transaction_stack[-1]) > 0 and \
                        ((error_check is not None and error_check) or (error_check is None and depth == 1)):
                    self._logger.debug('Performing VISA error check')

                    self._visa_error_check_flag = True
                    self._visa_transaction_error_check()
                    self._visa_error_check_flag = False

                    self._logger.debug('No errors')
            except VISAHardwareError as exc:
                transaction_buffer = ', '.join(self._visa_transaction_stack[-1])

                if error_ignore:
                    self._logger.warning(f"VISA hardware reported error {exc} after transaction: {transaction_buffer}",
                                         event=False)
                else:
                    raise VISAHardwareError(f"VISA hardware reported error after transaction: {transaction_buffer}") \
                        from exc
            finally:
                # Pop transaction buffer stack
                if not self._visa_error_check_flag:
                    self._visa_transaction_stack.pop(0)
                    self._logger.debug(f"Popped transaction stack (depth: {len(self._visa_transaction_stack)})")

                # Clear error check flag
                self._visa_error_check_flag = False

    @contextlib.contextmanager
    def get_hardware_lock(self, timeout: typing.Optional[float] = None, quiet: bool = False) -> int:
        with self._hardware_lock.lock(timeout, quiet):
            yield self.get_visa_transaction_lock(timeout=timeout)

    def _comm_stack_wrapper(func) -> typing.Callable:
        """ Decorator to wrap pyVISA exceptions in VISAException and provide rate limiting.

        :param func: method to decorate
        :return: decorated method
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if 'timeout' in kwargs:
                timeout = kwargs.pop('timeout')
            else:
                timeout = None

            if 'error_check' in kwargs:
                error_check = kwargs.pop('error_check')
            else:
                error_check = None

            if 'error_ignore' in kwargs:
                error_ignore = kwargs.pop('error_ignore')
            else:
                error_ignore = None

            # Apply message rate limiting if enabled
            with self.get_visa_transaction_lock(error_check, error_ignore, timeout):
                if self._visa_rate_limit is not None and self._visa_comm_timestamp is not None:
                    # Delay for minimum write interval
                    delay = self._visa_rate_limit - (self._visa_comm_timestamp - time.time())

                    if delay > 0:
                        # self._logger.debug(f"Rate limited, waiting {delay} s")
                        time.sleep(delay)
                try:
                    result = func(self, *args, **kwargs)

                    # Save timestamp for current operation
                    self._visa_comm_timestamp = time.time()

                    return result
                except pyvisa.errors.Error as exc:
                    raise VISACommunicationError('VISA library error occurred during communication') from exc

        return wrapper

    @staticmethod
    def _format_arg(x) -> str:
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
        format_args = list(map(cls._format_arg, format_args))
        format_kwargs = {k: cls._format_arg(v) for k, v in format_kwargs.items()}

        return x.format(*format_args, **format_kwargs)

    @_comm_stack_wrapper
    def visa_write(self, command: str, *format_args, visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
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
        command = self._format_command(command, format_args, format_kwargs)
        visa_kwargs = visa_kwargs or {}

        self._visa_transaction_stack[-1].append(command)

        if len(visa_kwargs) == 0:
            self._logger.debug(f"VISA write {command!r}")
        else:
            self._logger.debug(f"VISA write {command!r} (args: {visa_kwargs!s})")

        return self._visa_resource.write(command, **visa_kwargs)

    @_comm_stack_wrapper
    def visa_write_binary(self, command: str, payload: typing.Iterable, *format_args,
                          visa_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None, **format_kwargs) -> int:
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
        command = self._format_command(command, format_args, format_kwargs)
        visa_kwargs = visa_kwargs or {}

        self._visa_transaction_stack[-1].append(command)

        if len(visa_kwargs) == 0:
            self._logger.debug(f"VISA write binary {command!r} (payload: {payload!r})")
        else:
            self._logger.debug(f"VISA write binary {command!r} (payload: {payload!r}) (args: {visa_kwargs!s})")

        return self._visa_resource.write_binary_values(command, payload, **visa_kwargs)

    @_comm_stack_wrapper
    def visa_query(self, command: typing.Optional[str], *format_args, binary: bool = False,
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
                response = self._visa_resource.read(**visa_kwargs).strip()
            else:
                response = self._visa_resource.read_binary_values(**visa_kwargs)
        else:
            command = self._format_command(command, format_args, format_kwargs)

            self._visa_transaction_stack[-1].append(command)

            if len(visa_kwargs) == 0:
                self._logger.debug(f"VISA query: {command!r}")
            else:
                self._logger.debug(f"VISA query: {command!r} (args: {visa_kwargs!r})")

            if not binary:
                response = self._visa_resource.query(command, **visa_kwargs).strip()
            else:
                response = self._visa_resource.query_binary_values(command, **visa_kwargs)

        self._logger.debug(f"VISA response {response!r}")

        return response

    def _visa_disconnect(self):
        if self._visa_resource is None:
            self._logger.warning('VISA resource should be connected before requesting disconnect')
            return

        # Close resource
        self._visa_resource.close()

        # Free resource
        self._visa_resource = None

        self._logger.info(f"Released VISA resource {self._visa_address}")

    # State transition handling
    def handle_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(VISAHardware, self).handle_connect(event)

        if self._visa_resource is not None:
            self._logger.warning('VISA resource not properly disconnected')

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
                raise VISACommunicationError(
                    f"Unexpected VISA error occurred during acquisition of {self._visa_address}") \
                    from exc

        if not issubclass(type(resource), messagebased.MessageBasedResource):
            raise VISACommunicationError(self, 'Only message based VISA resources are supported')

        # Set optional timeout
        if self._visa_timeout is not None:
            resource.timeout = self._visa_timeout * 1000

        self._visa_resource = resource

        self._logger.info(f"Acquired VISA resource {self._visa_address}")

    def handle_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._visa_disconnect()

        super(VISAHardware, self).handle_disconnect(event)

    def handle_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(VISAHardware, self).handle_configure(event)

        assert self._visa_resource is not None

    def handle_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        assert self._visa_resource is not None

        super(VISAHardware, self).handle_cleanup(event)

    def handle_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._visa_disconnect()

        super(VISAHardware, self).handle_error(event)

    # Hardware overrides
    def get_hardware_instance_description(self) -> str:
        # Append VISA resource address to the class description
        return f"{self.get_hardware_description()} ({self.get_hardware_identifier(), self.get_visa_address()})"

    # Convert decorators to static methods
    _comm_stack_wrapper = staticmethod(_comm_stack_wrapper)
