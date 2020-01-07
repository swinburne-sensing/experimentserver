from __future__ import annotations

import abc
import contextlib
import copy
import collections
import functools
import inspect
import sys
import threading
import typing
from collections import OrderedDict

from transitions import EventData

import experimentserver
from experimentserver.data import MeasurementGroup, TYPE_FIELD_DICT
from experimentserver.data.measurement import Measurement, MeasurementSource, MeasurementTarget
from experimentserver.util.logging import LoggerObject
from experimentserver.util.module import import_submodules, HybridMethod
from experimentserver.util.thread import ThreadLock


CALLABLE_PARAMETER = typing.Callable[..., typing.NoReturn]


# Measurement methods may return either a measurement, series of measurements or just a dict
TYPE_MEASUREMENT_LIST = typing.List[Measurement]
TYPE_MEASUREMENT = typing.Union[Measurement, TYPE_MEASUREMENT_LIST, TYPE_FIELD_DICT]

CALLABLE_MEASUREMENT = typing.Callable[[], TYPE_MEASUREMENT]

# Parameter dicts provide a method name and a single argument, set of positional arguments, or a dict of keyword
# arguments
TYPE_PARAMETER_DICT = typing.MutableMapping[str, typing.Any]


class CommandError(experimentserver.ApplicationException):
    """ Base exception for errors caused by setting parameters or taking measurements.

    May be attached to a HardwareCommunicationError or HardwareReportedError.
    """
    pass


class CommunicationError(experimentserver.ApplicationException):
    """ Base exception for IO and other Hardware communication errors.

    This covers errors that are reported server side, and may require reconnection of the Hardware in order to
    recover.
    """
    pass


class HardwareError(experimentserver.ApplicationException):
    """ Base exception for Hardware reported errors or for errors caused by unexpected or invalid instrument response.

    This covers errors reported by the Hardware itself, and may not require reconnection of the Hardware in order to
    recover."""
    pass


class MeasurementError(CommandError):
    """ Exception thrown during measurement capture. """
    pass


class MeasurementUnavailable(MeasurementError):
    pass


class ParameterError(CommandError):
    """ Exception thrown during parameter configuration. """
    pass


class NoEventHandler(experimentserver.ApplicationException):
    """ Exception to indicate that reset events should be handled as setup events. """
    pass


class __OrderedRegistered(object):
    def __init__(self, method: typing.Callable, order: int):
        self.method = method
        self.order = order

    def __lt__(self, other):
        return self.order < other.order

    def __call__(self, *args, **kwargs):
        return self.method(*args, **kwargs)

    def bind(self, parent: Hardware):
        child = copy.deepcopy(self)

        child.method = child.method.__get__(parent, parent.__class__)

        return child


class _RegisteredMeasurement(__OrderedRegistered):
    """ Registered Hardware measurement method. """

    def __init__(self, method: CALLABLE_MEASUREMENT, description: str,
                 measurement_group: typing.Optional[MeasurementGroup], default: bool = False, force: bool = False,
                 order: int = 50, user: bool = True, setup: typing.Optional[str] = None,
                 setup_args: typing.Optional[typing.List] = None, setup_kwargs: typing.Optional[typing.Dict] = None):
        """ Initialise _HardwareMeasurement.

        :param method:
        :param description: user readable description of the measurement(s) produced by this method
        :param measurement_group:
        :param default:
        :param force:
        :param order:
        :param user:
        :param setup:
        :param setup_args:
        :param setup_kwargs:
        """
        super().__init__(method, order)

        self.description = description
        self.measurement_group = measurement_group
        self.default = default
        self.force = force
        self.user = user
        self.setup = setup
        self.setup_args = setup_args or []
        self.setup_kwargs = setup_kwargs or {}

    def call_setup(self, parent: Hardware):
        if self.setup is None:
            return

        method = getattr(parent, self.setup)

        return method(*self.setup_args, **self.setup_kwargs)


class _RegisteredParameter(__OrderedRegistered):
    """  """
    def __init__(self, method: CALLABLE_PARAMETER, description: typing.Dict[str, str], order: int = 50,
                 validation: typing.Optional[typing.Dict[str, typing.Callable[[typing.Any], bool]]] = None):
        """ Initialise _HardwareParameter.

        :param method:
        :param description:
        :param order:
        :param validation:
        """
        super().__init__(method, order)

        self.description = description
        self.validation = validation


class Hardware(LoggerObject, MeasurementSource, metaclass=abc.ABCMeta):
    """ Base class for all Hardware objects controlled in experiments.

    Hardware can represent instrumentation used to perform measurements (like a multimeter), or equipment that controls
    experimental parameters (like temperature or gas flow).

    If implementing new hardware modules then this should be the parent class (or one of its abstract children like
    VISAHardware or SCPIHardware if applicable).
    """

    # Timeout to prevent deadlocks
    _HARDWARE_LOCK_TIMEOUT = 30

    def __init__(self, identifier: str):
        """ Instantiate Hardware object.

        Note this class is abstract and several methods must be implemented in child classes.

        :param identifier: unique identifier
        """
        # Force identifier to be lower case and stripped
        self._hardware_identifier = identifier.strip().lower()

        LoggerObject.__init__(self, logger_name_postfix=f":{self._hardware_identifier}")
        MeasurementSource.__init__(self)

        # Hardware interaction lock
        self._hardware_lock = ThreadLock(f"{self._hardware_identifier}:hardware", self._HARDWARE_LOCK_TIMEOUT)

        # Parameter stack
        self._parameter_lock = threading.RLock()
        self._parameter_buffer: TYPE_PARAMETER_DICT = {}

        # Enabled measurement map
        self._measurement_lock = threading.RLock()
        self._measurement: typing.Optional[str, bool] = None

        # Measurement setup status flag
        self._measurement_setup = False

    # Hardware metadata
    @classmethod
    def get_author(cls) -> str:
        """ Return details of the author of this class using the information provided in the module.

        This may be overwritten if each class has multiple authors.

        :return: string containing the classes author and/or contact details
        """
        # Try and get details from class first
        if hasattr(cls, '__author__') and hasattr(cls, '__email__'):
            return f"{getattr(cls, '__author__')} <{getattr(cls, '__email__')}>"
        elif hasattr(cls, '__author__'):
            return getattr(cls, '__author__')
        elif hasattr(cls, '__email__'):
            return getattr(cls, '__email__')

        # Failing that try and get details from module
        class_module = sys.modules[cls.__module__]

        if hasattr(class_module, '__author__') and hasattr(class_module, '__email__'):
            return f"{getattr(class_module, '__author__')} <{getattr(class_module, '__email__')}>"
        elif hasattr(class_module, '__author__'):
            return getattr(class_module, '__author__')
        elif hasattr(class_module, '__email__'):
            return getattr(class_module, '__email__')

        # Last resort raise an exception
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def get_hardware_description() -> str:
        """ Provide a general description of this type of Hardware.

        :return: string description of this class
        """
        pass

    def get_hardware_identifier(self, capitalise: bool = False) -> str:
        """ Get the unique identifier for this Hardware object.

        :param capitalise: if True then underscores will be stripped and the string capitalise for use in thread names
        :return: str
        """
        if capitalise:
            return ''.join([x.capitalize() for x in self._hardware_identifier.split('_')])
        else:
            return self._hardware_identifier

    def get_hardware_instance_description(self) -> str:
        """ Provide a description of this particular instance of Hardware.

        :return: str
        """
        return f"{self.get_hardware_description()} ({self.get_hardware_identifier()})"

    # Export metadata
    def get_export_source_name(self) -> str:
        return self.get_hardware_identifier()

    # Error checking
    @abc.abstractmethod
    def _hardware_error_check(self):
        pass

    # Instrument lock
    @contextlib.contextmanager
    def get_hardware_lock(self, timeout: typing.Optional[float] = None, quiet: bool = False,
                          error_check: typing.Optional[bool] = None) -> int:
        """ Acquire exclusive reentrant hardware lock. Checks for any errors when released.

        :param timeout:
        :param quiet:
        :param error_check:
        :return: lock depth
        """
        with self._hardware_lock.lock(timeout, quiet) as depth:
            yield depth

            if error_check and depth == 1:
                self._hardware_error_check()

    # Parameter/measurement registration
    def register_measurement(measurement_group: typing.Optional[MeasurementGroup] = None,
                             **kwargs) -> typing.Callable:
        """ Registers a methods as a mechanism for retrieving measurements from instrumentation.

        Can optionally call a setup method when this measurement is first used.

        :param measurement_group:
        :param kwargs: passed to _HardwareMeasurement.__init__
        :return: decorated method
        """
        def wrapper(func):
            return_annotation = inspect.signature(func).return_annotation

            if return_annotation != TYPE_FIELD_DICT and return_annotation != Measurement \
                    and return_annotation != TYPE_MEASUREMENT_LIST:
                raise MeasurementError(f"Registered measurement method {func!r} must provide compatible "
                                       f"return annotation")

            if return_annotation == TYPE_FIELD_DICT and measurement_group is None:
                raise MeasurementError('Registered measurement methods that return a dict must provide a '
                                       'MeasurementGroup')

            return _RegisteredMeasurement(func, measurement_group=measurement_group, **kwargs)

        return wrapper

    def register_parameter(**kwargs) -> typing.Callable:
        """ Registers a method for control of experimental parameters.

        Registered parameters can be configured during experiment runtime.

        :param kwargs: passed to _HardwareParameter.__init__
        :return: decorated method
        """

        def wrapper(func):
            return _RegisteredParameter(func, **kwargs)

        return wrapper

    @HybridMethod
    def get_hardware_measurements(self, include_force: bool = True) \
            -> typing.MutableMapping[str, _RegisteredMeasurement]:
        """ Get all measurements produced by this class.

        This method returns a dict containing all methods that are registered using the Hardware.register_measurement
        decorator.

        This ia a hybrid method, it can be called bound to a class or as a class method.

        :param include_force: if True then forced measurements will be included, otherwise they will be ignored
        :return: dict containing all methods registered as result fetching methods
        """
        measurement_list = {}

        for attrib_name in dir(self):
            # Get a concrete reference from the method name
            attrib = getattr(self, attrib_name)

            # Check for metadata
            if type(attrib) is _RegisteredMeasurement:
                # Filter forced methods
                if attrib.force and not include_force:
                    continue

                measurement_list[attrib_name] = attrib

        # Sort by order
        return OrderedDict(sorted(measurement_list.items(), key=lambda x: x[1]))

    @HybridMethod
    def get_hardware_parameters(self) -> typing.MutableMapping[str, _RegisteredParameter]:
        """ Gets a list of all configurable parameters for this class.

        This returns a dict containing all methods that are registered using the Hardware.register_parameter decorator.

        This ia a hybrid method, it can be called bound to a class or as a class method.

        :return: dict containing all methods registered as configuration parameters
        """
        parameter_list = {}

        for attrib_name in dir(self):
            # Get a concrete reference from the method name
            attrib = getattr(self, attrib_name)

            # Check for metadata
            if type(attrib) is _RegisteredParameter:
                parameter_list[attrib_name] = attrib

        # Sort by order
        return OrderedDict(sorted(parameter_list.items(), key=lambda x: x[1]))

    # Parameter/measurement getter and setter
    @register_parameter(description='Enable a single measurement method', order=0)
    def set_hardware_measurement(self, name: typing.Optional[str] = None):
        """

        :param name:
        :return:
        """
        if name not in self._measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            # Disable all other measurements
            for measure in self._measurement.keys():
                self._measurement[measure] = False

            if name is not None:
                # Enable the selected measurement
                self.enable_hardware_measurement(name)

            # Clear instrument setup flag
            self._measurement_setup = False

    @register_parameter(description='Enable a measurement method', order=0)
    def enable_hardware_measurement(self, name):
        """

        :param name:
        :return:
        """
        if name not in self._measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            self._measurement[name] = True

            # Clear instrument setup flag
            self._measurement_setup = False

    @register_parameter(description='Disable a measurement method', order=0)
    def disable_hardware_measurement(self, name):
        """

        :param name:
        :return:
        """
        if name not in self._measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            self._measurement[name] = False

            # Clear instrument setup flag
            self._measurement_setup = False

    def produce_measurement(self) -> bool:
        """

        :return: True if measurements were produced, False otherwise
        """
        measurement_flag = False

        with self._measurement_lock:
            if self._measurement is None:
                raise MeasurementError('Measurements cannot be performed while Hardware is disconnected')

            # Get enabled measurements
            measurement_meta = self.get_hardware_measurements()

            # Discard disabled methods
            for measurement, measurement_enabled in self._measurement.items():
                if not measurement_enabled and not measurement_meta[measurement].force:
                    measurement_meta.pop(measurement)

            if not self._measurement_setup:
                # Apply measurement setup
                for meta in measurement_meta.values():
                    meta.call_setup(self)

                # Flag setup as complete
                self._measurement_setup = True

            with self.get_hardware_lock():
                for meta in measurement_meta.values():
                    measurement_method = meta.bind(self)

                    measurement_return = measurement_method()

                    if measurement_return is not None:
                        if type(measurement_return) is Measurement:
                            # Single result with metadata
                            MeasurementTarget.record(measurement_return)
                        elif type(measurement_return) is list:
                            # Multiple results with metadata
                            for measurement in measurement_return:
                                MeasurementTarget.record(measurement)
                        elif type(measurement_return) is dict:
                            # Single result
                            MeasurementTarget.record(Measurement(self, meta.measurement_group, measurement_return))
                        else:
                            raise MeasurementError(f"Unexpected return from measurement method: {measurement_return!r}")

                        # Indicate a measurement was produced
                        measurement_flag = True

        return measurement_flag

    def validate_parameters(self, parameter_commands: TYPE_PARAMETER_DICT) -> TYPE_PARAMETER_DICT:
        """

        :param parameter_commands:
        :return:
        :raises: ParameterException on invalid parameters or parameter arguments
        """
        parameter_list = self.get_hardware_parameters()
        parameter_valid: TYPE_PARAMETER_DICT = collections.OrderedDict()

        for parameter_name, parameter_args in sorted(parameter_commands.items(), key=lambda x: x[0]):
            try:
                parameter = parameter_list[parameter_name]
            except KeyError:
                raise ParameterError(f"Parameter {parameter_name} not recognised")

            # Convert arguments to kwargs dict
            if type(parameter_args) is not dict:
                if type(parameter_args) is str:
                    parameter_args = [x.strip() for x in parameter_args.split(',')]

                # If arguments are not iterable then convert to a tuple
                try:
                    _ = iter(parameter_args)
                except TypeError:
                    parameter_args = parameter_args,

                # Use arguments from array as positional arguments
                parameter_arg_names = list(inspect.signature(parameter.method).parameters)

                # Strip self argument of bound method
                parameter_arg_names = parameter_arg_names[1:]

                # Generate mapping
                parameter_args = dict(zip(parameter_arg_names[:len(parameter_args)], parameter_args))

            # Validate arguments if enabled
            if parameter.validation is not None:
                for arg_name, arg_value in parameter_args.items():
                    if arg_name in parameter.validation and not parameter.validation[arg_name](arg_value):
                        raise ParameterError(f"Parameter argument {arg_name} failed validation (value: "
                                                 f"{arg_value!r})")

            parameter_valid[parameter_name] = parameter_args

        return parameter_valid

    def set_parameters(self, parameter_commands: TYPE_PARAMETER_DICT, validated: bool = False) -> typing.NoReturn:
        """

        :param parameter_commands:
        :param validated: if True validation is skipped and the commands are assumed to be valid
        """
        parameter_list = self.get_hardware_parameters()

        # Validate commands
        if not validated:
            parameter_commands = self.validate_parameters(parameter_commands)

        # Sort parameters based on order before calling the appropriate method
        with self._parameter_lock:
            with self.get_hardware_lock():
                for parameter_name, parameter_args in parameter_commands.items():
                    parameter_list[parameter_name](**parameter_args)

                    # Buffer parameter after successful execution
                    self._parameter_buffer[parameter_name] = parameter_args

    # State transition handling
    @abc.abstractmethod
    def handle_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Handle connecting to external Hardware.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during connection
        :raises HardwareReportedError: when Hardware reports fatal error during connection
        """
        # Configure default measurements
        with self._measurement_lock:
            self._measurement = {measure: meta.default for measure, meta in
                                 self.get_hardware_measurements(include_force=False).items()}

            enabled_list = ', '.join([measure for measure, enabled in self._measurement.items() if enabled])

            self._logger.debug(f"Configured default measurements: {enabled_list}")

    @abc.abstractmethod
    def handle_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Handle disconnection from external Hardware.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during disconnection
        :raises HardwareReportedError: when Hardware reports fatal error during disconnection
        """
        # Clear enabled measurements
        with self._measurement_lock:
            self._measurement = None

    @abc.abstractmethod
    def handle_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during configuration
        :raises HardwareReportedError: when Hardware reports fatal error during configuration
        """
        # If parameters were previously provided then configure them now
        pass

    @abc.abstractmethod
    def handle_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during cleanup
        :raises HardwareReportedError: when Hardware reports fatal error during cleanup
        """
        pass

    def handle_start(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during start
        :raises HardwareReportedError: when Hardware reports fatal error during start
        """
        pass

    def handle_stop(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during stop
        :raises HardwareReportedError: when Hardware reports fatal error during stop
        """
        pass

    @abc.abstractmethod
    def handle_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        """
        self._logger.error(f"Hardware {self.get_hardware_identifier()} has entered an error state", event=True,
                           notify=True)

    def handle_reset(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication errors occur during reset
        :raises HardwareReportedError: when Hardware reports fatal error during reset
        :raises NoEventHandler: when no specific reset behaviour is needed
        """
        raise NoEventHandler()

    # Force handling methods to be wrapped by hardware lock acquisition
    def __getattribute__(self, item):
        return super(Hardware, self).__getattribute__(item)

    # object overrides
    def __str__(self) -> str:
        return self.get_hardware_instance_description()

    def __repr__(self) -> str:
        return f"<{self.__class__.__module__}.{self.__class__.__qualname__} ({self.get_hardware_identifier()}) object>"

    # Convert decorators to static methods
    register_measurement = staticmethod(register_measurement)
    register_parameter = staticmethod(register_parameter)


# Import submodules automatically
import_submodules(__name__)
