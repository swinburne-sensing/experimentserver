from __future__ import annotations

import abc
import contextlib
import collections
import functools
import inspect
import sys
import threading
import typing
from collections import OrderedDict

from transitions import EventData

import experimentserver.util.metadata as metadata
from .error import MeasurementError, MeasurementUnavailable, ParameterError, NoEventHandler
from .state.transition import HardwareTransition
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


class _MeasurementMetadata(metadata.OrderedMetadata):
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

        return_annotation = inspect.signature(method).return_annotation

        if return_annotation != TYPE_FIELD_DICT and return_annotation != Measurement \
                and return_annotation != TYPE_MEASUREMENT_LIST:
            raise MeasurementError(f"Registered measurement method {method!r} must provide compatible "
                                   f"return annotation")

        if return_annotation == TYPE_FIELD_DICT and measurement_group is None:
            raise MeasurementError('Registered measurement methods that return a dict must provide a '
                                   'MeasurementGroup')

        self.description = description
        self.measurement_group = measurement_group
        self.default = default
        self.force = force
        self.user = user
        self.setup = setup
        self.setup_args = setup_args or []
        self.setup_kwargs = setup_kwargs or {}


class _ParameterMetadata(metadata.OrderedMetadata):
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

    # Instrument lock
    @contextlib.contextmanager
    def hardware_lock(self, timeout: typing.Optional[float] = None, quiet: bool = False) -> int:
        """ Acquire exclusive reentrant hardware lock. Checks for any errors when released.

        :param timeout:
        :param quiet:
        :return: lock depth
        """
        with self._hardware_lock.lock(timeout, quiet) as depth:
            yield depth

    def hardware_lock_wrapper(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            with self.hardware_lock():
                return func(self, *args, **kwargs)

        return wrapper

    # Parameter/measurement registration
    register_measurement = functools.partial(metadata.register_metadata, _MeasurementMetadata)
    register_parameter = functools.partial(metadata.register_metadata, _ParameterMetadata)

    @HybridMethod
    def get_hardware_measurements(self, include_force: bool = True) -> typing.MutableMapping[str, _MeasurementMetadata]:
        """ Get all measurements produced by this class.

        This method returns a dict containing all methods that are registered using the Hardware.register_measurement
        decorator.

        This ia a hybrid method, it can be called bound to a class or as a class method.

        :param include_force: if True then forced measurements will be included, otherwise they will be ignored
        :return: dict containing all methods registered as result fetching methods
        """
        measurement_metadata = metadata.get_metadata(self, _MeasurementMetadata)

        if not include_force:
            measurement_metadata = {k: v for k, v in measurement_metadata.items() if not v.force}

        return measurement_metadata

    @HybridMethod
    def get_hardware_parameters(self) -> typing.MutableMapping[str, _ParameterMetadata]:
        """ Gets a list of all configurable parameters for this class.

        This returns a dict containing all methods that are registered using the Hardware.register_parameter decorator.

        This ia a hybrid method, it can be called bound to a class or as a class method.

        :return: dict containing all methods registered as configuration parameters
        """
        return metadata.get_metadata(self, _ParameterMetadata)

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

            with self.hardware_lock():
                for meta in measurement_meta.values():
                    # Bind method
                    measurement_method = meta.bind(self)

                    try:
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
                                raise MeasurementError(
                                    f"Unexpected return from measurement method: {measurement_return!r}")

                            # Indicate a measurement was produced
                            measurement_flag = True
                    except MeasurementUnavailable as exc:
                        # Ignore unavailable measurements
                        pass

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
            with self.hardware_lock():
                for parameter_name, parameter_kwargs in parameter_commands.items():
                    parameter_method = parameter_list[parameter_name].bind(self)

                    parameter_method(**parameter_kwargs)

                    # Buffer parameter after successful execution
                    self._parameter_buffer[parameter_name] = parameter_kwargs

    # State transition handling
    @abc.abstractmethod
    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Handle connecting to external Hardware.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during connection
        :raises HardwareReportedError: when Hardware reports fatal error during connection
        """
        # Configure default measurements
        with self._measurement_lock:
            self._measurement = {measure: meta.default for measure, meta in self.get_hardware_measurements().items()}

            enabled_list = ', '.join([measure for measure, enabled in self._measurement.items() if enabled])

            self._logger.debug(f"Configured default measurements: {enabled_list}")

    @abc.abstractmethod
    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Handle disconnection from external Hardware.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during disconnection
        :raises HardwareReportedError: when Hardware reports fatal error during disconnection
        """
        # Clear enabled measurements
        with self._measurement_lock:
            self._measurement = None

        # Clear buffered parameters
        with self._parameter_lock:
            self._parameter_buffer.clear()

    @abc.abstractmethod
    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during configuration
        :raises HardwareReportedError: when Hardware reports fatal error during configuration
        """
        # If parameters were previously provided then configure them now
        with self._parameter_lock:
            self.set_parameters(self._parameter_buffer, validated=True)

    @abc.abstractmethod
    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during cleanup
        :raises HardwareReportedError: when Hardware reports fatal error during cleanup
        """
        pass

    def transition_start(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during start
        :raises HardwareReportedError: when Hardware reports fatal error during start
        """
        pass

    def transition_stop(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during stop
        :raises HardwareReportedError: when Hardware reports fatal error during stop
        """
        pass

    @abc.abstractmethod
    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        """
        self._logger.error(f"Hardware {self.get_hardware_identifier()} has entered an error state", event=True,
                           notify=True)

    def transition_reset(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication errors occur during reset
        :raises HardwareReportedError: when Hardware reports fatal error during reset
        :raises NoEventHandler: when no specific reset behaviour is needed
        """
        raise NoEventHandler()

    # object overrides
    def __str__(self) -> str:
        return self.get_hardware_instance_description()

    def __repr__(self) -> str:
        return f"<{self.__class__.__module__}.{self.__class__.__qualname__} ({self.get_hardware_identifier()}) object>"

    # Convert decorators to static methods
    hardware_lock_wrapper = staticmethod(hardware_lock_wrapper)
    register_measurement = staticmethod(register_measurement)
    register_parameter = staticmethod(register_parameter)


# Import submodules automatically
import_submodules(__name__)
