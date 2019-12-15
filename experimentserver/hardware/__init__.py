from __future__ import annotations

import abc
import contextlib
import copy
import functools
import inspect
import sys
import threading
import typing
from collections import OrderedDict
from datetime import datetime

from transitions import EventData

from experimentserver.data import MeasurementGroup, TYPE_FIELD_DICT, TYPE_TAG_DICT
from experimentserver.data.export import ExporterSource, record_measurement
from experimentserver.util.logging import LoggerObject
from experimentserver.util.module import get_call_context, import_submodules


CALLABLE_PARAMETER = typing.Callable[..., typing.NoReturn]


# Measurements may return either a dict (default metadata will be used) or a tuple containing metadata
TYPE_MEASUREMENT_TUPLE = typing.Tuple[datetime, MeasurementGroup, TYPE_FIELD_DICT, typing.Optional[TYPE_TAG_DICT]]
TYPE_MEASUREMENT_TUPLE_LIST = typing.List[TYPE_MEASUREMENT_TUPLE]

TYPE_MEASUREMENT = typing.Union[TYPE_FIELD_DICT, TYPE_MEASUREMENT_TUPLE, TYPE_MEASUREMENT_TUPLE_LIST]

CALLABLE_MEASUREMENT = typing.Union[typing.Callable[[], TYPE_FIELD_DICT], typing.Callable[[], TYPE_MEASUREMENT_TUPLE],
                                    typing.Callable[[], TYPE_MEASUREMENT_TUPLE_LIST]]

# Parameter dicts provide a method name and a single argument, set of positional arguments, or a dict of keyword
# arguments
TYPE_PARAMETER_DICT = typing.Dict[str, typing.Union[typing.Any]]


class HardwareException(Exception):
    """ General purpose exception thrown by Hardware objects when errors are encountered. """
    pass


class MeasurementException(HardwareException):
    """ Exception thrown during measurement. """
    pass


class MeasurementNotReady(MeasurementException):
    pass


class ParameterException(HardwareException):
    """ Exception thrown during parameter configuration. """
    pass


class __OrderedCallable(object):
    def __init__(self, method: typing.Callable, order: int):
        self.parent = None
        self.method = method
        self.order = order

    def __call__(self, *args, **kwargs):
        if self.parent is None:
            method = self.method
        else:
            method = self.method.__get__(self.parent)

        return method(*args, **kwargs)

    def __get__(self, obj, _):
        child = copy.copy(self)
        child.parent = obj

        return child

    def __lt__(self, other):
        return self.order < other.order


class _HardwareMeasurement(__OrderedCallable):
    """ Registered Hardware measurement method. """

    def __init__(self, method: CALLABLE_MEASUREMENT, description: str,
                 measurement_group: typing.Optional[MeasurementGroup], default: bool = False, force: bool = False,
                 order: int = 50, user: bool = True, setup: typing.Optional[str] = None,
                 setup_args: typing.Optional[typing.List] = None, setup_kwargs: typing.Optional[typing.Dict] = None):
        """ Initialise _HardwareMeasurement.

        :param method:
        :param description: user readable description of the measurement(s) produced by this method
        :param measurement_group:
        :param fields:
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

        # Resolve string to bound method
        if type(self.setup) is str:
            self.setup = getattr(parent, self.setup)

        self.setup(*self.setup_args, **self.setup_kwargs)


class _HardwareParameter(__OrderedCallable):
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

        self.method = method
        self.description = description
        self.validation = validation


class Hardware(LoggerObject, ExporterSource, metaclass=abc.ABCMeta):
    """ Base class for all Hardware objects controlled in experiments.

    Hardware can represent instrumentation used to perform measurements (like a multimeter), or equipment that controls
    experimental parameters (like temperature or gas flow).

    If implementing new hardware modules then this should be the parent class (or one of its abstract children like
    VISAHardware or SCPIHardware if applicable).
    """

    def __init__(self, identifier: str):
        """ Instantiate Hardware object.

        Note this class is abstract and several methods must be implemented in child classes.

        :param identifier: unique identifier
        """
        # Force identifier to be lower case and stripped
        self._hardware_identifier = identifier.strip().lower()

        LoggerObject.__init__(self, logger_name_postfix=f":{self._hardware_identifier}")
        ExporterSource.__init__(self)

        # Hardware interaction lock
        self._hardware_lock = threading.RLock()

        # Parameter stack
        self._parameter_stack: typing.List[TYPE_PARAMETER_DICT] = []
        self._parameter_lock = threading.RLock()

        # Enabled measurement map
        self._measurement: typing.Optional[str, bool] = None
        self._measurement_lock = threading.RLock()

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

    def get_export_source_name(self) -> str:
        return self.get_hardware_identifier()

    # Instrument lock
    @contextlib.contextmanager
    def get_hardware_lock(self):
        """ Acquire exclusive reentrant hardware lock.

        :return: lock
        """
        context = ', '.join(get_call_context(2))

        try:
            self._logger.debug(f"Lock wait (context: {context})")
            self._hardware_lock.acquire()
            self._logger.debug(f"Lock acquired (context: {context})")
            yield
        finally:
            self._hardware_lock.release()
            self._logger.debug(f"Lock released (context: {context})")

    # Parameter/measurement registration
    def register_measurement(measurement_group: typing.Optional[MeasurementGroup] = None,
                             **kwargs) -> typing.Callable:
        """ Registers a methods as a mechanism for retrieving measurements from instrumentation.

        Can optionally call a setup method when this measurement is first used.

        :param args: passed to _HardwareMeasurement.__init__
        :param measurement_group:
        :param kwargs: passed to _HardwareMeasurement.__init__
        :return: decorated method
        """
        def wrapper(func: CALLABLE_MEASUREMENT):
            return_annotation = inspect.signature(func).return_annotation

            if return_annotation != TYPE_FIELD_DICT and return_annotation != TYPE_MEASUREMENT_TUPLE \
                    and return_annotation != TYPE_MEASUREMENT_TUPLE_LIST:
                raise MeasurementException(f"Registered measurement method {func!r} must provide compatible return "
                                           f"annotation")

            if return_annotation != TYPE_FIELD_DICT and measurement_group is None:
                raise MeasurementException('Registered measurement methods that return a dict must provice a '
                                           'MeasurementGroup')

            return _HardwareMeasurement(func, measurement_group=measurement_group, **kwargs)

        return wrapper

    def register_parameter(**kwargs) -> typing.Callable:
        """ Registers a method for control of experimental parameters.

        Registered parameters can be configured during experiment runtime.

        :param args: passed to _HardwareParameter.__init__
        :param kwargs: passed to _HardwareParameter.__init__
        :return: decorated method
        """

        def wrapper(func):
            return _HardwareParameter(func, **kwargs)

        return wrapper

    def get_hardware_measurements(self, include_force: bool = True) -> typing.Dict[str, _HardwareMeasurement]:
        """ Get all measurements produced by this class.

        This method returns a dict containing all methods that are registered using the Hardware.register_measurement
        decorator.

        :param include_force:
        :param name_filter:
        :return: dict containing all methods registered as result fetching methods
        """
        measurement_list = {}

        for method_name in dir(self):
            # Get a concrete reference from the method name
            method = getattr(self, method_name)

            # Check for metadata
            if type(method) is _HardwareMeasurement:
                # Filter forced methods
                if method.force and not include_force:
                    continue

                measurement_list[method_name] = method

        # Sort by order
        return OrderedDict(sorted(measurement_list.items(), key=lambda x: x[1]))

    def get_hardware_parameters(self) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
        """ Gets a list of all configurable parameters for this class.

        This returns a dict containing all methods that are registered using the Hardware.register_parameter decorator.

        :return: dict containing all methods registered as configuration parameters
        """
        parameter_list = {}

        for method_name in dir(self):
            # Get a concrete reference from the method name
            method = getattr(self, method_name)

            # Check for metadata
            if type(method) is _HardwareParameter:
                parameter_list[method_name] = method

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
            raise MeasurementException(f"Unrecognised measurement name {name}")

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
            raise MeasurementException(f"Unrecognised measurement name {name}")

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
            raise MeasurementException(f"Unrecognised measurement name {name}")

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
                raise MeasurementException('Measurements cannot be performed while Hardware is disconnected')

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
                    sample_datetime = datetime.now()

                    try:
                        measurement_return = meta()

                        if measurement_return is not None:
                            if type(measurement_return) is dict:
                                # Single result
                                record_measurement(sample_datetime, self, meta.measurement_group, measurement_return)
                            elif type(measurement_return) is tuple:
                                # Single result with metadata
                                (sample_datetime, measurement_group, fields, tags) = measurement_return

                                record_measurement(sample_datetime, self, measurement_group, fields, tags)
                            elif type(measurement_return) is list:
                                # Multiple results with metadata
                                for (sample_datetime, measurement_group, fields, tags) in measurement_return:
                                    record_measurement(sample_datetime, self, measurement_group, fields, tags)

                            # Indicate a measurement was produced
                            measurement_flag = True
                    except MeasurementNotReady as exc:
                        self._logger.debug(f"Measurement {meta.description} not ready, message: {exc}")
                        continue

        return measurement_flag

    def set_parameter(self, commands: TYPE_PARAMETER_DICT) -> typing.NoReturn:
        # Validate all parameters
        parameter_meta = self.get_hardware_parameters()

        processed_commands = {}

        for parameter_name, parameter_args in commands.items():
            if parameter_name not in parameter_meta:
                raise ParameterException(self, f"Parameter {parameter_name} not recognised")

            parameter_meta = parameter_meta[parameter_name]

            # Check arguments against supplied values
            if type(parameter_args) is not dict:
                if type(parameter_args) is str:
                    parameter_args = [x.strip() for x in parameter_args.split(',')]

                # If arguments are not iterable then convert to a tuple
                try:
                    _ = iter(parameter_args)
                except TypeError:
                    parameter_args = parameter_args,

                # Use arguments from array as positional arguments
                set_args_names = list(inspect.signature(parameter_meta.method).parameters)

                # Strip self argument
                set_args_names = set_args_names[1:]

                # Generate mapping
                parameter_args = dict(zip(set_args_names[:len(parameter_args)], parameter_args))

            processed_commands[parameter_meta] = parameter_args

        # Sort parameters based on order before calling the appropriate method
        with self.get_hardware_lock():
            for parameter_meta, parameter_args in sorted(processed_commands.items(),
                                                         key=lambda x: x[0]):
                parameter_meta(**parameter_args)

    # State transitions
    @abc.abstractmethod
    def handle_setup(self, event: typing.Optional[EventData]):
        """

        :return:
        """
        self._logger.debug('Setup')

        # Configure default measurements
        with self._measurement_lock:
            self._measurement = {measure: meta.default for measure, meta in
                                 self.get_hardware_measurements(include_force=False).items()}

    @abc.abstractmethod
    def handle_start(self, event: EventData):
        """

        :return:
        """
        self._logger.debug('Start')

    @abc.abstractmethod
    def handle_pause(self, event: EventData):
        """

        :return:
        """
        self._logger.debug('Pause')

    @abc.abstractmethod
    def handle_resume(self, event: EventData):
        """

        :return:
        """
        self._logger.debug('Resume')

    @abc.abstractmethod
    def handle_stop(self, event: EventData):
        """

        :return:
        """
        self._logger.debug('Stop')

    @abc.abstractmethod
    def handle_cleanup(self, event: EventData):
        """

        :return:
        """
        self._logger.debug('Cleanup')

        # Clear enabled measurements
        with self._measurement_lock:
            self._measurement = None

    @abc.abstractmethod
    def handle_error(self, event: EventData):
        """

        :return:
        """
        self._logger.error(f"Hardware {self.get_hardware_identifier()} has entered an error state", event=True,
                           notify=True)

    def handle_reset(self, event: EventData):
        """

        :return:
        """
        raise NotImplementedError()

    # object overrides
    def __str__(self) -> str:
        return self.get_hardware_instance_description()

    def __repr__(self) -> str:
        return f"<{self.__class__.__module__}.{self.__class__.__qualname__} ({self.get_hardware_identifier()}) object>"

    # Make decorators static
    register_measurement = staticmethod(register_measurement)
    register_parameter = staticmethod(register_parameter)


# Import submodules automatically
import_submodules(__name__)
