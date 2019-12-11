import abc
import contextlib
import inspect
import sys
import threading
import typing
from collections import OrderedDict
from datetime import datetime

from transitions import EventData

from experimentserver.data import MeasurementGroup, TYPING_FIELD, TYPING_TAG
from experimentserver.data.export import ExporterSource, record_measurement
from experimentserver.util.logging import LoggerObject
from experimentserver.util.module import get_call_context, import_submodules


# Measurements may return either a dict (default metadata will be used) or a tuple containing metadata
TYPING_MEASUREMENT = typing.Union[TYPING_FIELD, typing.Callable[[], typing.Tuple[datetime, MeasurementGroup,
                                                                                 TYPING_FIELD,
                                                                                 typing.Optional[TYPING_TAG]]]]
TYPING_PARAM = typing.Dict[str, typing.Any]


class HardwareException(Exception):
    pass


class MeasurementException(HardwareException):
    pass


class ParameterException(HardwareException):
    pass


class Hardware(LoggerObject, ExporterSource, metaclass=abc.ABCMeta):
    def __init__(self, identifier: str):
        # Force identifier to be lower case and stripped
        self._hardware_identifier = identifier.strip().lower()

        super().__init__(logger_name_postfix=f":{self._hardware_identifier}")

        # Hardware interaction lock
        self._hardware_lock = threading.RLock()

        # Enabled measurement map
        self._enabled_measurement_setup = False
        self._measurement_lock = threading.RLock()
        self._enabled_measurement = {measure: meta['default'] for measure, meta in self.get_measurement_meta().items()
                                     if not meta['force']}

    # Instrument lock
    @contextlib.contextmanager
    def get_hardware_lock(self):
        """ Aquire transaction lock.

        :return:
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
        """ Provide a description of this type of Hardware in general.

        :return: string description of this class
        """
        pass

    def get_hardware_identifier(self, capitalise: bool = False) -> str:
        if capitalise:
            return ''.join([x.capitalize() for x in self._hardware_identifier.split('_')])
        else:
            return self._hardware_identifier

    def get_export_source_name(self) -> str:
        return self.get_hardware_identifier()

    def get_hardware_instance_description(self) -> str:
        """ Provide a description of this particular instance of Hardware.

        :return: string describing this hardware instance
        """
        return f"{self.get_hardware_description()} ({self.get_hardware_identifier()})"

    # Parameter/measurement registration
    def register_measurement(description: str, measurement: typing.Optional[MeasurementGroup],
                             fields: typing.Dict[str, str], default: bool = False, force: bool = False, order: int = 50,
                             user: bool = True, setup: typing.Optional[str] = None,
                             setup_args: typing.Optional[typing.List] = None,
                             setup_kwargs: typing.Optional[typing.Dict] = None) -> typing.Callable:
        """ Registers a methods as a mechanism for retrieving measurements from instrumentation.

        Can optionally call a setup method when this measurement is first used.

        :param description: user readable description of the measurement(s) produced by this method
        :param measurement:
        :param fields:
        :param default: if True this method is used by default
        :param force: if True this measurement method will always be called when fetching measurements in addition to \
        configured methods
        :param order:
        :param user: if True this measurement may be enabled or disabled by the user
        :param setup: a callable method run when this measurement is first used, useful for changing instrument modes
        :param setup_args: list of arguments passed to setup method
        :param setup_kwargs: dict of keyword arguments passed to setup method
        :return:
        """
        def wrapper(func: typing.Callable[[], TYPING_MEASUREMENT]):
            if inspect.signature(func).return_annotation != TYPING_MEASUREMENT:
                raise MeasurementException(f"Registered measurement method {func!r} must provide compatible return "
                                           f"annotation")

            func.measurement_meta = {
                'method': func,
                'description': description,
                'measurement': measurement,
                'fields': fields,
                'default': default,
                'force': force,
                'order': order,
                'user': user,
                'setup': setup,
                'setup_args': setup_args or [],
                'setup_kwargs': setup_kwargs or {}
            }

            return func

        return wrapper

    def register_parameter(description: typing.Dict[str, str], order: int = 50,
                           validation: typing.Optional[typing.Dict[str, typing.Callable[[typing.Any], bool]]] = None) \
            -> typing.Callable:
        """ Registers a method for control of experimental parameters.

        Registered parameters can be configured during experiment runtime.

        :param description: user readable description of the parameter(s) controlled by this method
        :param order: used to configure order in which parameters are set, lower values are processed earlier
        """

        def wrapper(func):
            func.parameter_meta = {
                'method': func,
                'description': description,
                'order': order,
                'arg_names': inspect.signature(func).parameters,
                'validation': validation
            }

            return func

        return wrapper

    @classmethod
    def get_measurement_meta(cls) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
        """ Get all measurements produced by this class.

        This method returns a dict containing all methods that are registered using the Hardware.register_measurement
        decorator.

        :return: dict containing all methods registered as result fetching methods
        """
        measurement_list = {}

        for method_name in dir(cls):
            # Get a concrete reference from the method name
            method = getattr(cls, method_name)

            # Check for wrapper
            if hasattr(method, 'measurement_meta'):
                measurement_list[method_name] = method.measurement_meta

        # Sort by order
        measurement_list = OrderedDict(sorted(measurement_list.items(), key=lambda x: x[1]['order']))

        return measurement_list

    @classmethod
    def get_parameter_meta(cls) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
        """ Gets a list of all configurable parameters for this class.

        This returns a dict containing all methods that are registered using the Hardware.register_parameter decorator.

        :return: dict containing all methods registered as configuration parameters
        """
        parameter_list = {}

        for method_name in dir(cls):
            # Get a concrete reference from the method name
            method = getattr(cls, method_name)

            # Check for wrapper
            if hasattr(method, 'parameter_meta'):
                parameter_list[method_name] = method.parameter_meta

        # Sort by order
        parameter_list = OrderedDict(sorted(parameter_list.items(), key=lambda x: x[1]['order']))

        return parameter_list

    # Parameter/measurement getter and setter
    @register_parameter('Enable a single measurement method', order=0)
    def set_measurement(self, name: typing.Optional[str] = None):
        if name not in self._enabled_measurement:
            raise MeasurementException(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            # Disable all other measurements
            for measure in self._enabled_measurement.keys():
                self._enabled_measurement[measure] = False

            if name is not None:
                # Enable the selected measurement
                self.enable_measurement(name)

            # Clear instrument setup flag
            self._enabled_measurement_setup = False

    @register_parameter('Enable a measurement method', order=0)
    def enable_measurement(self, name):
        if name not in self._enabled_measurement:
            raise MeasurementException(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            self._enabled_measurement[name] = True

            # Clear instrument setup flag
            self._enabled_measurement_setup = False

    @register_parameter('Disable a measurement method', order=0)
    def disable_measurement(self, name):
        if name not in self._enabled_measurement:
            raise MeasurementException(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            self._enabled_measurement[name] = False

            # Clear instrument setup flag
            self._enabled_measurement_setup = False

    def produce_measurement(self) -> bool:
        measurement_flag = False

        with self._measurement_lock:
            # Get all measurements
            measurement_meta = self.get_measurement_meta()

            for measure, meta in measurement_meta.copy().items():
                # Filter measurements based upon enabled or forced status
                if not meta['force'] and self._enabled_measurement[measure]:
                    measurement_meta.pop(measure)

            if not self._enabled_measurement_setup:
                # Apply measurement setup
                for meta in measurement_meta.values():
                    if meta['setup'] is not None:
                        # Get setup method
                        setup_method = getattr(self, meta['setup'])

                        # Call with provided arguments
                        setup_method(*meta['setup_args'], **meta['setup_kwargs'])

                # Flag setup as complete
                self._enabled_measurement_setup = True

            with self.get_hardware_lock():
                for meta in measurement_meta.values():
                    measurement_method = meta['method']

                    # Take measurement
                    fields = measurement_method(self)

                    # Record measurement
                    record_measurement(datetime.now(), self, meta['measurement'], fields)

                    # Indicate a measurement was produced
                    measurement_flag = True

        return measurement_flag

    def set_parameter(self, commands: TYPING_PARAM, validate_only: bool = False) -> typing.NoReturn:
        # Validate all parameters
        parameter_meta = self.get_parameter_meta()

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
                set_args_names = list(parameter_meta['arg_names'].keys())

                # Strip self argument
                set_args_names = set_args_names[1:]

                # Generate mapping
                parameter_args = dict(zip(set_args_names[:len(parameter_args)], parameter_args))

            parameter_args_valid = parameter_meta['arg_valid']

            for arg_name, arg_value in parameter_args.items():
                if arg_name in parameter_args_valid and arg_value not in parameter_args_valid[arg_name]:
                    raise ParameterException(
                        self, f"Parameter {parameter_name} argument {arg_name} has a invalid value {arg_value} "
                        f"(valid values: {parameter_args_valid})")

            commands[parameter_name] = parameter_args

        # Sort parameters based on order before calling the appropriate method
        with self.get_hardware_lock():
            for parameter_name, parameter_args in sorted(commands.items(),
                                                         key=lambda x: parameter_meta[x[0]]['order']):
                parameter_method = getattr(self, parameter_name)

                if not validate_only:
                    parameter_method(**parameter_args)

    # State transitions
    @abc.abstractmethod
    def handle_setup(self, event: typing.Optional[EventData]):
        """

        :return:
        """
        self._logger.debug('Setup')

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
