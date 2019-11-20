import abc
import inspect
import queue
import sys
import typing
from datetime import datetime

from transitions import Machine, State

from experimentserver.data import RecordType, TYPING_TAGS, TYPING_RECORD
from experimentserver.data.export import Exporter
from experimentserver.util.logging import LoggerObject
from experimentserver.util.module import import_submodules


_HARDWARE_STATE = {state.name: state for state in [
    State(name='idle'),
    State(name='ready', on_enter='_event_setup'),
    State(name='running', on_enter='_event_start', on_exit='_event_stop'),
    State(name='paused', on_enter='_handle_pause', on_exit='_handle_resume'),
    State(name='error', on_enter='_handle_error')
]}

_HARDWARE_TRANSITIONS = [
    {'trigger': 'setup', 'source': 'idle', 'dest': 'ready'},
    {'trigger': 'start', 'source': 'ready', 'dest': 'running'},
    {'trigger': 'pause', 'source': 'running', 'dest': 'paused'},
    {'trigger': 'resume', 'source': 'paused', 'dest': 'running'},
    {'trigger': 'stop', 'source': ['running', 'paused'], 'dest': 'ready'},
    {'trigger': 'error', 'source': '*', 'dest': 'error'}
]


class HardwareException(Exception):
    pass


class MeasurementException(HardwareException):
    pass


class ParameterException(HardwareException):
    pass


class Hardware(LoggerObject, metaclass=abc.ABCMeta):
    def __init__(self, identifier: str):
        # Force identifier to be lower case and stripped
        self._hardware_identifier = identifier.strip().lower()

        super().__init__(logger_append=f":{self._hardware_identifier}")

    # Hardware metadata
    @classmethod
    def get_author(cls) -> str:
        """ Return details of the author of this class using the information provided in the module.

        This may be overwritten if each class has multiple authors.

        :return: string containing the classes author and/or contact details
        """
        # Try and get details from class first
        if hasattr(cls, '__author__') and hasattr(cls, '__email__'):
            return "{} <{}>".format(getattr(cls, '__author__'), getattr(cls, '__email__'))
        elif hasattr(cls, '__author__'):
            return getattr(cls, '__author__')
        elif hasattr(cls, '__email__'):
            return getattr(cls, '__email__')

        # Failing that try and get details from module
        class_module = sys.modules[cls.__module__]

        if hasattr(class_module, '__author__') and hasattr(class_module, '__email__'):
            return "{} <{}>".format(getattr(class_module, '__author__'), getattr(class_module, '__email__'))
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

    def get_hardware_instance_description(self) -> str:
        """ Provide a description of this particular instance of Hardware.

        :return: string describing this hardware instance
        """
        return f"{self.get_hardware_description()} ({self.get_hardware_identifier()})"

    # Parameter/measurement registration
    def register_measurement(description: str, force: bool = False, default: bool = False,
                             setup: typing.Optional[typing.Callable] = None,
                             setup_args: typing.Optional[typing.List] = None,
                             setup_kwargs: typing.Optional[typing.Dict] = None) -> typing.Callable:
        """ Registers a methods as a mechanism for retrieving measurements from instrumentation.

        Can optionally call a setup method when this measurement is first used.

        :param description: user readable description of the measurement(s) produced by this method
        :param force: if True this measurement method will always be called when fetching measurements in addition to \
        configured methods
        :param default: if True this method is used by default
        :param setup: a callable method run when this measurement is first used, useful for changing instrument modes
        :param setup_args: list of arguments passed to setup method
        :param setup_kwargs: dict of keyword arguments passed to setup method
        :return:
        """

        def wrapper(func):
            func.measurement_meta = {
                'description': description,
                'force': force,
                'default': default,
                'setup': setup,
                'setup_args': setup_args or [],
                'setup_kwargs': setup_kwargs or {}
            }

            return func

        return wrapper

    def register_parameter(description: typing.Dict[str, str], required: bool = False, order: int = 50,
                           arg_valid: typing.Optional[typing.Dict[str, typing.List]] = None) -> typing.Callable:
        """ Registers a method for control of experimental parameters.

        Registered parameters can be configured during experiment runtime.

        :param description: user readable description of the parameter(s) controlled by this method
        :param required: if True this parameter must be given a value on startup, otherwise it is optional
        :param order: used to configure order in which parameters are set, lower values are processed earlier
        :param arg_valid: dict of arguments and valid values for those arguments (where arbitrary input should not be \
        used)
        """

        def wrapper(func):
            func.parameter_meta = {
                'description': description,
                'required': required,
                'order': order,
                'arg_names': inspect.signature(func).parameters,
                'arg_valid': arg_valid or {}
            }

            return func

        return wrapper

    @classmethod
    def get_measurement_meta(cls) -> dict:
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

        return measurement_list

    @classmethod
    def get_parameter_meta(cls) -> dict:
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

        return parameter_list

    # Parameter/measurement getter and setter
    def set_parameter(self, parameter: typing.Dict[str, typing.Any], validate_only: bool = False) -> typing.NoReturn:
        # Validate all parameters
        parameter_meta_dict = self.get_parameter_meta()

        for parameter_name, parameter_args in parameter.items():
            if parameter_name not in parameter_meta_dict:
                raise ParameterException(self, "{} is not a known parameter".format(parameter_name))

            parameter_meta = parameter_meta_dict[parameter_name]

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
                        self, "Parameter {} argument {} has a invalid value {} (valid values: {})".format(
                            parameter_name, arg_name, arg_value, parameter_args_valid))

            parameter[parameter_name] = parameter_args

        # Sort parameters based on order before calling the appropriate method
        for parameter_name, parameter_args in sorted(parameter.items(),
                                                     key=lambda x: parameter_meta_dict[x[0]]['order']):
            parameter_method = getattr(self, parameter_name)

            if not validate_only:
                parameter_method(**parameter_args)

    # State transitions
    @abc.abstractmethod
    def handle_setup(self):
        """

        :return:
        """
        pass

    @abc.abstractmethod
    def handle_start(self):
        """

        :return:
        """
        pass

    @abc.abstractmethod
    def handle_stop(self):
        """

        :return:
        """
        pass

    @abc.abstractmethod
    def handle_pause(self):
        """

        :return:
        """
        pass

    @abc.abstractmethod
    def handle_resume(self):
        """

        :return:
        """
        pass

    @abc.abstractmethod
    def handle_error(self):
        """

        :return:
        """
        pass

    # object overrides
    def __str__(self) -> str:
        return self.get_hardware_instance_description()

    def __repr__(self) -> str:
        return f"<{self.__class__.__module__}.{self.__class__.__qualname__} ({self.get_hardware_identifier()}) object>"

    # Make decorators static
    register_measurement = staticmethod(register_measurement)
    register_parameter = staticmethod(register_parameter)


class HardwareStateManager(LoggerObject):
    def __init__(self, hardware: Hardware, exporter: Exporter):
        super().__init__(logger_append=f":{hardware.get_hardware_identifier()}")

        # Target hardware object
        self._hardware = hardware

        # Target for measurements
        self._exporter = exporter

        # State machine for managing hardware state
        self._state = Machine(states=list(_HARDWARE_STATE.values()), transitions=_HARDWARE_TRANSITIONS,
                              initial=_HARDWARE_STATE['idle'], before_state_change=self._callback_before,
                              after_state_change=self._callback_after)

        # Queue for hardware configuration
        self._parameter_queue = queue.Queue()

        # Cache for reconfiguration of instrument after a fault
        self._parameter_cache = {}

    def _record(self, record_type: RecordType, record: TYPING_RECORD, extra_tags: typing.Optional[TYPING_TAGS] = None):
        extra_tags = extra_tags or {}

        # Append source hardware identifier
        extra_tags.update({
            'source': self._hardware.get_hardware_identifier()
        })

        self._exporter.record(datetime.now(), record_type, record, extra_tags)

    def _callback_before(self):
        pass

    def _callback_after(self):
        pass

    def _event_setup(self):
        self._logger.info('Entering setup state')
        self._hardware.handle_setup()
        self._logger.debug('Setup transition OK')

    def _event_start(self):
        self._logger.info('Entering start state')
        self._hardware.handle_start()
        self._logger.debug('Start transition OK')

    def _event_stop(self):
        self._logger.info('Entering stop state')
        self._hardware.handle_stop()
        self._logger.debug('Stop transition OK')

    def _event_pause(self):
        self._logger.info('Entering pause state')
        self._hardware.handle_pause()
        self._logger.debug('Pause transition OK')

    def _event_resume(self):
        self._logger.info('Entering resume state')
        self._hardware.handle_resume()
        self._logger.debug('Resume transition OK')

    def _event_error(self):
        self._logger.info('Entering error state')
        self._hardware.handle_error()
        self._logger.debug('Error state transition OK')

    def _thread_state_manager(self):
        pass


# Import submodules automatically
import_submodules(__name__)
