import abc
import contextlib
import functools
import inspect
import sys
import threading
import typing

from transitions import EventData

import experimentserver.util.metadata as metadata
from ..error import MeasurementError, MeasurementUnavailable, ParameterError, NoResetHandler
from ..metadata import TYPE_PARAMETER_DICT, _MeasurementMetadata, _ParameterMetadata
from experimentserver.data.measurement import MeasurementSource, Measurement, MeasurementTarget
from experimentserver.util.logging import LoggerObject
from experimentserver.util.metadata import BoundMetadataCall
from experimentserver.util.module import HybridMethod, AbstractTracked
from experimentserver.util.thread import ThreadLock


class Hardware(AbstractTracked, LoggerObject, MeasurementSource):
    """ Base class for all Hardware objects controlled in experiments.

    Hardware can represent instrumentation used to perform measurements (like a multimeter), or equipment that controls
    experimental parameters (like temperature or gas flow).

    If implementing new hardware modules then this should be the parent class (or one of its abstract children like
    VISAHardware or SCPIHardware if applicable).
    """

    # Timeout to prevent deadlocks
    _HARDWARE_LOCK_TIMEOUT = 30

    def __init__(self, identifier: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        """ Instantiate Hardware object.

        Note this class is abstract and several methods must be implemented in child classes.

        :param identifier: unique identifier
        :param parameters: parameters to always configure
        """
        # Force identifier to be lower case and stripped
        identifier = identifier.strip().lower()

        AbstractTracked.__init__(self, identifier)
        LoggerObject.__init__(self, logger_name_postfix=f":{self._identifier}")
        MeasurementSource.__init__(self)

        # Hardware interaction lock
        self._hardware_lock = ThreadLock(f"{self._identifier}:hardware", self._HARDWARE_LOCK_TIMEOUT)

        # Parameters to always configure
        self._initial_parameters = self.bind_parameter(parameters)

        # Parameter buffer (for resets)
        self._parameter_lock = threading.RLock()
        self._parameter_buffer: typing.Dict[int, metadata.BoundMetadataCall] = {}

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
    def get_hardware_class_description() -> str:
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
            return ''.join([x.capitalize() for x in self.get_identifier().split('_')])
        else:
            return self.get_identifier()

    def get_hardware_instance_description(self) -> str:
        """ Provide a description of this particular instance of Hardware.

        :return: str
        """
        return f"{self.get_hardware_class_description()} ({self.get_hardware_identifier()})"

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

    # Parameter/measurement registration
    register_measurement = functools.partial(metadata.register_metadata, _MeasurementMetadata)
    register_parameter = functools.partial(metadata.register_metadata, _ParameterMetadata)

    @HybridMethod
    def get_hardware_measurement_metadata(self, include_force: bool = True) \
            -> typing.MutableMapping[str, _MeasurementMetadata]:
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
    def get_hardware_parameter_metadata(self) -> typing.MutableMapping[str, _ParameterMetadata]:
        """ Gets a list of all configurable parameters for this class.

        This returns a dict containing all methods that are registered using the Hardware.register_parameter decorator.

        This ia a hybrid method, it can be called bound to a class or as a class method.

        :return: dict containing all methods registered as configurable parameters
        """
        return metadata.get_metadata(self, _ParameterMetadata)

    # Parameter/measurement getter and setter
    def _validate_measurement(self, name):
        measurement_metadata = self.get_hardware_measurement_metadata()

        return name in measurement_metadata

    @register_parameter(description='Enable a single measurement method', order=0,
                        validation={'name': _validate_measurement})
    def set_hardware_measurement(self, name: typing.Optional[str] = None) -> typing.NoReturn:
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

        self.get_logger().info(f"Configured measurement: {name}")

    @register_parameter(description='Enable a measurement method', order=0,
                        validation={'name': _validate_measurement})
    def enable_hardware_measurement(self, name: str) -> typing.NoReturn:
        """

        :param name:
        :return:
        """
        if name not in self._measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            self._measurement[name] = True

        self.get_logger().info(f"Enabled measurement: {name}")

    @register_parameter(description='Disable a measurement method', order=0,
                        validation={'name': _validate_measurement})
    def disable_hardware_measurement(self, name: str) -> typing.NoReturn:
        """

        :param name:
        :return:
        """
        if name not in self._measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._measurement_lock:
            self._measurement[name] = False

        self.get_logger().info(f"Disabled measurement: {name}")

    def produce_measurement(self) -> bool:
        """

        :return: True if measurements were produced, False otherwise
        """
        measurement_flag = False

        with self._measurement_lock:
            if self._measurement is None:
                raise MeasurementError('Measurements have not been configured, hardware may be disconnected')

            # Get enabled measurements
            measurement_meta = self.get_hardware_measurement_metadata()

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

    def bind_parameter(self, parameter_command: typing.Union[None, TYPE_PARAMETER_DICT,
                                                             typing.List[TYPE_PARAMETER_DICT]]) \
            -> typing.List[metadata.BoundMetadataCall]:
        """

        :param parameter_command:
        :return:
        :raises: ParameterException on invalid parameters or parameter arguments
        """
        # Ignore empty lists
        if parameter_command is None:
            return []

        # Parse list of bindings into single set
        if type(parameter_command) is list:
            return_list = []

            for parameter_command_inst in parameter_command:
                return_list.extend(self.bind_parameter(parameter_command_inst))

            return return_list

        parameter_bound = []
        parameter_metadata_list = self.get_hardware_parameter_metadata()

        for parameter_name, parameter_args in parameter_command.items():
            try:
                parameter_metadata = parameter_metadata_list[parameter_name]
            except KeyError:
                raise ParameterError(f"Unrecognised parameter {parameter_name}")

            if type(parameter_args) is not dict:
                if type(parameter_args) is str:
                    parameter_args = [x.strip() for x in parameter_args.split(',')]

                # If arguments are not iterable then convert to a tuple
                try:
                    _ = iter(parameter_args)
                except TypeError:
                    parameter_args = parameter_args,

                # Use arguments from array as positional arguments
                parameter_args_name = list(inspect.signature(parameter_metadata.method).parameters)[1:]

                # Generate mapping
                parameter_kwargs = dict(zip(parameter_args_name[:len(parameter_args)], parameter_args))
            else:
                parameter_kwargs = parameter_args

            parameter_bound.append(parameter_metadata.bind(self, **parameter_kwargs))

        return parameter_bound

    def _handle_parameter(self, event: EventData) -> typing.NoReturn:
        """ Handle application of parameters from a normal (non-error) state.

        :param event:
        """
        if len(event.args) == 0:
            raise ParameterError('Missing arguments to parameter handler')
        elif len(event.args) > 1:
            raise ParameterError('Too many arguments passed to parameter handler')

        # Get parameter arguments
        parameter_command = event.args[0]

        # If not bound then do that now
        if type(parameter_command) is not list and not all([type(x) is BoundMetadataCall for x in parameter_command]):
            parameter_command = self.bind_parameter(parameter_command)

        # Ignore empty lists
        if len(parameter_command) == 0:
            return

        with self._parameter_lock:
            with self.hardware_lock():
                # Sort parameters based on order before calling the appropriate method
                for parameter_call in sorted(parameter_command):
                    # Call bound method
                    parameter_call()

                    # Buffer parameter after successful execution
                    self._parameter_buffer[hash(parameter_call)] = parameter_call

    def _buffer_parameters(self, event: EventData) -> typing.NoReturn:
        """ Handling buffering of parameters from an error state.

        :param event:
        :return:
        """
        bound_parameters = self.bind_parameter(*event.args, **event.kwargs)

        # Sort parameters based on order before calling the appropriate method
        with self._parameter_lock:
            with self.hardware_lock():
                for parameter_call in sorted(bound_parameters):
                    # Buffer parameter
                    self._parameter_buffer[hash(parameter_call)] = parameter_call

    # State transition handling
    @abc.abstractmethod
    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Called during connection to external hardware. This method should handle connection to an instrument without
        any configuration.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during connection
        :raises HardwareError: when Hardware reports fatal error during connection
        """
        # Configure default measurements
        with self._measurement_lock:
            self._measurement = {
                measure: meta.default for measure, meta in self.get_hardware_measurement_metadata().items()
            }

            enabled_list = ', '.join([measure for measure, enabled in self._measurement.items() if enabled])

            self.get_logger().info(f"Configured default measurements: {enabled_list}")

    @abc.abstractmethod
    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Called during disconnection from external hardware. Should handle cleaning up of all connection resources.

        Note that is method is not called when disconnection occurs as the result of an error.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during disconnection
        :raises HardwareError: when Hardware reports fatal error during disconnection
        """
        pass

    @abc.abstractmethod
    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Called after connection but before generating measurements. Should send any necessary configuration commands
        to instrument and prepare for dat acquisition.

        :param event: transition metadata
        :raises CommandError: when invalid commands are run during configuration
        :raises CommunicationError: when communication error occurs during configuration
        :raises HardwareError: when Hardware reports fatal error during configuration
        """
        # If parameters were previously provided then configure them now
        with self._parameter_lock:
            # Get initial parameters
            if self._initial_parameters is not None:
                parameter_list = {hash(x): x for x in self._initial_parameters}
            else:
                parameter_list = {}

            # Check for overwritten parameters
            for parameter in self._parameter_buffer.values():
                parameter_list[hash(parameter)] = parameter

            # Apply parameters
            for parameter_call in sorted(parameter_list.values()):
                parameter_call()

    @abc.abstractmethod
    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Called in preparation for release of external hardware. Should return hardware to a safe/known configuration
        such that reconfiguration is ready to be performed.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during cleanup
        :raises HardwareError: when Hardware reports fatal error during cleanup
        """
        # Clear enabled measurements
        with self._measurement_lock:
            self._measurement = None

        # Clear buffered parameters
        with self._parameter_lock:
            self._parameter_buffer.clear()

    def transition_start(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Called to begin generation of measurements. Should prepare the hardware for data acquisition.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during start
        :raises HardwareError: when Hardware reports fatal error during start
        """
        pass

    def transition_stop(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Called to stop generation of measurements. Should return the hardware to a configured/ready manager.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during stop
        :raises HardwareReportedError: when Hardware reports fatal error during stop
        """
        pass

    @abc.abstractmethod
    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Called when hardware manager transition results in an error, or when any communication or hardware reported
        error occurs.

        Note that is should be fail-safe, communication cannot be guaranteed and errors should be ignored or discarded.

        :param event: transition metadata
        """
        self.get_logger().error(f"Hardware {self.get_hardware_identifier()} has entered an error manager", event=True,
                                notify=True)

    def transition_reset(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        """ Optionally called when resetting from an error manager. Note that reset will transition the hardware to the
        manager prior to the occurrence of the error.

        Note that errors raised during this call will return the hardware to an error manager.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication errors occur during reset
        :raises HardwareReportedError: when Hardware reports fatal error during reset
        """
        raise NoResetHandler()

    # object overrides
    def __str__(self) -> str:
        return self.get_hardware_instance_description()

    def __repr__(self) -> str:
        return f"<{self.__class__.__module__}.{self.__class__.__qualname__} ({self.get_hardware_identifier()}) object>"

    # Convert decorators to static methods
    register_measurement = staticmethod(register_measurement)
    register_parameter = staticmethod(register_parameter)
