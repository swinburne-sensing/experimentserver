from __future__ import annotations

import abc
import contextlib
import functools
import inspect
import sys
import threading
import typing

from experimentlib.logging.classes import LoggedAbstract
from experimentlib.util.classes import HybridMethod
from transitions import EventData

import experimentserver.util.metadata as metadata
from experimentserver.measurement import T_FIELD_MAP, T_TAG_MAP, T_DYNAMIC_FIELD_MAP, MeasurementSource, Measurement, MeasurementTarget
from experimentserver.hardware.error import HardwareIdentifierError, MeasurementError, MeasurementUnavailable, \
    ParameterError, NoResetHandler
from experimentserver.hardware.metadata import TYPE_PARAMETER_DICT, TYPE_PARAMETER_COMMAND, _MeasurementMetadata, \
    _ParameterMetadata
from experimentserver.util.metadata import BoundMetadataCall
from experimentserver.util.thread import ThreadLock


class Hardware(LoggedAbstract, MeasurementSource):
    """ Base class for all Hardware objects controlled in experiments.

    Hardware can represent instrumentation used to perform measurements (like a multimeter), or equipment that controls
    experimental parameters (like temperature or gas flow).

    If implementing new hardware modules then this should be the parent class (or one of its abstract children like
    VISAHardware or SCPIHardware if applicable).
    """

    # List of active hardware instances
    _HARDWARE_LUT: typing.MutableMapping[str, Hardware] = {}
    _HARDWARE_LUT_LOCK = threading.RLock()

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

        LoggedAbstract.__init__(self, identifier)
        MeasurementSource.__init__(self)

        self._identifier = identifier

        # Hardware interaction lock
        self._hardware_lock = ThreadLock(f"{self._identifier}:hardware", self._HARDWARE_LOCK_TIMEOUT)

        # Parameters to always configure
        self._initial_parameters = self.bind_parameter(parameters)

        # Parameter buffer (for resets)
        self._parameter_lock = threading.RLock()
        self._parameter_buffer: typing.Dict[int, metadata.BoundMetadataCall] = {}

        # Enabled measurement map
        self._enabled_measurement_lock = threading.RLock()
        self._enabled_measurement: typing.Optional[typing.MutableMapping[str, bool]] = None
        self._measurement_delay: typing.Optional[float] = None

        # Add to list of instances
        with self._HARDWARE_LUT_LOCK:
            if identifier in self._HARDWARE_LUT:
                raise HardwareIdentifierError(f"Identifier conflict, \"{identifier}\" already defined")

            # Save instance
            self._HARDWARE_LUT[self._identifier] = self

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
            return str(getattr(class_module, '__author__'))
        elif hasattr(class_module, '__email__'):
            return str(getattr(class_module, '__email__'))

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
            return ''.join([x.capitalize() for x in self._identifier.split('_')])
        else:
            return self._identifier

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
    def hardware_lock(self, timeout: typing.Optional[float] = None, quiet: bool = False, **kwargs: typing.Any) \
            -> typing.Generator[None, None, None]:
        """ Acquire exclusive reentrant hardware lock. Checks for any errors when released.

        :param origin:
        :param timeout:
        :param quiet:
        :return: lock depth
        """
        with self._hardware_lock.lock(origin, timeout, quiet) as depth:
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
    def _validate_measurement(self, name: str) -> bool:
        measurement_metadata = self.get_hardware_measurement_metadata()

        return name in measurement_metadata

    @register_parameter(description='Enable a single measurement method', order=0,
                        validation={'name': _validate_measurement})
    def set_hardware_measurement(self, name: typing.Optional[str] = None) -> None:
        """

        :param name:
        :return:
        """
        if self._enabled_measurement is None:
            raise MeasurementError('Measurement map not initialized')
        
        if name not in self._enabled_measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._enabled_measurement_lock:
            # Disable all other measurements
            for measure in self._enabled_measurement.keys():
                self._enabled_measurement[measure] = False

            if name is not None:
                # Enable the selected measurement
                self.enable_hardware_measurement(name)

        self.logger().info(f"Configured measurement: {name}")

    @register_parameter(description='Enable a measurement method', order=0,
                        validation={'name': _validate_measurement})
    def enable_hardware_measurement(self, name: str) -> None:
        """

        :param name:
        :return:
        """
        if self._enabled_measurement is None:
            raise MeasurementError('Measurement map not initialized')

        if name not in self._enabled_measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._enabled_measurement_lock:
            self._enabled_measurement[name] = True

        self.logger().info(f"Enabled measurement: {name}")

    @register_parameter(description='Disable a measurement method', order=0,
                        validation={'name': _validate_measurement})
    def disable_hardware_measurement(self, name: str) -> None:
        """

        :param name:
        :return:
        """
        if self._enabled_measurement is None:
            raise MeasurementError('Measurement map not initialized')

        if name not in self._enabled_measurement:
            raise MeasurementError(f"Unrecognised measurement name {name}")

        with self._enabled_measurement_lock:
            self._enabled_measurement[name] = False

        self.logger().info(f"Disabled measurement: {name}")

    def produce_measurement(self, extra_dynamic_fields: typing.Optional[T_DYNAMIC_FIELD_MAP] = None,
                            extra_tags: typing.Optional[T_TAG_MAP] = None) -> bool:
        """ 

        :return: True if measurements were produced, False otherwise
        """
        extra_tags = extra_tags or {}

        measurement_flag = False

        if self._measurement_delay is not None:
            self.sleep(self._measurement_delay, 'measurement rate limit')

        with self._enabled_measurement_lock:
            if self._enabled_measurement is None:
                raise MeasurementError('Measurements have not been configured, hardware may be disconnected')

            # Get enabled measurements
            measurement_meta: typing.MutableMapping[str, _MeasurementMetadata] = self.get_hardware_measurement_metadata()

            # Discard disabled methods
            for measurement, measurement_enabled in self._enabled_measurement.items():
                if not measurement_enabled and not measurement_meta[measurement].force:
                    measurement_meta.pop(measurement)

            with self.hardware_lock('produce_measurement'):
                for meta in measurement_meta.values():
                    # Bind method
                    measurement_method = meta.bind(self)

                    try:
                        measurement_return = typing.cast(
                            typing.Union[None, Measurement, typing.Sequence[Measurement], T_FIELD_MAP],
                            measurement_method()
                        )

                        if measurement_return is not None:
                            if isinstance(measurement_return, Measurement):
                                # Single result with metadata
                                measurement_return.add_tags(extra_tags)

                                if extra_dynamic_fields is not None:
                                    for field_name, field_callable in extra_dynamic_fields.items():
                                        measurement_return.add_field(field_name, field_callable(measurement_return))

                                MeasurementTarget.record(measurement_return)
                            elif isinstance(measurement_return, list):
                                measurement_return = typing.cast(typing.List[Measurement], measurement_return)

                                # Multiple results with metadata
                                for measurement in measurement_return:
                                    measurement.add_tags(extra_tags)

                                    if extra_dynamic_fields is not None:
                                        for field_name, field_callable in extra_dynamic_fields.items():
                                            measurement.add_field(field_name, field_callable(measurement))

                                    MeasurementTarget.record(measurement)
                            elif isinstance(measurement_return, dict):
                                if meta.measurement_group is None:
                                    raise MeasurementError(
                                        f"Missing measurement_group on method: {meta!r}"
                                    )

                                measurement_return = typing.cast(T_FIELD_MAP, measurement_return)

                                # Single result without metadata
                                MeasurementTarget.record(
                                    Measurement(
                                        self,
                                        meta.measurement_group,
                                        measurement_return,
                                        dynamic_fields=extra_dynamic_fields,
                                        tags=extra_tags
                                    )
                                )
                            else:
                                raise MeasurementError(
                                    f"Unexpected return from measurement method: {measurement_return!r}"
                                )

                            # Indicate a measurement was produced
                            measurement_flag = True
                    except MeasurementUnavailable:
                        # Ignore unavailable measurements
                        pass

        return measurement_flag

    def bind_parameter(self, parameter_command: TYPE_PARAMETER_COMMAND) \
            -> typing.List[metadata.BoundMetadataCall]:
        """

        :param parameter_command:
        :return:
        :raises ParameterError: on invalid parameters or parameter arguments
        """
        # Ignore empty lists
        if parameter_command is None:
            return []

        # Parse list of bindings into single set
        if isinstance(parameter_command, list):
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

            if not isinstance(parameter_args, dict):
                if isinstance(parameter_args, str):
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

    def _parse_parameter_event(self, event: EventData) -> typing.Optional[typing.List[BoundMetadataCall]]:
        if len(event.args) == 0:
            raise ParameterError('Missing arguments to parameter handler')
        elif len(event.args) > 1:
            raise ParameterError('Too many arguments passed to parameter handler')

        # Get parameter arguments
        parameter_command = event.args[0]

        # If not bound then do that now
        if not isinstance(parameter_command, list) or \
            not all((isinstance(x, BoundMetadataCall) for x in parameter_command)):
            self.logger().debug('Parameters bound inside state transition')
            parameter_command = self.bind_parameter(parameter_command)

        # Ignore empty lists
        if len(parameter_command) == 0:
            return None

        return parameter_command

    def _handle_parameter(self, event: EventData) -> None:
        """ Handle application of parameters from a normal (non-error) state.

        :param event:
        """
        # Get parameter arguments
        parameter_command = self._parse_parameter_event(event)

        if parameter_command is not None:
            with self._parameter_lock:
                with self.hardware_lock('_handle_parameter'):
                    # Sort parameters based on order before calling the appropriate method
                    for parameter_call in sorted(parameter_command):
                        # Call bound method
                        parameter_call()

                        # Buffer parameter after successful execution
                        self._parameter_buffer[hash(parameter_call)] = parameter_call

    def _buffer_parameters(self, event: EventData) -> None:
        """ Handling buffering of parameters inside an error state.

        :param event:
        :return:
        """
        # Get parameter arguments
        parameter_command = self._parse_parameter_event(event)

        if parameter_command is not None:
            with self._parameter_lock:
                with self.hardware_lock('_buffer_parameters'):
                    # Sort parameters based on order before calling the appropriate method
                    for parameter_call in sorted(parameter_command):
                        # Buffer parameter after successful execution
                        self._parameter_buffer[hash(parameter_call)] = parameter_call

    # State transition handling
    @abc.abstractmethod
    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        """ Called during connection to external hardware. This method should handle connection to an instrument without
        any configuration.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during connection
        :raises HardwareError: when Hardware reports fatal error during connection
        """
        # Configure default measurements
        with self._enabled_measurement_lock:
            self._enabled_measurement = {
                measure: meta.default for measure, meta in self.get_hardware_measurement_metadata().items()
            }

            enabled_list = ', '.join([measure for measure, enabled in self._enabled_measurement.items() if enabled])

            self.logger().info(f"Configured default measurements: {enabled_list}")

    @abc.abstractmethod
    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        """ Called during disconnection from external hardware. Should handle cleaning up of all connection resources.

        Note that is method is not called when disconnection occurs as the result of an error.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during disconnection
        :raises HardwareError: when Hardware reports fatal error during disconnection
        """
        pass

    @abc.abstractmethod
    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
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
    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        """ Called in preparation for release of external hardware. Should return hardware to a safe/known configuration
        such that reconfiguration is ready to be performed.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during cleanup
        :raises HardwareError: when Hardware reports fatal error during cleanup
        """
        # Clear enabled measurements
        with self._enabled_measurement_lock:
            self._enabled_measurement = None

        # Clear buffered parameters
        with self._parameter_lock:
            self._parameter_buffer.clear()

    def transition_start(self, event: typing.Optional[EventData] = None) -> None:
        """ Called to begin generation of measurements. Should prepare the hardware for data acquisition.

        :param event: transition metadata
        :raises CommunicationError: when communication error occurs during start
        :raises HardwareError: when Hardware reports fatal error during start
        """
        pass

    def transition_stop(self, event: typing.Optional[EventData] = None) -> None:
        """ Called to stop generation of measurements. Should return the hardware to a configured/ready manager.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication error occurs during stop
        :raises HardwareReportedError: when Hardware reports fatal error during stop
        """
        pass

    @abc.abstractmethod
    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        """ Called when hardware manager transition results in an error, or when any communication or hardware reported
        error occurs.

        Note that is should be fail-safe, communication cannot be guaranteed and errors should be ignored or discarded.

        :param event: transition metadata
        """
        self.logger().error(f"Hardware {self.get_hardware_identifier()} has entered an error state")

    def transition_reset(self, event: typing.Optional[EventData] = None) -> None:
        """ Optionally called when resetting from an error state.

        Note that reset will transition the hardware to the state saved prior to the occurrence of the error.

        Note that errors raised during this call will return the hardware to an error state.

        :param event: transition metadata
        :raises HardwareCommunicationError: when communication errors occur during reset
        :raises HardwareReportedError: when Hardware reports fatal error during reset
        """
        raise NoResetHandler()

    # object overrides
    def __str__(self) -> str:
        return self.get_hardware_instance_description()

    def __repr__(self) -> str:
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}(identifier={self.get_hardware_identifier()})"

    # Convert decorators to static methods
    register_measurement = staticmethod(register_measurement)  # type: ignore
    register_parameter = staticmethod(register_parameter)  # type: ignore


TYPE_HARDWARE = typing.TypeVar('TYPE_HARDWARE', bound=Hardware)
