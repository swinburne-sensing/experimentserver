from __future__ import annotations

import abc
import time
import typing
from datetime import datetime, timedelta

from ... import ApplicationException
from ...config import ConfigManager
from ...data import TYPE_TAG_DICT, Measurement
from ...data.measurement import dynamic_field_time_delta
from ...hardware import HardwareManager, HardwareTransition, ParameterError
from ...hardware.metadata import BoundMetadataCall, TYPE_PARAMETER_DICT
from ...util.constant import FORMAT_TIMESTAMP
from ...util.logging import LoggerObject
from ...util.module import class_instance_from_str, get_all_subclasses, import_submodules, TrackedIdentifierError
from ...util.uniqueid import hex_str


class StageConfigurationError(ApplicationException):
    """ Exception for import/export errors. """
    pass


class StageRuntimeError(ApplicationException):
    """ Exception for stage runtime errors. """
    pass


class BaseStage(LoggerObject, metaclass=abc.ABCMeta):
    """ Stages form the basis of an experimental procedure. Stages are entered at their beginning, run multiple times
        until flagging completion. Stages can be exported to/imported from dict objects. """

    # Export version indicator
    EXPORT_VERSION = 1
    _EXPORT_COMPATIBILITY = 1,

    def __init__(self, config: ConfigManager, uid: typing.Optional[str] = None,
                 parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 metadata: typing.Optional[TYPE_TAG_DICT] = None):
        """ Create new Stage instance.

        :param config:
        :param uid: stage unique identifier (auto generated if not provided)
        :param parameters: hardware parameters (or dependencies) applied in stage
        :param metadata: metadata to apply to measurements made during this stage
        """
        # Shared procedure configuration
        self._stage_config = config

        # Stage ID
        self._stage_uid = uid or hex_str()

        super(BaseStage, self).__init__(logger_name_postfix='_' + self._stage_uid)

        # Hardware parameters
        self._stage_parameters = parameters or {}

        # Metadata tied to stage
        self._stage_metadata = metadata or {}

        # Timestamps
        self._stage_enter_timestamp: typing.Optional[datetime] = None
        self._stage_pause_timestamp: typing.Optional[datetime] = None

        # Buffer for bound hardware parameter calls
        self._stage_hardware_parameters_bound: typing.List[typing.Tuple[HardwareManager,
                                                                        typing.List[BoundMetadataCall]]] = []

    @staticmethod
    def get_config_dependencies() -> typing.Optional[typing.Sequence[str]]:
        """ Get string settings that must be defined for this stage to function.

        :return: optional str sequence
        """
        return None

    def get_stage_hardware(self) -> typing.List[str]:
        return list(self._stage_parameters.keys())

    @abc.abstractmethod
    def get_stage_duration(self) -> typing.Optional[timedelta]:
        """ Get estimate of runtime duration, may be None if duration is unknown.

        :return: compatible time type or None
        """
        pass

    def get_stage_remaining(self) -> typing.Optional[timedelta]:
        """ Get estimate of remaining duration, may be None if duration is unknown.

        :return: compatible time type or None
        """
        # Indicate unknown duration
        if self.get_stage_duration() is None:
            return None

        # Indicate zero duration
        if self.get_stage_duration().total_seconds() == 0:
            return timedelta()

        if self._stage_enter_timestamp is not None:
            remaining = self.get_stage_duration() - (datetime.now() - self._stage_enter_timestamp)

            if remaining.total_seconds() > 0:
                return remaining
            else:
                return timedelta()
        else:
            return self.get_stage_duration()

    def get_stage_summary(self) -> typing.Sequence[typing.Union[str, typing.Sequence[str]]]:
        """ Get a string description (or multiple descriptions) of this stages behaviour.

        :return:
        """
        lines = []

        for tag_key, tag_value in self._stage_metadata.items():
            lines.append(f"Add metadata \"{tag_key}\": \"{tag_value}\"")

        # If validated use bound commands
        if len(self._stage_hardware_parameters_bound) > 0:
            for hardware_manager, hardware_parameters in self._stage_hardware_parameters_bound:
                hardware = hardware_manager.get_hardware()

                lines.append(f"Configure {hardware.get_hardware_instance_description()}")

                parameter_lines = []

                for parameter_command in hardware_parameters:
                    parameter_lines.append(f"{parameter_command!s}")

                lines.append(parameter_lines)
        else:
            for hardware_identifier, hardware_parameters in self._stage_parameters.items():
                lines.append(f"Configure {hardware_identifier}")

                parameter_lines = []

                for parameter_command, parameter_args in hardware_parameters.items():
                    parameter_lines.append(f"{parameter_command}({parameter_args})")

                lines.append(parameter_lines)

        return lines

    def stage_validate(self) -> typing.NoReturn:
        """ Validate stage. Should throw

        :raise TrackedIdentifierError: when a required hardware identifier is missing or the wrong type
        """
        # Clear bound parameters
        self._stage_hardware_parameters_bound = []

        # Check for any dependencies
        dependencies = self.get_config_dependencies()

        if dependencies is not None:
            for dependent in dependencies:
                if dependent not in self._stage_config:
                    raise StageRuntimeError(f"Missing required configuration parameter: {dependent}")

        for hardware_identifier, hardware_parameters in self._stage_parameters.items():
            if hardware_parameters is not None:
                try:
                    self.get_logger().debug(f"Testing parameters {hardware_identifier}: {hardware_parameters!r}")

                    # Test hardware is available
                    hardware_manager = HardwareManager.get_instance(hardware_identifier)

                    # Test parameters
                    bound_parameters = hardware_manager.get_hardware().bind_parameter(hardware_parameters)

                    # Save bound parameters
                    self._stage_hardware_parameters_bound.append((hardware_manager, bound_parameters))
                except TrackedIdentifierError:
                    raise StageRuntimeError(f"Hardware identifier {hardware_identifier} not available")
                except ParameterError as exc:
                    raise StageRuntimeError(f"Invalid parameter set for {hardware_identifier} (error: {exc!s})")

    def stage_enter(self) -> typing.NoReturn:
        """ Called upon stage entry. """
        self.get_logger().debug('Entering stage')

        # Save entry time
        self._stage_enter_timestamp = datetime.now()

        # Apply stage metadata
        Measurement.push_metadata()

        Measurement.add_tags({
            'stage_uid': self._stage_uid,
            'stage_type': self.__class__.__name__,
            'stage_state': 'running',
            'stage_time': time.strftime(FORMAT_TIMESTAMP),
            'stage_timestamp': time.time(),
        })
        Measurement.add_dynamic_field('time_delta_stage', dynamic_field_time_delta(datetime.now()))

        if len(self._stage_metadata) > 0:
            Measurement.add_tags(self._stage_metadata)

        # Queue stage parameters
        for hardware_manager, parameters_bound_list in self._stage_hardware_parameters_bound:
            self.get_logger().debug(f"Queue {hardware_manager.get_hardware().get_hardware_identifier()} parameters")

            hardware_manager.queue_transition(HardwareTransition.PARAMETER, parameters_bound_list, block=False)

    def stage_pause(self) -> typing.NoReturn:
        """ Called upon stage pause. """
        self.get_logger().debug('Pausing stage')

        # Save pause timestamp
        self._stage_pause_timestamp = datetime.now()

        if len(self._stage_metadata) > 0:
            Measurement.add_tag('stage_state', 'paused')

    @abc.abstractmethod
    def stage_run(self) -> bool:
        """ Called multiple times while stage is current.

        :return: True if still active, False otherwise.
        """
        pass

    def stage_exit(self) -> typing.NoReturn:
        """ Called upon stage completion. """
        # Restore metadata
        if len(self._stage_metadata) > 0:
            Measurement.pop_metadata()

        # Clear timestamps
        self._stage_enter_timestamp = None
        self._stage_pause_timestamp = None

        self.get_logger().debug('Exiting stage')

    def stage_resume(self) -> typing.NoReturn:
        """ Called upon stage resume. """
        if len(self._stage_metadata) > 0:
            Measurement.add_tag('stage_state', 'running')

        self.get_logger().debug('Resuming stage')

    # Import/export
    @classmethod
    def stage_import(cls, procedure_config: ConfigManager, data: typing.Dict[str, typing.Any]) -> BaseStage:
        """ Generate Stage from a previously exported dict.

        :param procedure_config:
        :param data:
        :return:
        """
        # Get properties
        try:
            target_class = data.pop('class')
        except KeyError:
            raise StageConfigurationError(f"Stage could not be imported, missing class property")

        target_version = data.pop('version', None)

        # Add config
        data['config'] = procedure_config

        if target_version is not None and target_version not in cls._EXPORT_COMPATIBILITY:
            raise StageConfigurationError(f"Stage could not be imported, version {target_version} not in "
                                          f"compatibility list ({', '.join(map(str, cls._EXPORT_COMPATIBILITY))})")

        # Create class
        stage = class_instance_from_str(target_class, __name__, **data)

        # Check for valid subclass
        if not issubclass(stage.__class__, cls):
            raise StageConfigurationError(f"{target_class} not a valid subclass of {cls.__name__}")

        return stage

    @abc.abstractmethod
    def stage_export(self) -> typing.Dict[str, typing.Any]:
        """ Export Stage to a dict.

        :return: dict
        """
        stage_class_fqn = self.__class__.__module__ + '.' + self.__class__.__qualname__

        if __name__ not in stage_class_fqn:
            raise StageConfigurationError(f"Stage class {stage_class_fqn} is not a member of stage module {__name__}")

        # Get partially qualified name
        stage_class_pqn = stage_class_fqn[len(__name__) + 1:]

        stage = {
            'class': stage_class_pqn,
            'version': self.EXPORT_VERSION,
            'uid': self._stage_uid
        }

        if len(self._stage_metadata) > 0:
            stage['metadata'] = self._stage_metadata

        if len(self._stage_parameters) > 0:
            stage['parameters'] = self._stage_parameters

        return stage


TYPE_STAGE = typing.TypeVar('TYPE_STAGE', bound=BaseStage)


# Import submodules automatically
import_submodules(__name__)
