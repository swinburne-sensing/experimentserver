from __future__ import annotations

import abc
import copy
import typing
from datetime import datetime, timedelta

from experimentserver import ApplicationException
from experimentserver.data import Measurement, TYPE_TIME, to_timedelta, TYPE_TAG_DICT
from experimentserver.hardware import ParameterError
from experimentserver.hardware.metadata import TYPE_PARAMETER_COMMAND
from experimentserver.hardware.manager import HardwareManager, HardwareTransition
from experimentserver.util.constant import FORMAT_TIMESTAMP
from experimentserver.util.logging import LoggerObject
from experimentserver.util.module import get_all_subclasses
from experimentserver.util.uniqueid import hex_str


class StageConfigurationError(ApplicationException):
    pass


class BaseStage(LoggerObject, metaclass=abc.ABCMeta):
    """ Stages form the basis of any experiment. An experimental procedure is made up from several stages that normally
    advance in sequence, although this may be overridden at runtime.

    A BaseStage may expand to multiple ConcreteStage objects at runtime. """

    # Export version indicator
    _EXPORT_VERSION = 1
    _EXPORT_COMPATIBILITY = 1,

    def __init__(self, stage_id: typing.Optional[str] = None,
                 stage_metadata: typing.Optional[TYPE_TAG_DICT] = None,
                 stage_hardware: typing.Optional[typing.Sequence[str]] = None, stage_type: str = 'stage'):
        super(BaseStage, self).__init__()

        # Stage ID
        self._stage_id = stage_id or hex_str()

        # Metadata tied to stage
        self._stage_metadata = stage_metadata or {}

        # Hardware required for stage
        self._stage_hardware = stage_hardware or []

        # Stage type or label
        self._stage_type = stage_type

    @abc.abstractmethod
    def has_duration(self) -> bool:
        """ Test if stage has a duration (eg. measurements may occur during this stage).

        :return: True if stage takes non-zero duration, False otherwise
        """
        pass

    @abc.abstractmethod
    def get_duration(self) -> typing.Optional[TYPE_TIME]:
        """ Get estimate of runtime duration, may be None if duration is unknown.

        :return: compatible time type or None
        """
        pass

    @abc.abstractmethod
    def get_concrete(self) -> typing.Sequence[ConcreteStage]:
        """ Used to generate procedure for runtime.

        """
        pass

    # Import/export
    def stage_export(self) -> typing.Dict[str, typing.Any]:
        """ Export stage data to a dict.

        :return: dict
        """
        stage = {
            'class': self.__class__.__name__,
            'version': self._EXPORT_VERSION,
            'stage_id': self._stage_id,
            'stage_type': self._stage_type
        }

        if len(self._stage_metadata) > 0:
            stage['stage_metadata'] = self._stage_metadata

        if len(self._stage_hardware) > 0:
            stage['stage_hardware'] = self._stage_hardware

        return stage

    @classmethod
    def stage_import(cls, data: typing.Dict[str, typing.Any]) -> ConcreteStage:
        """ Generate stage from dict.

        :param data:
        :return:
        """
        target_class = data.pop('class')
        target_version = data.pop('version')

        if target_version not in cls._EXPORT_COMPATIBILITY:
            raise StageConfigurationError(f"Data structure could not be imported, version {target_version} not in "
                                          f"compatibility list ({', '.join(map(str, cls._EXPORT_COMPATIBILITY))})")

        # Get all known subclasses
        subclasses = get_all_subclasses(cls)
        subclasses = {subclass.__name__: subclass for subclass in subclasses}

        if target_class not in subclasses:
            raise StageConfigurationError(f"{target_class} not a valid subclass of {cls.__name__}")

        # Create class
        return target_class(**data)


class ConcreteStage(BaseStage, metaclass=abc.ABCMeta):
    def __init__(self, *args, stage_parameters: typing.MutableMapping[str, TYPE_PARAMETER_COMMAND] = None, **kwargs):
        """

        :param args:
        :param stage_parameters:
        :param kwargs:
        """
        super(ConcreteStage, self).__init__(*args, **kwargs)

        # Parameters applied on stage entry
        self._stage_parameters = {}

        # Validate parameters
        hardware_managers = HardwareManager.get_all_instances()

        for hardware_identifier, parameter_set in stage_parameters.items():
            if hardware_identifier not in hardware_managers:
                raise StageConfigurationError(f"Unknown hardware identifier {hardware_identifier}")

            hardware_manager = hardware_managers[hardware_identifier]

            try:
                parameter_set = hardware_manager.get_hardware().bind_parameter(parameter_set)
            except ParameterError:
                raise StageConfigurationError()

            # Store bound parameters
            self._stage_parameters[hardware_identifier] = hardware_manager, parameter_set

    @abc.abstractmethod
    def get_status(self) -> str:
        """ Get status message for stage.

        :return: str
        """
        pass

    def get_concrete(self) -> typing.Sequence[ConcreteStage]:
        # Return a copy of this stage
        return copy.copy(self),

    # Events
    def handle_enter(self) -> typing.NoReturn:
        """ Called upon stage entry. """
        # Apply app_metadata for stages that have some length
        if self.has_duration():
            # Push current app_metadata
            Measurement.push_metadata()

            # Add stage metadata
            Measurement.add_tags({
                'stage_id': self._stage_id,
                'stage_type': self._stage_type,
                'stage_state': 'run'
            })

            # Add extra tags
            Measurement.add_tags(self._stage_metadata)

        # Apply parameters
        for hardware_identifier, parameter_set in self._stage_parameters.items():
            hardware_manager = HardwareManager.get_instance(hardware_identifier)
            hardware_manager.queue_transition(HardwareTransition.PARAMETER, parameter_set)

    def handle_pause(self) -> typing.NoReturn:
        """ Called upon stage pause. """
        if self.has_duration():
            Measurement.add_tag('stage_state', 'pause')
        else:
            self.get_logger().warning('Stage without duration received pause event')

    def handle_run(self) -> bool:
        """ Called at regular intervals during runtime, should indicate when stage has completed.

        :return: True if Stage has completed, False otherwise
        """
        pass

    def handle_resume(self) -> typing.NoReturn:
        """ Called upon stage resume after pause. """
        if self.has_duration():
            Measurement.add_tag('stage_state', 'run')
        else:
            self.get_logger().warning('Stage without duration received resume event')

    def handle_exit(self) -> typing.NoReturn:
        """ Called upon stage exit/completion. """
        if self.has_duration():
            # Restore app_metadata
            Measurement.pop_metadata()


class Delay(ConcreteStage):
    def __init__(self, *args, delay_interval: TYPE_TIME, delay_sync_minute: bool = False, **kwargs):
        super(Delay, self).__init__(*args, **kwargs)

        # Convert interval
        self._delay_interval = to_timedelta(delay_interval)
        self._delay_sync_minute = delay_sync_minute

        # Timestamps, pause must be stored to offset delay on resume
        self._delay_enter_timestamp: typing.Optional[datetime] = None
        self._delay_pause_timestamp: typing.Optional[datetime] = None
        self._delay_exit_timestamp: typing.Optional[datetime] = None

    def has_duration(self) -> bool:
        return True

    def get_duration(self) -> typing.Optional[TYPE_TIME]:
        return self._delay_interval

    def get_status(self) -> str:
        if self._delay_exit_timestamp is None:
            return f"Delay for {self._delay_interval}"
        else:
            return f"Completion at {self._delay_exit_timestamp} (remaining: " \
                   f"{datetime.now() - self._delay_exit_timestamp})"

    def _sync_delay_exit_timestamp(self, offset: typing.Optional[timedelta] = None):
        if offset is not None:
            self._delay_exit_timestamp += offset

        if self._delay_sync_minute:
            # Delay to next full minute
            self._delay_exit_timestamp += timedelta(minutes=1)
            self._delay_exit_timestamp -= timedelta(seconds=self._delay_exit_timestamp.second,
                                                    microseconds=self._delay_exit_timestamp.microsecond)

    def handle_enter(self):
        super(Delay, self).handle_enter()

        # Save entry time
        self._delay_enter_timestamp = datetime.now()

        # Calculate exit timestamp
        self._delay_exit_timestamp = self._delay_enter_timestamp + self._delay_interval

        self._sync_delay_exit_timestamp()

        self.get_logger().info(f"Delay for {self._delay_interval} until "
                               f"{self._delay_exit_timestamp.strftime(FORMAT_TIMESTAMP)}")

        # Add tag
        Measurement.add_tag('delay_interval', self._delay_interval.total_seconds())

    def handle_pause(self):
        super(Delay, self).handle_pause()

        # Save pause timestamp
        self._delay_pause_timestamp = datetime.now()

    def handle_run(self) -> bool:
        # Test for stage completion
        return datetime.now() > self._delay_exit_timestamp

    def handle_resume(self):
        # Update resume timestamp
        self._sync_delay_exit_timestamp(datetime.now() - self._delay_pause_timestamp)

        super(Delay, self).handle_resume()

    def handle_exit(self) -> typing.NoReturn:
        # Clear timestamps
        self._delay_enter_timestamp = None
        self._delay_pause_timestamp = None
        self._delay_exit_timestamp = None

        super().handle_exit()

    def stage_export(self) -> typing.Dict[str, typing.Any]:
        stage = super().stage_export()

        stage.update({
            'delay_interval': self._delay_interval.total_seconds(),
            'delay_sync_minute': self._delay_sync_minute
        })

        return stage


class PulseStage(BaseStage):
    class PulsePhase(Delay):
        def __init__(self, *args, **kwargs):
            super(PulseStage.PulsePhase, self).__init__(*args, **kwargs)

    def __init__(self, *args, pulse_exposure: TYPE_TIME, pulse_recovery: TYPE_TIME, mixture,
                 pulse_setup: typing.Optional[TYPE_TIME] = None, **kwargs):
        super(PulseStage, self).__init__(*args, **kwargs)

        self._pulse_setup = to_timedelta(pulse_setup, True)
        self._pulse_exposure = to_timedelta(pulse_exposure)
        self._pulse_recovery = to_timedelta(pulse_recovery)

    def has_duration(self) -> bool:
        return True

    def get_duration(self) -> typing.Optional[TYPE_TIME]:
        if self._pulse_setup is None:
            return self._pulse_exposure + self._pulse_recovery
        else:
            return self._pulse_setup + self._pulse_exposure + self._pulse_recovery

    def get_concrete(self) -> typing.Sequence[ConcreteStage]:
        phases = []
        
        if self._pulse_setup is not None:
            setup_phase = self.PulsePhase(delay_interval=self._pulse_setup, stage_metadata={
                'pulse_phase': 'setup'
            })

            phases.append(setup_phase)

        # Generate exposure stage
        exposure_stage = self.PulsePhase(delay_interval=self._pulse_exposure, stage_metadata={
            'pulse_phase': 'expose'
        })

        phases.append(exposure_stage)

        recover_stage = self.PulsePhase(delay_interval=self._pulse_recovery, stage_metadata={
            'pulse_phase': 'recover'
        })

        phases.append(recover_stage)

        return phases
