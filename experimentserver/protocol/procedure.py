import io
import threading
import time
import typing
from datetime import datetime, timedelta

import transitions
import yaml

from .. import ApplicationException
from .control import ProcedureState, ProcedureTransition
from .file import YAMLProcedureLoader
from .stage import BaseStage, TYPE_STAGE
from ..config import ConfigManager
from ..data import TYPE_TAG_DICT, Measurement
from ..data.measurement import dynamic_field_time_delta
from ..hardware import HardwareManager, HardwareState, HardwareTransition
from ..util.constant import FORMAT_TIMESTAMP
from ..util.state import ManagedStateMachine
from ..util.uniqueid import hex_str


class ProcedureConfigurationError(ApplicationException):
    pass


class ProcedureRuntimeError(ApplicationException):
    pass


class Procedure(ManagedStateMachine):
    """ Procedures represent experimental protocols. """

    # Limit the maximum repetition rate of the management thread when no measurements are made
    _MINIMUM_RUN_PERIOD = 0.1

    # Export version indicator
    _EXPORT_VERSION = 1
    _EXPORT_COMPATIBILITY = 1,

    def __init__(self, uid: typing.Optional[str] = None,
                 config: typing.Union[None, ConfigManager, typing.Dict[str, typing.Any]] = None,
                 hardware: typing.Optional[typing.Sequence[str]] = None,
                 metadata: typing.Optional[TYPE_TAG_DICT] = None,
                 stages: typing.Optional[typing.List[BaseStage]] = None):
        """ Create new Procedure instance.

        :param uid:
        :param metadata:
        :param stages:
        """
        # Setup state machine
        super(Procedure, self).__init__(None, self, ProcedureState, ProcedureTransition, ProcedureState.SETUP)

        # Stage ID
        self._procedure_uid = uid or hex_str()

        # Shared configuration
        if config is None or type(config) is not ConfigManager:
            # New configuration
            self._procedure_config: ConfigManager = ConfigManager()

            # If dict is provided then update using that
            if config is not None:
                self._procedure_config.update(config)
        else:
            self._procedure_config = config

        # Additional hardware
        self._procedure_hardware = hardware or []

        # Metadata tied to stage
        self._procedure_metadata = metadata or {}

        # Stage storage
        self._procedure_stages: typing.List[BaseStage] = stages or []
        self._procedure_stages_lock = threading.RLock()

        # Stage index, next, and advance trigger
        self._procedure_stage_current = None
        self._procedure_stage_next = None
        self._procedure_stage_advance = False

        # Active hardware
        self._procedure_hardware_managers: typing.Dict[str, HardwareManager] = {}

    def __del__(self):
        current_state = self.get_state()

        if current_state != ProcedureState.SETUP:
            # Try and stop running or validated procedure
            ProcedureTransition.STOP.apply(self)

    def get_uid(self) -> str:
        return self._procedure_uid

    # Stage management
    def add_stage(self, stage_class: typing.Type[TYPE_STAGE], index: typing.Optional[int] = None,
                  **stage_kwargs) -> typing.NoReturn:
        if self.get_state().is_valid():
            raise ProcedureConfigurationError('Procedure is read-only once validated')

        # Create stage
        stage_kwargs['config'] = self._procedure_config

        stage = stage_class(**stage_kwargs)

        with self._procedure_stages_lock:
            if index is None:
                self._procedure_stages.append(stage)
            else:
                self._procedure_stages.insert(index, stage)**stage_kwargs

    def add_hardware(self, identifier: str) -> typing.NoReturn:
        if self.get_state().is_valid():
            raise ProcedureConfigurationError('Procedure is read-only once validated')

        if identifier not in self._procedure_hardware:
            self._procedure_hardware.append(identifier)

    def remove_stage(self, index: int) -> typing.NoReturn:
        if self.get_state().is_valid():
            raise ProcedureConfigurationError('Procedure is read-only once validated')

        with self._procedure_stages_lock:
            if index < 0 or index >= len(self._procedure_stages):
                raise ProcedureConfigurationError(f"Stage index {index} is outside valid range")

            self._procedure_stages.pop(index)

    def remove_hardware(self, identifier: str) -> typing.NoReturn:
        if self.get_state().is_valid():
            raise ProcedureConfigurationError('Procedure is read-only once validated')

        if identifier in self._procedure_hardware:
            self._procedure_hardware.remove(identifier)

    def get_stage_count(self) -> int:
        with self._procedure_stages_lock:
            return len(self._procedure_stages)

    def get_procedure_summary(self) -> typing.Dict[str, typing.Any]:
        with self._procedure_stages_lock:
            return {
                'uid': self._procedure_uid,
                'hardware': list(self.get_procedure_hardware()),
                'metadata': self._procedure_metadata,
                'config': self._procedure_config.dump(),
                'stage_current': self._procedure_stage_current,
                'stage_next': self._procedure_stage_next
            }

    def get_stages_summary(self, in_seconds: bool = False):
        stage_summary = []
        stage_index = 0

        with self._procedure_stages_lock:
            for stage in self._procedure_stages:
                stage_duration = stage.get_stage_duration()

                if stage_duration is None:
                    stage_duration = 'Unknown'
                elif stage_duration.total_seconds() == 0:
                    stage_duration = 'Instant'
                elif in_seconds:
                    stage_duration = stage_duration.total_seconds()

                stage_remaining = stage.get_stage_remaining()

                if stage_remaining is None:
                    stage_remaining = 'Unknown'
                elif stage_remaining.total_seconds() == 0:
                    stage_remaining = 'Instant'
                elif in_seconds:
                    stage_remaining = stage_remaining.total_seconds()

                stage_summary.append({
                    'index': stage_index,
                    'class': stage.__class__.__name__,
                    'duration': stage_duration,
                    'duration_remaining': stage_remaining,
                    'summary': stage.get_stage_summary()
                })

                stage_index += 1

        return stage_summary

    def get_procedure_hardware(self) -> typing.Set[str]:
        # Get all required hardware
        hardware_identifier_list = self._procedure_hardware.copy()

        for stage in self._procedure_stages:
            hardware_identifier_list.extend(stage.get_stage_hardware())

        return set(hardware_identifier_list)

    # Procedure metadata
    def get_procedure_duration(self) -> timedelta:
        """ Estimate procedure duration from sum of all stage durations.

        :return: timedelta
        """
        durations = [x.get_stage_duration() for x in self._procedure_stages]

        return timedelta(seconds=sum((t.total_seconds() for t in durations if t is not None)))

    # Stage management
    def get_stage_current(self) -> typing.Optional[BaseStage]:
        """ Get the current stage.

        :return: Stage
        """
        if not self.get_state().is_valid():
            return None

        with self._procedure_stages_lock:
            if self._procedure_stage_current is not None:
                return self._procedure_stages[self._procedure_stage_current]
            else:
                return None

    def get_stage_next(self) -> typing.Optional[BaseStage]:
        """ Get the next stage.

        :return: Stage
        """
        if not self.get_state().is_valid():
            return None

        with self._procedure_stages_lock:
            if self._procedure_stage_next is not None:
                return self._procedure_stages[self._procedure_stage_next]
            else:
                return None

    # Event handling
    def _procedure_validate(self, _: transitions.EventData):
        if len(self._procedure_stages) == 0:
            raise ProcedureConfigurationError('Procedure is empty')

        # Validate stages
        for stage in self._procedure_stages:
            stage.stage_validate()

        # Generate list of active hardware
        self._procedure_hardware_managers = {}

        # Get handles to required hardware
        for hardware_identifier in self.get_procedure_hardware():
            self._procedure_hardware_managers[hardware_identifier] = HardwareManager.get_instance(hardware_identifier)

        # Reset index to beginning
        self._procedure_stage_current = 0
        self._procedure_stage_advance = False

        if len(self._procedure_stages) > 1:
            self._procedure_stage_next = 1
        else:
            self._procedure_stage_next = None

        # Setup metadata
        Measurement.push_metadata()

        Measurement.add_tags({
            'procedure_uid': self._procedure_uid,
            'procedure_state': 'ready'
        })

        Measurement.add_tags(self._procedure_metadata)

        Measurement.add_tag('procedure_state', 'ready')

    def _procedure_start(self, _: transitions.EventData):
        # Connect and configure required hardware
        for hardware_identifier, hardware_manager in self._procedure_hardware_managers.items():
            self.get_logger().debug(f"Setting up {hardware_identifier}")

            # Check current state
            hardware_state = hardware_manager.get_state()

            if hardware_state.is_error():
                raise ProcedureRuntimeError(f"Hardware {hardware_identifier} is in an error state, cannot start")

            if hardware_state == HardwareState.DISCONNECTED:
                hardware_manager.queue_transition(HardwareTransition.CONNECT, block=False, raise_exception=False)

            if hardware_state == HardwareState.DISCONNECTED or hardware_state == HardwareState.CONNECTED:
                hardware_manager.queue_transition(HardwareTransition.CONFIGURE, block=False, raise_exception=False)

            if hardware_state == HardwareState.DISCONNECTED or hardware_state == HardwareState.CONNECTED or \
                    hardware_state == HardwareState.CONFIGURE:
                hardware_manager.queue_transition(HardwareTransition.START, block=False, raise_exception=False)

        # Add procedure metadata
        Measurement.add_tags({
            'procedure_time': time.strftime(FORMAT_TIMESTAMP),
            'procedure_timestamp': time.time(),
            'procedure_state': 'setup'
        })

        # Wait for connection and configuration to complete
        for hardware_identifier, hardware_manager in self._procedure_hardware_managers.items():
            try:
                hardware_manager.get_error()
            except Exception as exc:
                # If hardware reported error during transition then go to error state
                ProcedureTransition.ERROR.apply(self)

                raise ProcedureRuntimeError(f"Error occurred while starting {hardware_identifier}") from exc

            if hardware_manager.get_state() != HardwareState.RUNNING:
                # If all hardware not running then transition to error state
                ProcedureTransition.ERROR.apply(self)

                raise ProcedureRuntimeError(f"{hardware_identifier} failed to start")

            self.get_logger().debug(f"{hardware_identifier} ready")

        # Notify for start
        completion_datetime = datetime.now() + self.get_procedure_duration()

        self.get_logger().info(f"Starting procedure: {self._procedure_uid}, estimated completion: "
                               f"{completion_datetime.strftime(FORMAT_TIMESTAMP)}", event=True, notify=True)

        Measurement.add_tags({
            'procedure_stage_index': 0,
            'procedure_state': 'running'
        })

        Measurement.add_dynamic_field('time_delta_procedure', dynamic_field_time_delta(datetime.now()))

        # Enter first stage
        initial_stage = self._procedure_stages[self._procedure_stage_current]
        initial_stage.stage_enter()

    def _procedure_pause(self, _: transitions.EventData):
        current_stage = self._procedure_stages[self._procedure_stage_current]
        current_stage.stage_pause()

        # Indicate procedure paused in metadata
        Measurement.add_tag('procedure_state', 'paused')

    def _procedure_resume(self, _: transitions.EventData):
        current_stage = self._procedure_stages[self._procedure_stage_current]
        current_stage.stage_resume()

        Measurement.add_tag('procedure_state', 'running')

    def _procedure_stop(self, _: transitions.EventData):
        # Stop measurements and cleanup configured hardware
        for hardware_manager in self._procedure_hardware_managers.values():
            hardware_manager.force_disconnect()

        self._procedure_hardware_managers = {}

        # Restore metadata
        Measurement.pop_metadata()

        self.get_logger().info(f"Procedure {self._procedure_uid} stopped", event=True, notify=True)

    def _stage_next(self, _: transitions.EventData):
        # Queue next stage
        self.get_logger().info('Jump to next stage', event=True)

        self._procedure_stage_next = self._procedure_stage_current + 1

        if self._procedure_stage_next >= len(self._procedure_stages):
            self._procedure_stage_next = None

        self._procedure_stage_advance = True

    def _stage_previous(self, _: transitions.EventData):
        # Queue previous stage
        self.get_logger().info('Jump to previous stage', event=True)

        self._procedure_stage_next = self._procedure_stage_current - 1

        if self._procedure_stage_next < 0:
            self._procedure_stage_next = 0

        self._procedure_stage_advance = True

    def _stage_repeat(self, _: transitions.EventData):
        # Queue current stage
        self.get_logger().info('Repeat current stage', event=True)

        self._procedure_stage_next = self._procedure_stage_current

        self._procedure_stage_advance = True

    def _stage_finish(self, _: transitions.EventData):
        # Queue completion after current stage
        self.get_logger().info('Complete current stage then finish', event=True)

        self._procedure_stage_next = None

    def _stage_goto(self, event: transitions.EventData):
        self._procedure_stage_next = int(event.args[0])

        self.get_logger().info(f'Jump to stage {self._procedure_stage_next}', event=True)

        if self._procedure_stage_next >= len(self._procedure_stages):
            self._procedure_stage_next = len(self._procedure_stages) - 1

        if self._procedure_stage_next < 0:
            self._procedure_stage_next = 0

        self._procedure_stage_advance = True

    def _handle_error(self, _: transitions.EventData):
        # Stop any active hardware
        for hardware_manager in self._procedure_hardware_managers.values():
            hardware_manager.force_disconnect()

        self._procedure_hardware_managers = {}

    # Import/export
    @classmethod
    def procedure_import(cls, data: typing.Union[str, typing.Dict[str, typing.Any]]):
        """ Generate Procedure from previously exported dict.

        :param data:
        :return:
        """
        if type(data) is str:
            data = yaml.load(data, YAMLProcedureLoader)

        target_version = data.pop('version')

        if target_version not in cls._EXPORT_COMPATIBILITY:
            raise ProcedureConfigurationError(f"Data structure could not be imported, version {target_version} not in "
                                              f"compatibility list ({', '.join(map(str, cls._EXPORT_COMPATIBILITY))})")

        target_class = data.pop('class')

        if target_class != cls.__name__:
            raise ProcedureConfigurationError(f"Procedure class {target_class} does not match expected class "
                                              f"{cls.__name__}")

        procedure_config = ConfigManager()
        procedure_config.update(data.pop('config'))
        target_stages = data.pop('stages')
        procedure_stages = []

        # Instantiate stages from data
        for stage_data in target_stages:
            procedure_stages.append(BaseStage.stage_import(procedure_config, stage_data))

        # Create procedure class
        data['stages'] = procedure_stages

        procedure = cls(**data)

        return procedure

    def procedure_export(self, as_str: bool = False) -> typing.Dict[str, typing.Any]:
        """ Export Procedure to a dict.

        :return: dict
        """
        procedure = {
            'class': self.__class__.__name__,
            'version': self._EXPORT_VERSION,
            'uid': self._procedure_uid,
            'config': self._procedure_config.dump(),
            'stages': []
        }

        if len(self._procedure_metadata) > 0:
            procedure['metadata'] = self._procedure_metadata

        if len(self._procedure_hardware) > 0:
            procedure['hardware'] = self._procedure_hardware

        # Export stages
        with self._procedure_stages_lock:
            for stage in self._procedure_stages:
                procedure['stages'].append(stage.stage_export())

        if as_str:
            return yaml.dump(procedure)
        else:
            return procedure

    def _thread_manager(self) -> typing.NoReturn:
        entry_time = time.time()

        with self._procedure_stages_lock:
            try:
                # Apply pending state transitions
                (_, current_state) = self._process_transition()

                # Handle running state
                if current_state == ProcedureState.RUNNING:
                    if self._procedure_stage_current is None:
                        pass

                    # Get current stage
                    current_stage = self._procedure_stages[self._procedure_stage_current]

                    # Run and check for completion
                    self._procedure_stage_advance |= not current_stage.stage_run()

                    if self._procedure_stage_advance:
                        # Exit current stage
                        current_stage.stage_exit()

                        # Check for completion
                        if self._procedure_stage_next is None:
                            ProcedureTransition.STOP.apply(self)
                        else:
                            self._procedure_stage_current = self._procedure_stage_next

                            # Update procedure metadata
                            Measurement.add_tag('procedure_stage_index', self._procedure_stage_current)

                            # Enter next stage
                            current_stage = self.get_stage_current()
                            current_stage.stage_enter()

                            # Update next stage
                            self._procedure_stage_next += 1

                            if self._procedure_stage_next >= len(self._procedure_stages):
                                self._procedure_stage_next = None

                        self._procedure_stage_advance = False
            except (ProcedureConfigurationError, ProcedureRuntimeError):
                self.get_logger().exception('Unhandled error in procedure')

                # Transition to error state
                ProcedureTransition.ERROR.apply(self)

        # Limit run rate
        delta_time = time.time() - entry_time

        if delta_time < self._MINIMUM_RUN_PERIOD:
            time.sleep(self._MINIMUM_RUN_PERIOD - delta_time)