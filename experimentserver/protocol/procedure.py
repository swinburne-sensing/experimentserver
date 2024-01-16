import threading
import time
import typing
from datetime import timedelta

import transitions
import yaml
from experimentlib.data import unit
from experimentlib.util.constant import FORMAT_TIMESTAMP_CONSOLE
from experimentlib.util.iterate import flatten_list
from experimentlib.util.generate import hex_str
from experimentlib.util.time import now

from experimentserver import ApplicationException
from experimentserver.protocol.control import ProcedureState, ProcedureTransition
from .file import YAMLProcedureLoader, HEADER
from .stage import BaseStage, TYPE_STAGE
from experimentserver.config import ConfigManager, ConfigNode
from experimentserver.hardware.control import HardwareState, HardwareTransition
from experimentserver.hardware.manager import HardwareManager
from experimentserver.measurement import T_TAG_MAP, Measurement, dynamic_field_time_delta
from experimentserver.util.state import ManagedStateMachine


class ProcedureLoadError(ApplicationException):
    pass


class ProcedureRuntimeError(ApplicationException):
    pass


class Procedure(ManagedStateMachine):
    """ Procedures represent experimental protocols. """

    # Limit the maximum repetition rate of the management thread when no measurements are made
    _MINIMUM_RUN_PERIOD = 0.1

    # Export version indicator
    _EXPORT_VERSION = 2
    _EXPORT_COMPATIBILITY = 2,

    def __init__(self, stages: typing.List[BaseStage], metadata: T_TAG_MAP, uid: typing.Optional[str] = None,
                 config: typing.Union[None, ConfigManager, typing.Dict[str, typing.Any]] = None,
                 hardware: typing.Optional[typing.Sequence[str]] = None):
        """ Create new Procedure instance.

        :param stages:
        :param metadata:
        :param uid:
        :param config:
        :param hardware:
        """
        # Procedure ID
        self._procedure_uid = uid or hex_str()

        ManagedStateMachine.__init__(self, self._procedure_uid, self, ProcedureState, ProcedureTransition,
                                     ProcedureState.SETUP)

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

        # Metadata tied to procedure
        if isinstance(metadata, ConfigNode):
            metadata = dict(metadata)

        self._procedure_metadata = {k: v for k, v in metadata.items()}

        if 'experiment' not in self._procedure_metadata:
            raise ProcedureLoadError('Missing \"experiment\" value in metadata')

        if 'sample' not in self._procedure_metadata:
            raise ProcedureLoadError('Missing \"sample\" value in metadata')

        try:
            if 'led_current' in self._procedure_metadata:
                self._procedure_metadata['led_current'] = unit.parse(self._procedure_metadata['led_current'],
                                                                     unit.registry.mA)

            if 'led_optical_power' in self._procedure_metadata:
                self._procedure_metadata['led_optical_power'] = unit.parse(
                    self._procedure_metadata['led_optical_power'], unit.registry.uW)

            if 'led_wavelength' in self._procedure_metadata:
                self._procedure_metadata['led_wavelength'] = unit.parse(self._procedure_metadata['led_wavelength'],
                                                                        unit.registry.nm)
        except unit.QuantityParseError as exc:
            raise ProcedureLoadError('Error while converting value in metadata') from exc

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
            raise ProcedureLoadError('Procedure is read-only once validated')

        # Create stage
        stage_kwargs['config'] = self._procedure_config

        stage = stage_class(**stage_kwargs)

        with self._procedure_stages_lock:
            if index is None:
                self._procedure_stages.append(stage)
            else:
                self._procedure_stages.insert(index, stage)

    def add_hardware(self, identifier: str) -> typing.NoReturn:
        if self.get_state().is_valid():
            raise ProcedureLoadError('Procedure is read-only once validated')

        if identifier not in self._procedure_hardware:
            self._procedure_hardware.append(identifier)

    def remove_stage(self, index: int) -> typing.NoReturn:
        if self.get_state().is_valid():
            raise ProcedureLoadError('Procedure is read-only once validated')

        with self._procedure_stages_lock:
            if index < 0 or index >= len(self._procedure_stages):
                raise ProcedureLoadError(f"Stage index {index} is outside valid range")

            self._procedure_stages.pop(index)

    def remove_hardware(self, identifier: str) -> typing.NoReturn:
        if self.get_state().is_valid():
            raise ProcedureLoadError('Procedure is read-only once validated')

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
                'metadata': {k: str(v) for k, v in self._procedure_metadata.items()},
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
            raise ProcedureLoadError('Procedure is empty')

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
        with Measurement.metadata_global_lock:
            Measurement.push_global_metadata()

            Measurement.add_global_tags({
                'procedure_uid': self._procedure_uid,
                'procedure_state': 'ready'
            })

            Measurement.add_global_tags(self._procedure_metadata)

            Measurement.add_global_tag('procedure_state', 'ready')

    def _procedure_start(self, _: transitions.EventData):
        # Connect required hardware
        for hardware_identifier, hardware_manager in self._procedure_hardware_managers.items():
            self.logger().info(f"Connecting {hardware_identifier}")

            # Check current state
            hardware_state = hardware_manager.get_state()

            if hardware_state.is_error():
                raise ProcedureRuntimeError(f"Hardware {hardware_identifier} is in an error state, cannot start "
                                            f"(during connect)")

            if hardware_state == HardwareState.DISCONNECTED:
                hardware_manager.queue_transition(HardwareTransition.CONNECT, block=False, raise_exception=False)

        # Configure
        for hardware_identifier, hardware_manager in self._procedure_hardware_managers.items():
            self.logger().info(f"Configuring {hardware_identifier}")

            # Check current state
            hardware_state = hardware_manager.get_state()

            if hardware_state.is_error():
                raise ProcedureRuntimeError(f"Hardware {hardware_identifier} is in an error state, cannot start "
                                            f"(during configure)")

            if hardware_state == HardwareState.CONNECTED:
                hardware_manager.queue_transition(HardwareTransition.CONFIGURE, block=False, raise_exception=False)

        for hardware_identifier, hardware_manager in self._procedure_hardware_managers.items():
            self.logger().info(f"Starting {hardware_identifier}")

            # Check current state
            hardware_state = hardware_manager.get_state()

            if hardware_state.is_error():
                raise ProcedureRuntimeError(f"Hardware {hardware_identifier} is in an error state, cannot start "
                                            f"(during start)")

            if hardware_state == HardwareState.CONFIGURED:
                hardware_manager.queue_transition(HardwareTransition.START, block=False, raise_exception=False)

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

            self.logger().debug(f"{hardware_identifier} ready")

        # Add procedure metadata
        with Measurement.metadata_global_lock:
            Measurement.add_global_tags({
                'procedure_time': now(),
                'procedure_stage_index': 0,
                'procedure_state': 'running'
            })

            # Notify for start
            completion_datetime = now() + self.get_procedure_duration()

            self.logger().info(f"Starting procedure: {self._procedure_uid}, estimated completion: "
                               f"{completion_datetime.strftime(FORMAT_TIMESTAMP_CONSOLE)}", event=True, notify=True)

            Measurement.add_global_dynamic_field('time_delta_procedure', dynamic_field_time_delta(now()))

            # Enter first stage
            initial_stage = self._procedure_stages[self._procedure_stage_current]
            initial_stage.stage_enter()

    def _procedure_pause(self, _: transitions.EventData):
        with Measurement.metadata_global_lock:
            current_stage = self._procedure_stages[self._procedure_stage_current]
            current_stage.stage_pause()

            # Indicate procedure paused in metadata
            Measurement.add_global_tag('procedure_state', 'paused')

        self.logger().info(f"Procedure {self._procedure_uid} paused", event=True)

    def _procedure_resume(self, _: transitions.EventData):
        with Measurement.metadata_global_lock:
            current_stage = self._procedure_stages[self._procedure_stage_current]
            current_stage.stage_resume()

            Measurement.add_global_tag('procedure_state', 'running')

        self.logger().info(f"Procedure {self._procedure_uid} resumed", event=True)

    def _procedure_stop(self, _: transitions.EventData):
        with Measurement.metadata_global_lock:
            # Stop measurements and cleanup configured hardware
            for hardware_manager in self._procedure_hardware_managers.values():
                hardware_manager.force_disconnect()

            self._procedure_hardware_managers = {}

            # Restore metadata
            Measurement.flush_global_metadata()

        self.logger().info(f"Procedure {self._procedure_uid} stopped", event=True, notify=True)

    def _stage_next(self, _: transitions.EventData):
        # Queue next stage
        self.logger().info('Skip to next stage', event=True)

        # self._procedure_stage_next = self._procedure_stage_current + 1

        # if self._procedure_stage_next >= len(self._procedure_stages):
        #     self._procedure_stage_next = None

        self._procedure_stage_advance = True

    def _stage_previous(self, _: transitions.EventData):
        # Queue previous stage
        self.logger().info('Jump to previous stage', event=True)

        self._procedure_stage_next = self._procedure_stage_current - 1

        if self._procedure_stage_next < 0:
            self._procedure_stage_next = 0

        self._procedure_stage_advance = True

    def _stage_repeat(self, _: transitions.EventData):
        # Queue current stage
        self.logger().info('Repeat current stage', event=True)

        self._procedure_stage_next = self._procedure_stage_current

        self._procedure_stage_advance = True

    def _stage_finish(self, _: transitions.EventData):
        # Queue completion after current stage
        self.logger().info('Complete current stage then finish', event=True)

        self._procedure_stage_next = None

    def _stage_goto(self, event: transitions.EventData):
        self._procedure_stage_next = int(event.args[0])

        self.logger().info(f'Queue next stage {self._procedure_stage_next}', event=True)

        if self._procedure_stage_next >= len(self._procedure_stages):
            self._procedure_stage_next = len(self._procedure_stages) - 1

        if self._procedure_stage_next < 0:
            self._procedure_stage_next = 0

        # self._procedure_stage_advance = True

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
            raise ProcedureLoadError(f"Data structure could not be imported, precedure version "
                                     f"{target_version} not in compatibility list "
                                     f"({', '.join(map(str, cls._EXPORT_COMPATIBILITY))})")

        target_class = data.pop('class', 'Procedure')

        if target_class != cls.__name__:
            raise ProcedureLoadError(f"Procedure class {target_class} does not match expected class "
                                     f"{cls.__name__}")

        procedure_config = ConfigManager()

        if 'config' in data:
            procedure_config.update(data.pop('config'))

        target_stages = data.pop('stages')
        procedure_stages = []

        # Instantiate stages from data
        for stage_data in flatten_list(target_stages):
            procedure_stages.append(BaseStage.stage_import(procedure_config, stage_data))

        # Create procedure class
        data['stages'] = procedure_stages

        procedure = cls(**data)

        return procedure

    def procedure_export(self, include_header: bool = True) -> typing.Dict[str, typing.Any]:
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

        if include_header:
            return HEADER + yaml.dump(procedure)
        else:
            return yaml.dump(procedure)

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
                            Measurement.add_global_tag('procedure_stage_index', self._procedure_stage_current)

                            # Enter next stage
                            current_stage = self.get_stage_current()
                            current_stage.stage_enter()

                            # Update next stage
                            self._procedure_stage_next += 1

                            if self._procedure_stage_next >= len(self._procedure_stages):
                                self._procedure_stage_next = None

                        self._procedure_stage_advance = False
            except (ProcedureLoadError, ProcedureRuntimeError):
                self.logger().exception('Unhandled error in procedure')

                # Transition to error state
                ProcedureTransition.ERROR.apply(self)

        # Limit run rate
        delta_time = time.time() - entry_time

        if delta_time < self._MINIMUM_RUN_PERIOD:
            time.sleep(self._MINIMUM_RUN_PERIOD - delta_time)
