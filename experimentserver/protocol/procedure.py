from __future__ import annotations
import threading
import time
import typing
from datetime import timedelta

import jinja2
import transitions
import yaml
from experimentlib.data import unit
from experimentlib.util.constant import FORMAT_TIMESTAMP_CONSOLE
from experimentlib.util.iterate import flatten_list
from experimentlib.util.generate import hex_str
from experimentlib.util.time import now

from experimentserver import ApplicationException, MultipleException
from experimentserver.protocol.control import ProcedureState, ProcedureTransition
from .file import YAMLProcedureLoader
from .stage import BaseStage, TYPE_STAGE
from experimentserver.config import ConfigManager, ConfigNode
from experimentserver.hardware.control import HardwareTransition
from experimentserver.hardware.manager import HardwareManager
from experimentserver.measurement import T_TAG_MAP, Measurement, MeasurementTarget, dynamic_field_time_delta
from experimentserver.util.lock import MonitoredLock
from experimentserver.util.state import ManagedStateMachine


class ProcedureLoadError(ApplicationException):
    pass


class ProcedureRuntimeError(ApplicationException):
    pass


class Procedure(ManagedStateMachine[ProcedureState, ProcedureTransition]):
    """ Procedures represent experimental protocols. """

    # Limit the maximum repetition rate of the management thread when no measurements are made
    _MINIMUM_RUN_PERIOD = 0.1

    # Export version indicator
    _EXPORT_VERSION = 2
    _EXPORT_COMPATIBILITY = 2,

    # Timeout to prevent deadlocks
    _STAGES_LOCK_TIMEOUT = 15

    def __init__(self, stages: typing.List[BaseStage], metadata: T_TAG_MAP, uid: typing.Optional[str] = None,
                 config: typing.Union[None, ConfigManager, typing.Dict[str, typing.Any]] = None,
                 hardware: typing.Optional[typing.Iterable[str]] = None):
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
        if isinstance(config, ConfigManager):
            self._procedure_config = config
        else:
            # New configuration
            self._procedure_config = ConfigManager()

            # If dict is provided then update using that
            if config is not None:
                self._procedure_config.update(config)

        # Additional hardware
        self._procedure_hardware = list(hardware or [])

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
        self._procedure_stages_lock = MonitoredLock(f"{self._procedure_uid}.stages", self._STAGES_LOCK_TIMEOUT)

        # Stage index, next, and advance trigger
        self._procedure_stage_current: typing.Optional[int] = None
        self._procedure_stage_next: typing.Optional[int] = None
        self._procedure_stage_advance = False

        # Active hardware
        self._procedure_hardware_managers: typing.Dict[str, HardwareManager] = {}

    def __del__(self) -> None:
        current_state = self.get_state()

        if current_state != ProcedureState.SETUP:
            # Try and stop running or validated procedure
            ProcedureTransition.STOP.apply(self)

    def get_uid(self) -> str:
        return self._procedure_uid

    # Stage management
    def add_stage(self, stage_class: typing.Type[TYPE_STAGE], index: typing.Optional[int] = None,
                  **stage_kwargs: typing.Any) -> None:
        if self.get_state().is_valid():
            raise ProcedureLoadError('Procedure is read-only once validated')

        # Create stage
        stage_kwargs['config'] = self._procedure_config

        stage = stage_class(**stage_kwargs)

        with self._procedure_stages_lock.lock():
            if index is None:
                self._procedure_stages.append(stage)
            else:
                self._procedure_stages.insert(index, stage)

    def add_hardware(self, identifier: str) -> None:
        if self.get_state().is_valid():
            raise ProcedureLoadError('Procedure is read-only once validated')

        if identifier not in self._procedure_hardware:
            self._procedure_hardware.append(identifier)

    def remove_stage(self, index: int) -> None:
        if self.get_state().is_valid():
            raise ProcedureLoadError('Procedure is read-only once validated')

        with self._procedure_stages_lock.lock():
            if index < 0 or index >= len(self._procedure_stages):
                raise ProcedureLoadError(f"Stage index {index} is outside valid range")

            self._procedure_stages.pop(index)

    def remove_hardware(self, identifier: str) -> None:
        if self.get_state().is_valid():
            raise ProcedureLoadError('Procedure is read-only once validated')

        if identifier in self._procedure_hardware:
            self._procedure_hardware.remove(identifier)

    def get_stage_count(self) -> int:
        with self._procedure_stages_lock.lock():
            return len(self._procedure_stages)

    def get_procedure_summary(self) -> typing.Dict[str, typing.Any]:
        with self._procedure_stages_lock.lock():
            return {
                'uid': self._procedure_uid,
                'hardware': list(self.get_procedure_hardware()),
                'metadata': {k: str(v) for k, v in self._procedure_metadata.items()},
                'config': self._procedure_config.dump(),
                'stage_current': self._procedure_stage_current,
                'stage_next': self._procedure_stage_next
            }

    def get_stages_summary(self, in_seconds: bool = False) -> typing.List[typing.Dict[str, typing.Union[str, float]]]:
        stage_summary = []
        stage_index = 0

        with self._procedure_stages_lock.lock():
            for stage in self._procedure_stages:
                stage_duration = stage.get_stage_duration()
                stage_duration_value: typing.Union[timedelta, str, float]

                if stage_duration is None:
                    stage_duration_value = 'Unknown'
                elif stage_duration.total_seconds() == 0:
                    stage_duration_value = 'Instant'
                elif in_seconds:
                    stage_duration_value = stage_duration.total_seconds()
                else:
                    stage_duration_value = stage_duration

                stage_remaining = stage.get_stage_remaining()
                stage_remaining_value: typing.Union[timedelta, str, float] = 0.0

                if stage_remaining is None:
                    stage_remaining_value = 'Unknown'
                elif stage_remaining.total_seconds() == 0:
                    stage_remaining_value = 'Instant'
                elif in_seconds:
                    stage_remaining_value = stage_remaining.total_seconds()
                else:
                    stage_remaining_value = stage_remaining

                stage_summary.append({
                    'index': stage_index,
                    'class': stage.__class__.__name__,
                    'duration': stage_duration_value,
                    'duration_remaining': stage_remaining_value,
                    'summary': stage.get_stage_summary()
                })

                stage_index += 1

        return stage_summary

    def get_procedure_hardware(self) -> typing.Set[str]:
        # Get all required hardware
        hardware_identifier_list = self._procedure_hardware.copy()

        with self._procedure_stages_lock.lock():
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
    def get_stage_current(self) -> typing.Tuple[typing.Optional[int], typing.Optional[BaseStage]]:
        """ Get the current stage.

        :return: Stage
        """
        if not self.get_state().is_valid():
            return None, None

        with self._procedure_stages_lock.lock():
            if self._procedure_stage_current is not None:
                return self._procedure_stage_current, self._procedure_stages[self._procedure_stage_current]
            else:
                return None, None

    def get_stage_next(self) -> typing.Tuple[typing.Optional[int], typing.Optional[BaseStage]]:
        """ Get the next stage.

        :return: Stage
        """
        if not self.get_state().is_valid():
            return None, None

        with self._procedure_stages_lock.lock():
            if self._procedure_stage_next is not None:
                return self._procedure_stage_next, self._procedure_stages[self._procedure_stage_next]
            else:
                return None, None

    # Event handling
    def _procedure_validate(self, _: transitions.EventData) -> None:
        if len(self._procedure_stages) == 0:
            raise ProcedureLoadError('Procedure is empty')

        # Validate stages
        with self._procedure_stages_lock.lock():
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
        with Measurement.metadata_global_lock.lock():
            Measurement.push_global_metadata()

            # Tell measurement targets a new group has started
            MeasurementTarget.trigger_start(
                experiment_procedure_summary=self.get_procedure_summary(),
                experiment_procedure_stages=self.get_stages_summary()
            )

            Measurement.add_global_tags({
                'procedure_uid': self._procedure_uid,
                'procedure_state': 'ready'
            })

            Measurement.add_global_tags(self._procedure_metadata)

            Measurement.add_global_tag('procedure_state', 'ready')
    
    def _hardware_error_check(self) -> None:
        errors = {}

        for identifier, manager in self._procedure_hardware_managers.items():
            exc = manager.get_error(raise_exception=False)

            if exc is not None:
                self.logger().warning(f"{identifier} reported errors")
                errors[identifier] = exc
            else:
                self.logger().debug(f"{identifier} reported no errors")
        
        if len(errors) == 1:
            ProcedureTransition.ERROR.apply(self)
            identifier, exc = next(iter(errors.items()))
            raise ProcedureRuntimeError(f"Error occurred while starting {identifier}") from exc
        elif len(errors) > 1:
            raise ProcedureRuntimeError('Multiple hardware errors occured') from \
                MultipleException('Multiple hardware errors generated during procedure transition', list(errors.values()))

    def _hardware_transition(self, transition: HardwareTransition) -> None:
        for manager in self._procedure_hardware_managers.values():
            self.logger().info(
                f"Running transition {transition!s} on {manager.get_hardware().get_hardware_identifier()}"
            )
            manager.queue_transition(transition, block=False, raise_exception=False)

    def _procedure_start(self, _: transitions.EventData) -> None:
        # Trigger connect on all hardware
        self._hardware_transition(HardwareTransition.CONNECT)
        self._hardware_error_check()

        # Trigger configure on all hardware
        self._hardware_transition(HardwareTransition.CONFIGURE)
        self._hardware_error_check()

        # Trigger start on all hardware
        self._hardware_transition(HardwareTransition.START)
        self._hardware_error_check()

        # Add procedure metadata
        with Measurement.metadata_global_lock.lock():
            Measurement.add_global_tags({
                'procedure_time': now(),
                'procedure_stage_index': 0,
                'procedure_state': 'running'
            })

            Measurement.add_global_dynamic_field('time_delta_procedure', dynamic_field_time_delta(now()))

        # Notify for start
        completion_datetime = now() + self.get_procedure_duration()

        self.logger().info(f"Starting procedure: {self._procedure_uid}, estimated completion: "
                            f"{completion_datetime.strftime(FORMAT_TIMESTAMP_CONSOLE)}", event=True, notify=True)

        # Enter first stage
        assert self._procedure_stage_current is not None
        initial_stage = self._procedure_stages[self._procedure_stage_current]
        initial_stage.stage_enter()

    def _procedure_pause(self, _: transitions.EventData) -> None:
        _, current_stage = self.get_stage_current()
        assert current_stage is not None
        current_stage.stage_pause()

        # Indicate procedure paused in metadata
        Measurement.add_global_tag('procedure_state', 'paused')

        self.logger().info(f"Procedure {self._procedure_uid} paused", event=True)

    def _procedure_resume(self, _: transitions.EventData) -> None:
        _, current_stage = self.get_stage_current()
        assert current_stage is not None
        current_stage.stage_resume()

        Measurement.add_global_tag('procedure_state', 'running')

        self.logger().info(f"Procedure {self._procedure_uid} resumed", event=True)

    def _procedure_stop(self, _: transitions.EventData) -> None:
        _, current_stage = self.get_stage_current()

        if current_stage is not None:
            current_stage.stage_exit()

        with Measurement.metadata_global_lock.lock():
            # Clear current stage
            self._procedure_stage_current = None

            # Tell measurement targets a new group has stopped
            MeasurementTarget.trigger_stop(
                procedure_summary=self.get_procedure_summary(),
                procedure_stages=self.get_stages_summary()
            )

            # Restore metadata
            Measurement.flush_global_metadata()
        
        # Stop measurements and cleanup configured hardware
        for hardware_manager in self._procedure_hardware_managers.values():
            hardware_manager.force_disconnect()

        self._procedure_hardware_managers = {}

        self.logger().info(f"Procedure {self._procedure_uid} stopped", event=True, notify=True)

    def _stage_next(self, _: transitions.EventData) -> None:
        # Queue next stage
        self.logger().info('Skip to next stage', event=True)

        with self._procedure_stages_lock.lock():
            self._procedure_stage_advance = True

    def _stage_previous(self, _: transitions.EventData) -> None:
        # Queue previous stage
        self.logger().info('Jump to previous stage', event=True)

        with self._procedure_stages_lock.lock():
            assert self._procedure_stage_current is not None
            self._procedure_stage_next = self._procedure_stage_current - 1

            if self._procedure_stage_next < 0:
                self._procedure_stage_next = 0

            self._procedure_stage_advance = True

    def _stage_repeat(self, _: transitions.EventData) -> None:
        # Queue current stage
        self.logger().info('Repeat current stage', event=True)

        with self._procedure_stages_lock.lock():
            assert self._procedure_stage_current is not None
            self._procedure_stage_next = self._procedure_stage_current

            self._procedure_stage_advance = True

    def _stage_finish(self, _: transitions.EventData) -> None:
        # Queue completion after current stage
        self.logger().info('Complete current stage then finish', event=True)

        with self._procedure_stages_lock.lock():
            self._procedure_stage_next = None

    def _stage_goto(self, event: transitions.EventData) -> None:
        stage_next = int(event.args[0])

        self.logger().info(f'Queue next stage {stage_next}', event=True)

        with self._procedure_stages_lock.lock():
            self._procedure_stage_next = stage_next

            if self._procedure_stage_next >= len(self._procedure_stages):
                self._procedure_stage_next = len(self._procedure_stages) - 1

            if self._procedure_stage_next < 0:
                self._procedure_stage_next = 0

        # self._procedure_stage_advance = True

    def _handle_error(self, _: transitions.EventData) -> None:
        # Stop any active hardware
        for hardware_manager in self._procedure_hardware_managers.values():
            hardware_manager.force_disconnect()

        self._procedure_hardware_managers = {}
    
    @staticmethod
    def render_template(template_content: str) -> str:
        template_env = jinja2.Environment(
            loader=jinja2.loaders.DictLoader({}),
            autoescape=jinja2.select_autoescape()
        )

        procedure_file_template = template_env.from_string(
            template_content,
            {
                'hex_str': hex_str
            }
        )

        return procedure_file_template.render()

    # Import/export
    @classmethod
    def procedure_import(cls, data: typing.Union[str, typing.Dict[str, typing.Any]]) -> Procedure:
        """ Generate Procedure from previously exported dict.

        :param data:
        :return:
        """
        if isinstance(data, str):
            data = typing.cast(typing.Dict[str, typing.Any], yaml.load(data, YAMLProcedureLoader))

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

    def procedure_export(self) -> typing.Dict[str, typing.Any]:
        """ Export Procedure to a dict.

        :return: dict
        """
        stages: typing.List[typing.Dict[str, typing.Any]] = []

        procedure = {
            'class': self.__class__.__name__,
            'version': self._EXPORT_VERSION,
            'uid': self._procedure_uid,
            'config': self._procedure_config.dump(),
            'stages': stages
        }

        if len(self._procedure_metadata) > 0:
            procedure['metadata'] = self._procedure_metadata

        if len(self._procedure_hardware) > 0:
            procedure['hardware'] = self._procedure_hardware

        # Export stages
        with self._procedure_stages_lock.lock():
            for stage in self._procedure_stages:
                stages.append(stage.stage_export())
        
        return procedure

    def _thread_manager(self) -> None:
        entry_time = time.time()

        try:
            # Apply pending state transitions
            (_, current_state) = self._process_transition()

            # Handle running state
            if current_state == ProcedureState.RUNNING:
                _, current_stage = self.get_stage_current()

                if current_stage is None:
                    self.logger().warning('No current stage')
                    ProcedureTransition.STOP.apply(self)
                    return

                stage_advance = not current_stage.stage_run()

                if not stage_advance:
                    with self._procedure_stages_lock.lock():
                        stage_advance |= self._procedure_stage_advance

                if stage_advance:
                    # Exit current stage and prepare next
                    current_stage.stage_exit()
                    _, next_stage = self.get_stage_next()

                    # Check for completion
                    if next_stage is None:
                        ProcedureTransition.STOP.apply(self)
                        return

                    # Enter next stage
                    with self._procedure_stages_lock.lock():
                        self._procedure_stage_current = self._procedure_stage_next

                        # Update next stage
                        if self._procedure_stage_next is not None:
                            self._procedure_stage_next += 1

                            if self._procedure_stage_next >= len(self._procedure_stages):
                                self._procedure_stage_next = None
                        
                        self._procedure_stage_advance = False

                        _, current_stage = self.get_stage_current()
                    
                    if current_stage is None:
                        ProcedureTransition.STOP.apply(self)
                        return

                    current_stage.stage_enter()

                    # Update procedure metadata
                    Measurement.add_global_tag('procedure_stage_index', self._procedure_stage_current)
        except (ProcedureLoadError, ProcedureRuntimeError):
            self.logger().exception('Unhandled error in procedure')

            # Transition to error state
            ProcedureTransition.ERROR.apply(self)

        # Limit run rate
        delta_time = time.time() - entry_time

        if delta_time < self._MINIMUM_RUN_PERIOD:
            time.sleep(self._MINIMUM_RUN_PERIOD - delta_time)
