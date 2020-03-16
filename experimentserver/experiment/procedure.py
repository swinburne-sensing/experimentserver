import json
import time
import threading
import typing
import uuid
from datetime import datetime, timedelta

import transitions

from . import BaseStage, ProcedureError
from .control import ProcedureState, ProcedureTransition
from experimentserver.data import Measurement
from experimentserver.data.measurement import dynamic_field_time_delta
from experimentserver.hardware import HardwareManager, HardwareState, HardwareTransition
from experimentserver.util.state import ManagedStateMachine


class Procedure(ManagedStateMachine):
    # Limit the maximum repetition rate of the management thread when no measurements are made
    _MINIMUM_RUN_PERIOD = 0.1

    def __init__(self):
        ManagedStateMachine.__init__(self, None, self, ProcedureState, ProcedureTransition, ProcedureState.SETUP)

        # Track hardware in use
        self._hardware_active: typing.Dict[str, HardwareManager] = {}

        # Stage storage
        self._stages_lock = threading.RLock()
        self._stages: typing.List[BaseStage] = []

        # Stage state
        self._stage_index = None
        self._stage_index_next = None
        self._stage_index_trigger = False

    def add_hardware(self, identifier: str):
        if self.get_state() != ProcedureState.SETUP:
            raise ProcedureError('Procedure already validated, stop to make changes')

        self._hardware_active[identifier] = HardwareManager.get_instance(identifier)

        self.get_logger().debug(f"Added hardware {identifier}")

    def remove_hardware(self, identifier: str):
        if self.get_state() != ProcedureState.SETUP:
            raise ProcedureError('Procedure already validated, stop to make changes')

        if identifier not in self._hardware_active:
            raise ProcedureError(f"Hardware {identifier} not in the list of active hardware")

        self._hardware_active.pop(identifier)

        self.get_logger().debug(f"Removed hardware {identifier}")

    def get_active_hardware(self):
        return list(self._hardware_active.keys())

    def add_stage(self, stage: BaseStage, index: typing.Optional[int] = None) -> typing.NoReturn:
        if self.get_state() != ProcedureState.SETUP:
            raise ProcedureError('Procedure already validated, stop to make changes')

        with self._stages_lock:
            if index is None:
                self._stages.append(stage)
            else:
                self._stages.insert(index, stage)

        self.get_logger().debug(f"Added stage {stage}")

    def remove_stage(self, index: int) -> typing.NoReturn:
        if self.get_state() != ProcedureState.SETUP:
            raise ProcedureError('Procedure already validated, stop to make changes')

        with self._stages_lock:
            stage = self._stages.pop(index)

        self.get_logger().debug(f"Removed stage {stage} at index {index}")

    def get_stage(self, index: typing.Optional[int] = None) -> typing.Optional[BaseStage]:
        with self._stages_lock:
            if index is None and self._stage_index is None:
                return None

            return self._stages[index or self._stage_index]

    def get_stages(self):
        with self._stages_lock:
            return self._stages.copy()

    def export_stages(self, valid_only: bool = False) -> typing.List[typing.Dict[str, typing.Any]]:
        """

        :return: str
        """
        with self._stages_lock:
            return [x.export_stage() for x in self._stages]

    def import_stages(self, data: typing.List[typing.Dict[str, typing.Any]]) -> typing.NoReturn:
        """

        :param data:
        """
        if self.get_state() != ProcedureState.SETUP:
            raise ProcedureError('Procedure already validated, stop to make changes')

        # Parse JSON
        data = json.loads(data)

        for stage in data:
            stage_class = stage.pop('class')

        with self._stages_lock:
            self._stages.clear()

    def get_duration(self) -> timedelta:
        return timedelta(seconds=sum([x.get_duration().total_seconds() for x in self._stages]))

    def get_status(self) -> str:
        with self._stages_lock:
            if self.get_state().is_running() and self.get_stage() is not None:
                return f"Stage {self._stage_index}: {self.get_stage().get_status()}"
            else:
                return f"Estimated runtime: {self.get_duration()!s}, completion at " \
                       f"{(datetime.now() + self.get_duration()).strftime('%Y-%m-%d %H:%M:%S')}"

    # Event handling
    def _procedure_validate(self, _: transitions.EventData):
        if len(self._stages) == 0:
            raise ProcedureError('Procedure contains no stages')

        # Check for valid stages
        for stage in self._stages:
            stage.validate()

        # Reset index to beginning
        self._stage_index = 0

        if len(self._stages) > 1:
            self._stage_index_next = 1
        else:
            self._stage_index_next = None

    def _procedure_start(self, _: transitions.EventData):
        # Connect and configure active hardware
        for hardware_identifier, hardware_manager in self._hardware_active.items():
            self.get_logger().debug(f"Setting up {hardware_identifier}")

            hardware_state = hardware_manager.get_state()

            if hardware_state == HardwareState.DISCONNECTED:
                hardware_manager.queue_transition(HardwareTransition.CONNECT, block=False, raise_exception=False)

            if hardware_state == HardwareState.DISCONNECTED or hardware_state == HardwareState.CONNECTED:
                hardware_manager.queue_transition(HardwareTransition.CONFIGURE, block=False, raise_exception=False)

            if hardware_state.is_error():
                raise ProcedureError(f"Hardware {hardware_identifier} is in an error state, cannot start")

        # Wait for connection and configuration
        for hardware_identifier, hardware_manager in self._hardware_active.items():
            try:
                hardware_manager.get_error()
            except Exception as exc:
                # If hardware reported error during transition then go to error state
                ProcedureTransition.ERROR.apply(self)

                raise ProcedureError(f"Hardware {hardware_identifier} failed to start") from exc

            if hardware_manager.get_state() != HardwareState.CONFIGURED:
                # If all hardware not running then transition to error state
                ProcedureTransition.ERROR.apply(self)

            self.get_logger().debug(f"{hardware_identifier} ready")

        # Create app_metadata
        self._uuid = uuid.uuid4()
        self.get_logger().info(f"Starting procedure: {self._uuid}", event=True, notify=True)

        # Push app_metadata
        Measurement.push_metadata()

        Measurement.add_dynamic_field('time_delta_procedure', dynamic_field_time_delta(datetime.now()))

        # Add procedure app_metadata
        Measurement.add_tag('procedure_uuid', self._uuid)
        Measurement.add_tag('procedure_time', time.strftime('%Y-%m-%d %H:%M:%S'))
        Measurement.add_tag('procedure_timestamp', time.time())
        Measurement.add_tag('procedure_state', 'running')
        Measurement.add_tag('procedure_stage_index', 0)

        # Start hardware
        for hardware_manager in self._hardware_active.values():
            hardware_manager.queue_transition(HardwareTransition.START)

        # Enter first stage
        initial_stage = self._stages[self._stage_index]
        initial_stage.start()

    def _procedure_pause(self, _: transitions.EventData):
        current_stage = self.get_stage()
        current_stage.pause()

        # Indicate procedure paused in app_metadata
        Measurement.add_tag('procedure_state', 'paused')

    def _procedure_resume(self, _: transitions.EventData):
        current_stage = self.get_stage()
        current_stage.resume()

        Measurement.add_tag('procedure_state', 'running')

    def _procedure_stop(self, _: transitions.EventData):
        # Stop measurements and cleanup configured hardware
        for hardware_manager in self._hardware_active.values():
            hardware_manager.queue_transition(HardwareTransition.STOP)
            hardware_manager.queue_transition(HardwareTransition.CLEANUP)

        # Restore app_metadata
        Measurement.pop_metadata()

        self.get_logger().info(f"Procedure {self._uuid} completed", event=True, notify=True)

    def _stage_next(self, _: transitions.EventData):
        # Queue next stage
        self.get_logger().info('Jump to next stage', event=True)

        self._stage_index_next = self._stage_index + 1

        if self._stage_index_next >= len(self._stages):
            self._stage_index_next = None

        self._stage_index_trigger = True

    def _stage_previous(self, _: transitions.EventData):
        # Queue previous stage
        self.get_logger().info('Jump to previous stage', event=True)

        self._stage_index_next = self._stage_index - 1

        if self._stage_index_next < 0:
            self._stage_index_next = 0

        self._stage_index_trigger = True

    def _stage_repeat(self, _: transitions.EventData):
        # Queue current stage
        self.get_logger().info('Repeat current stage', event=True)

        self._stage_index_next = self._stage_index

        self._stage_index_trigger = True

    def _stage_finish(self, _: transitions.EventData):
        # Queue completion after current stage
        self.get_logger().info('Complete current stage then finish', event=True)

        self._stage_index_next = None

    def _stage_goto(self, event: transitions.EventData):
        self._stage_index_next = int(event.args[0])

        self.get_logger().info(f'Jump to stage {self._stage_index_next}', event=True)

        if self._stage_index_next >= len(self._stages):
            self._stage_index_next = len(self._stages) - 1

        if self._stage_index_next < 0:
            self._stage_index_next = 0

        self._stage_index_trigger = True

    def _handle_error(self, event: transitions.EventData):
        pass

    def _thread_manager(self) -> typing.NoReturn:
        start_time = time.time()

        with self._stages_lock:
            try:
                # Apply pending state transitions
                (_, current_state) = self._process_transition()

                # Handle stage
                if current_state == ProcedureState.RUNNING:
                    # Get current stage
                    current_stage = self._stages[self._stage_index]

                    # Check for completion of current stage
                    self._stage_index_trigger |= current_stage.tick()

                    if self._stage_index_trigger:
                        # Exit current stage
                        current_stage.stop()

                        # Check for completion
                        if self._stage_index_next is None:
                            ProcedureTransition.STOP.apply(self)
                        else:
                            self._stage_index = self._stage_index_next

                            # Enter next stage
                            current_stage = self._stages[self._stage_index]
                            current_stage.start()

                            # Update app_metadata
                            Measurement.add_tag('procedure_stage_index', self._stage_index)

                            # Update next stage
                            self._stage_index_next += 1

                            if self._stage_index_next >= len(self._stages):
                                self._stage_index_next = None

                        self._stage_index_trigger = False
            except ProcedureError:
                self.get_logger().exception('Unhandled error in procedure')

                # Transition to error state
                ProcedureTransition.ERROR.apply(self)

        # Limit run rate
        delta_time = time.time() - start_time

        if delta_time < self._MINIMUM_RUN_PERIOD:
            time.sleep(self._MINIMUM_RUN_PERIOD - delta_time)
