from __future__ import annotations
import enum
import queue
import threading
import time
import typing

import transitions
from wrapt import ObjectProxy

from . import Hardware, HardwareException, TYPING_PARAM
from experimentserver.util.thread import CallbackThread


class HardwareStateException(HardwareException):
    pass


class HardwareState(enum.Enum):
    IDLE = 'idle'
    READY = 'ready'
    RUNNING = 'running'
    PAUSED = 'paused'
    ERROR_READY = 'error_ready'
    ERROR_RUNTIME = 'error_runtime'


class HardwareStateTransition(enum.Enum):
    SETUP = 'setup'
    START = 'start'
    PAUSE = 'pause'
    RESUME = 'resume'
    STOP = 'stop'
    CLEANUP = 'cleanup'
    ERROR = 'error'
    RESET = 'reset'

    @staticmethod
    def get_transitions():
        return [
            {
                'trigger': HardwareStateTransition.SETUP.value,
                'source': HardwareState.IDLE.value,
                'dest': HardwareState.READY.value,
                'before': 'handle_setup'
            },
            {
                'trigger': HardwareStateTransition.START.value,
                'source': HardwareState.READY.value,
                'dest': HardwareState.RUNNING.value,
                'before': 'handle_start'
            },
            {
                'trigger': HardwareStateTransition.PAUSE.value,
                'source': HardwareState.RUNNING.value,
                'dest': HardwareState.PAUSED.value,
                'before': 'handle_pause'
            },
            {
                'trigger': HardwareStateTransition.RESUME.value,
                'source': HardwareState.PAUSED.value,
                'dest': HardwareState.RUNNING.value,
                'before': 'handle_resume'
            },
            {
                'trigger': HardwareStateTransition.STOP.value,
                'source': [HardwareState.RUNNING.value, HardwareState.PAUSED.value],
                'dest': HardwareState.READY.value,
                'before': 'handle_stop'
            },
            {
                'trigger': HardwareStateTransition.CLEANUP.value,
                'source': HardwareState.READY.value,
                'dest': HardwareState.IDLE.value,
                'before': 'handle_cleanup'
            },
            {
                'trigger': HardwareStateTransition.ERROR.value,
                'source': HardwareState.READY.value,
                'dest': HardwareState.ERROR_READY.value,
                'before': 'handle_error'
            },
            {
                'trigger': HardwareStateTransition.ERROR.value,
                'source': [HardwareState.RUNNING.value, HardwareState.PAUSED.value],
                'dest': HardwareState.ERROR_RUNTIME.value,
                'before': 'handle_error'
            },
            {
                'trigger': HardwareStateTransition.RESET.value,
                'source': HardwareState.ERROR_READY.value,
                'dest': HardwareState.READY.value,
                'before': '_handle_reset_ready'
            },
            {
                'trigger': HardwareStateTransition.RESET.value,
                'source': HardwareState.ERROR_RUNTIME.value,
                'dest': HardwareState.RUNNING.value,
                'before': '_handle_reset_runtime'
            }
        ]


# noinspection PyAbstractClass
class _HardwareStateMachine(ObjectProxy):
    """ A proxy object. """
    pass

    def _handle_reset_ready(self, event: transitions.EventData):
        # Clear error state
        try:
            self.handle_reset(event)
        except NotImplementedError:
            # Use default setup method
            self.handle_setup(event)

    def _handle_reset_runtime(self, event: transitions.EventData):
        # Clear error state
        try:
            self.handle_reset(event)
        except NotImplementedError:
            # Use default setup method
            self.handle_setup(event)

        # Process start transition
        self.handle_start(event)


class HardwareStateManager(CallbackThread):
    _MINIMUM_RUN_PERIOD = 1

    _TIMEOUT_RESET = 10

    _manager_instances: typing.List[HardwareStateManager] = []

    def __init__(self, hardware: Hardware):
        super().__init__(name=hardware.get_hardware_identifier(True), callback=self._thread_manager, run_final=True)

        # References to managed objects
        self._hardware = _HardwareStateMachine(hardware)

        # State machine
        self._machine = transitions.Machine(model=self._hardware, states=[x.value for x in HardwareState],
                                            transitions=HardwareStateTransition.get_transitions(),
                                            initial=HardwareState.IDLE.value, send_event=True)

        # State locks
        self._state_transition: typing.Optional[HardwareStateTransition] = None
        self._state_lock = threading.Condition()
        self._state_error: typing.Optional[Exception] = None

        # Parameter queue
        self._parameter_queue = queue.Queue()

        # Reset timeout
        self._reset_time = None

        # Add self to list of hardware managers
        self._manager_instances.append(self)

    @classmethod
    def get_all_managers(cls):
        return cls._manager_instances

    def get_error(self):
        with self._state_lock:
            return self._state_error

    def is_idle(self) -> bool:
        return self.get_state() == HardwareState.IDLE

    def is_error(self) -> bool:
        return self.get_state() == HardwareState.ERROR_READY or self.get_state() == HardwareState.ERROR_RUNTIME

    def get_hardware(self) -> Hardware:
        return self._hardware

    def get_state(self) -> HardwareState:
        with self._state_lock:
            return HardwareState(self._hardware.state)

    def queue_parameter(self, parameter_dict: TYPING_PARAM):
        # Validate parameters?

        # Queue parameter update
        self._parameter_queue.put(parameter_dict)

    def queue_transition(self, transition: HardwareStateTransition, wait: bool = True):
        with self._state_lock:
            # Check for pending state transition
            if transition is not HardwareStateTransition.ERROR and self._state_transition is not None:
                # Wait for completion
                self._state_lock.wait()

            # Clear any existing error
            self._state_error = None

            # Set target
            self._state_transition = transition

            # Optionally wait for completion
            if wait:
                self._state_lock.wait()

                # Validate state change?
                if self._state_error is not None:
                    raise HardwareStateException(f"Failed to process state transition {transition}") \
                        from self._state_error

    def _thread_manager(self):
        operation_flag = False

        with self._state_lock:
            # Fetch current state
            current_state = self.get_state()

            # Handle shutdown gracefully
            if self._test_stop():
                try:
                    # If currently running then stop
                    if current_state == HardwareState.RUNNING or current_state == HardwareState.PAUSED:
                        self._logger.info('Stopping running hardware')

                        self._hardware.stop()

                        # Update to new state
                        current_state = self.get_state()

                    # If stopped then cleanup
                    if current_state == HardwareState.READY:
                        self._logger.info('Cleaning up hardware')

                        self._hardware.cleanup()

                        # Update to new state
                        current_state = self.get_state()
                except Exception:
                    self._logger.exception(f"An error occurred during shutdown")
                finally:
                    if current_state != HardwareState.IDLE:
                        self._logger.error(f"Hardware could not be returned to idle state during shutdown")

                return

            if self._state_transition is not None:
                state_transition = self._state_transition

                # Clear pending state
                self._state_transition = None

                self._logger.info(f"Transition {self._hardware.state} -> {state_transition}")

                try:
                    # Clear any existing error flag
                    self._state_error = None

                    # Get transition method
                    transition_method = getattr(self._hardware, state_transition.value)

                    # Attempt to change state
                    with self._hardware.get_hardware_lock():
                        transition_method()

                    # Indicate state change occurred
                    operation_flag = True
                except transitions.core.MachineError as exc:
                    self._logger.warning(f"Unable to process state transition", exc_info=True)

                    # Cache error
                    self._state_error = exc
                except HardwareException as exc:
                    self._logger.exception(f"Hardware error occurred while processing state transition "
                                           f"{state_transition}", event=True, notify=True)

                    # Cache error
                    self._state_error = exc

                    # Set state to error if state was not idle
                    if current_state != HardwareState.IDLE:
                        self._hardware.error()
                except Exception as exc:
                    # Cache error
                    self._state_error = exc

                    # Set state to error if state was not idle
                    if current_state != HardwareState.IDLE:
                        self._hardware.error()

                    # Raise exception to thread
                    raise
                finally:
                    # Notify waiting threads
                    self._state_lock.notify_all()

                current_state = self.get_state()

                self._logger.info(f"State {current_state}")

        time_run_start = time.time()

        if current_state == HardwareState.ERROR_READY or current_state == HardwareState.ERROR_RUNTIME:
            if self._reset_time is None:
                # Set timeout for reset if not already set
                self._reset_time = time.time() + self._TIMEOUT_RESET
            elif time.time() >= self._reset_time:
                # Hardware in error state, attempt to reset after timeout
                self._logger.info(f"Attempting to reset {self._hardware.get_hardware_identifier()}", event=True)

                with self._hardware.get_hardware_lock():
                    try:
                        self._hardware.reset()
                    except (transitions.MachineError, HardwareException):
                        self._logger.warning("Exception occurred during reset", exc_info=True)

                        # Set new timeout
                        self._reset_time = time.time() + self._TIMEOUT_RESET

            # Set operation flag to enable delay before next check
            operation_flag = True
        else:
            self._reset_time = None

        if current_state != HardwareState.IDLE and current_state != HardwareState.ERROR_READY \
                and current_state != HardwareState.ERROR_RUNTIME:
            with self._hardware.get_hardware_lock():
                # Apply pending parameter changes
                while not self._parameter_queue.empty():
                    parameter_dict = self._parameter_queue.get()

                    self._hardware.set_parameter(parameter_dict)

                    operation_flag = True

                # If running then take a measurement
                if current_state == HardwareState.RUNNING:
                    operation_flag |= self._hardware.produce_measurement()

        # If no operations occurred then limit rate process is run
        if not operation_flag:
            time_run_delta = time.time() - time_run_start

            if time_run_delta < self._MINIMUM_RUN_PERIOD:
                time.sleep(self._MINIMUM_RUN_PERIOD - time_run_delta)

    def _handle_thread_exception(self, exc: Exception):
        super()._handle_thread_exception(exc)

        # Transition to error state
        try:
            self._hardware.error()
        except transitions.MachineError:
            self._logger.warning('Could not transition to error state (may be idle or already in error state)')
