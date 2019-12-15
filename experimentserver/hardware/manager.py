from __future__ import annotations
import enum
import queue
import threading
import time
import typing

import transitions
from wrapt import ObjectProxy

from . import Hardware, HardwareException, TYPE_PARAMETER_DICT
from experimentserver.util.thread import CallbackThread


class HardwareState(enum.Enum):
    """ Enum representing Hardware state machine states. """

    IDLE = 'idle'
    READY = 'ready'
    RUNNING = 'running'
    PAUSED = 'paused'
    ERROR_READY = 'error_ready'
    ERROR_RUNTIME = 'error_runtime'

    def is_error(self):
        return self == self.ERROR_READY or self == self.ERROR_RUNTIME

    def is_idle(self):
        return self == self.IDLE

    def is_active(self):
        return not self.is_idle() and not self.is_error()


class HardwareStateTransition(enum.Enum):
    """ Enum representing Hardware state machine transitions. """

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
                'trigger': HardwareStateTransition.STOP.value,
                'source': HardwareState.ERROR_RUNTIME,
                'dest': HardwareState.ERROR_READY
            },
            {
                'trigger': HardwareStateTransition.CLEANUP.value,
                'source': [HardwareState.READY.value, HardwareState.ERROR_READY],
                'dest': HardwareState.IDLE.value,
                'before': 'handle_cleanup'
            },
            {
                'trigger': HardwareStateTransition.CLEANUP.value,
                'source': [HardwareState.RUNNING.value, HardwareState.PAUSED.value, HardwareState.ERROR_RUNTIME],
                'dest': HardwareState.IDLE.value,
                'before': '_handle_cleanup_running'
            },
            {
                'trigger': HardwareStateTransition.ERROR.value,
                'source': HardwareState.IDLE.value,
                'dest': HardwareState.IDLE.value,
                'before': '_handle_nop'
            },
            {
                'trigger': HardwareStateTransition.ERROR.value,
                'source': HardwareState.ERROR_READY.value,
                'dest': HardwareState.ERROR_READY.value,
                'before': '_handle_nop'
            },
            {
                'trigger': HardwareStateTransition.ERROR.value,
                'source': HardwareState.ERROR_RUNTIME.value,
                'dest': HardwareState.ERROR_RUNTIME.value,
                'before': '_handle_nop'
            },
            {
                'trigger': HardwareStateTransition.ERROR.value,
                'source': HardwareState.READY.value,
                'dest': HardwareState.ERROR_READY.value,
                'before': '_handle_error_wrapper'
            },
            {
                'trigger': HardwareStateTransition.ERROR.value,
                'source': [HardwareState.RUNNING.value, HardwareState.PAUSED.value],
                'dest': HardwareState.ERROR_RUNTIME.value,
                'before': '_handle_error_wrapper'
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
    """ A proxy object required to wrap Hardware objects and handle reset transitions. """

    def _handle_nop(self, _: transitions.EventData) -> typing.NoReturn:
        self._logger.debug('No transition')

    def _handle_cleanup_running(self, event: transitions.EventData) -> typing.NoReturn:
        # Stop first
        self.handle_stop(event)

        # Then cleanup
        self.handle_cleanup(event)

    def _handle_error_wrapper(self, event: transitions.EventData) -> typing.NoReturn:
        # Always allow transition to error state
        try:
            self.handle_error(event)
        except Exception:
            self._logger.critical('Unhandled exception while transitioning to error state', exc_info=True)

    def _handle_reset_ready(self, event: transitions.EventData) -> typing.NoReturn:
        """ Called when transitioning from error state to ready state.

        :param event: transition metadata
        """
        # Clear error state
        try:
            self.handle_reset(event)
        except NotImplementedError:
            # Use default setup method
            self.handle_setup(event)

    def _handle_reset_runtime(self, event: transitions.EventData) -> typing.NoReturn:
        """ Called when transitioning from error state to running state.

        :param event:
        :return:
        """
        # Clear error state
        try:
            self.handle_reset(event)
        except NotImplementedError:
            # Use default setup method
            self.handle_setup(event)

        # Process start transition
        self.handle_start(event)

    def handle_transition(self, transition: HardwareStateTransition):
        pass


class HardwareStateManager(CallbackThread):
    """ Hardware state manager for asynchronous monitoring and control of Hardware objects. """

    # Limit the maximum repetition rate of the management thread
    _MINIMUM_RUN_PERIOD = 1

    # Timeout for Hardware reset after errors occur
    _TIMEOUT_RESET = 10

    # Internal list of manager instances
    _manager_instances: typing.List[HardwareStateManager] = []

    def __init__(self, hardware: Hardware):
        """

        :param hardware: Hardware object to manage
        """
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
        self._parameter_queue = []
        self._parameter_lock = threading.Condition()
        self._parameter_error: typing.Optional[Exception] = None

        # Reset timeout
        self._reset_time = None

        # Add self to list of hardware managers
        self._manager_instances.append(self)

    @classmethod
    def get_all_managers(cls) -> typing.List[HardwareStateManager]:
        """ Get all Hardware state managers that have been created.

        :return: list
        """
        return cls._manager_instances

    def get_error(self) -> typing.Optional[Exception]:
        """ Get Exception thrown by last transition if available.

        :return: Exception or None
        """
        with self._state_lock:
            return self._state_error

    def is_idle(self) -> bool:
        """ Test if Hardware is in idle state.

        :return: True when Hardware is idle, False otherwise
        """
        return self.get_state().is_idle()

    def is_error(self) -> bool:
        """ Test if Hardware is in error state.

        :return: True when Hardware is in error state, False otherwise

        :return:
        """
        return self.get_state().is_error()

    def get_hardware(self) -> Hardware:
        """ Get handle to the target Hardware object.

        :return: Hardware
        """
        return self._hardware

    def get_state(self) -> HardwareState:
        """ Get the current Hardware state.

        :return: HardwareState
        """
        with self._state_lock:
            return HardwareState(self._hardware.state)

    def queue_parameter(self, parameter_dict: TYPE_PARAMETER_DICT, wait: bool = False):
        """

        :param parameter_dict:
        :param wait:
        :return:
        """
        with self._parameter_lock:
            # Append to queue
            self._parameter_queue.append(parameter_dict)

            if wait:
                # Clear any existing error
                self._parameter_error = None

                self._parameter_lock.wait()

                # Validate parameter
                if self._parameter_error is not None:
                    raise self._parameter_error

    def queue_transition(self, transition: HardwareStateTransition, wait: bool = True):
        with self._state_lock:
            # Check for pending state transition
            if transition is not HardwareStateTransition.ERROR and self._state_transition is not None:
                # Wait for completion
                self._state_lock.wait()

            # Set target
            self._state_transition = transition

            if wait:
                # Clear any existing error
                self._state_error = None

                self._state_lock.wait()

                # Validate state change
                if self._state_error is not None:
                    raise self._state_error

    def _thread_manager(self):
        operation_flag = False

        # Record start time of loop
        time_run_start = time.time()

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
                except HardwareException:
                    self._logger.exception('Hardware error occurred during shutdown')

                    self._hardware.error()
                except transitions.MachineError:
                    self._logger.exception('State machine error occurred during shutdown')

                    self._hardware.error()
                except Exception:
                    self._logger.exception('Unhandled exception occurred during shutdown')

                    self._hardware.error()
                finally:
                    current_state = self.get_state()

                    if current_state != HardwareState.IDLE:
                        self._logger.error(f"{self._hardware.get_hardware_identifier()} could not be returned "
                                           f"to idle state during shutdown", notify=True)

                return

            # Handle state transition
            if self._state_transition is not None:
                initial_state = current_state

                self._logger.debug(f"Processing transition {self._state_transition}")

                # Clear any existing error flag
                self._state_error = None

                try:
                    # Get transition method
                    transition_method = getattr(self._hardware, self._state_transition.value)

                    # Attempt to change state
                    with self._hardware.get_hardware_lock():
                        transition_method()

                    # Indicate a hardware operation occurred
                    operation_flag = True
                except HardwareException as exc:
                    self._state_error = exc

                    # Set state to error if state was not idle or already in error state
                    self._hardware.error()
                except transitions.MachineError as exc:
                    # Cache error
                    self._state_error = exc
                except Exception as exc:
                    # Cache error for calling thread
                    self._state_error = exc

                    # Set state to error if state was not idle
                    self._hardware.error()

                    # Raise exception to thread
                    raise
                finally:
                    # Clear pending state
                    self._state_transition = None

                    # Update current state
                    current_state = self.get_state()

                    if initial_state == current_state:
                        self._logger.warning(f"Unchanged state {current_state}", event=True, notify=False)
                    else:
                        self._logger.info(f"Transitioned state {initial_state} -> {current_state}", event=True)

                    # Notify waiting threads
                    self._state_lock.notify_all()

        # Check for error state and handle automatic reset
        if current_state.is_error():
            if self._reset_time is None:
                # Set timeout for reset if not already set
                self._reset_time = time.time() + self._TIMEOUT_RESET
            elif time.time() >= self._reset_time:
                # Handle reset timeout
                self._logger.info(f"Attempting to reset {self._hardware.get_hardware_identifier()}", event=True)

                with self._hardware.get_hardware_lock():
                    try:
                        self._hardware.reset()

                        self._logger.info('Reset successful', notify=True, event=True)

                        # Clear reset timeout
                        self._reset_time = None
                    except HardwareException:
                        self._logger.warning('Hardware error occurred during reset', exc_info=True, event=True)

                        # Set new timeout
                        self._reset_time = time.time() + self._TIMEOUT_RESET
                    finally:
                        current_state = self.get_state()

            # Set operation flag to enable delay before next check
            operation_flag = True

        # If active then process parameters and measurements
        if current_state.is_active() and len(self._parameter_queue) > 0:
            with self._parameter_lock:
                try:
                    # Apply pending parameters
                    with self._hardware.get_hardware_lock():
                        while len(self._parameter_queue) > 0:
                            parameter_dict = self._parameter_queue.pop(0)
                            self._hardware.set_parameter(parameter_dict)
                except HardwareException as exc:
                    # Cache error
                    self._parameter_error = exc

                    # Transition to error state
                    self._hardware.error()
                except Exception as exc:
                    # Cache error for calling thread
                    self._parameter_error = exc

                    # Transition to error state
                    self._hardware.error()

                    raise
                finally:
                    # Notify waiting thread(s)
                    self._parameter_lock.notify_all()

        # If running then take a measurement
        if current_state == HardwareState.RUNNING:
            try:
                with self._hardware.get_hardware_lock():
                    operation_flag |= self._hardware.produce_measurement()
            except HardwareException:
                self._logger.exception('Hardware error occurred while gathering measurements')

                # Transition to error state
                self._hardware.error()
            except Exception:
                # Transition to error state
                self._hardware.error()

                raise

        # If no operations occurred then limit rate process is run
        if not operation_flag:
            time_run_delta = time.time() - time_run_start

            if time_run_delta < self._MINIMUM_RUN_PERIOD:
                time.sleep(self._MINIMUM_RUN_PERIOD - time_run_delta)
