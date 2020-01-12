from __future__ import annotations
import threading
import time
import typing
from datetime import datetime

import transitions
from wrapt import ObjectProxy

from .. import Hardware, TYPE_PARAMETER_DICT
from experimentserver.hardware import NoEventHandler
from experimentserver.hardware.error import CommandError, CommunicationError, HardwareError
from .transition import HardwareState, HardwareTransition
from experimentserver.util.thread import CallbackThread, LockTimeout


# noinspection PyAbstractClass
class HardwareStateWrapper(ObjectProxy):
    """ A proxy object required to wrap Hardware objects and handle reset transitions. """
    def __init__(self, wrapped: Hardware):
        super(HardwareStateWrapper, self).__init__(wrapped)

    def _wrapped_transition_error(self, event: transitions.EventData):
        try:
            self.transition_error(event)
        except CommunicationError:
            self._logger.exception('Communication error occurred while handling error')
        except HardwareError:
            self._logger.exception('Hardware reported error while handling error')

    def _wrapped_transition_reset(self, event: transitions.EventData):
        try:
            self.transition_reset(event)
        except NoEventHandler:
            pass


class StateManager(CallbackThread):
    """ Hardware state state for asynchronous monitoring and control of Hardware objects. """

    # Limit the maximum repetition rate of the management thread
    _MINIMUM_RUN_PERIOD = 1

    # Timeout for Hardware reset after errors occur
    _TIMEOUT_RESET = 30

    # Internal list of state instances
    _manager_instances: typing.List[StateManager] = []

    def __init__(self, hardware: Hardware):
        """

        :param hardware: Hardware object to manage
        """
        super().__init__(name=hardware.get_hardware_identifier(True), callback=self._thread_manager, run_final=True)

        # Wrap Hardware object with additional handling methods
        self._hardware: Hardware = HardwareStateWrapper(hardware)

        # Create state machine attached to the wrapped Hardware object
        self._machine = transitions.Machine(model=self._hardware, states=[x.value for x in HardwareState],
                                            transitions=HardwareTransition.get_transitions(),
                                            initial=HardwareState.DISCONNECTED.value, send_event=True)

        # State lock and error state
        self._state_lock = threading.Condition()
        self._state_error: typing.Optional[Exception] = None

        # Pending transition
        self._transition_pending: typing.Optional[HardwareTransition] = None
        self._target_state: typing.Optional[HardwareState] = None

        # Parameter lock and error state
        self._parameter_lock = threading.Condition()
        self._parameter_error: typing.Optional[Exception] = None

        # Parameter queue
        self._parameter_queue = []

        # Reset timeout
        self._reset_time = None

        # Add self to list of hardware managers
        self._manager_instances.append(self)

    @classmethod
    def get_all_managers(cls) -> typing.List[StateManager]:
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
        return self._hardware.__wrapped__

    def get_state(self) -> HardwareState:
        """ Get the current Hardware state.

        :return: HardwareState
        """
        with self._state_lock:
            return HardwareState(self._hardware.state)

    def queue_parameter(self, parameter_dict: TYPE_PARAMETER_DICT, timeout: typing.Optional[float] = None,
                        wait: bool = True) -> typing.NoReturn:
        """ Queue parameter change(s).

        :param parameter_dict:
        :param timeout:
        :param wait:
        """
        with self._parameter_lock:
            # Append to queue
            self._parameter_queue.append(parameter_dict)

            # Clear any existing error
            self._parameter_error = None

            if wait:
                # Wait for thread to apply changes
                processed = self._parameter_lock.wait(timeout)

                # Check for errors
                if self._parameter_error is not None:
                    raise self._parameter_error

                if not processed:
                    # Remove from queue
                    self._parameter_queue.remove(parameter_dict)

                    raise CommandError('Parameters not configured before timeout, hardware might not be ready or running')

    def queue_transition(self, pending_transition: HardwareTransition, wait: bool = True) -> typing.NoReturn:
        with self._state_lock:
            # Check for pending state transition
            if self._transition_pending is not None:
                # Wait for completion
                self._state_lock.wait()

            # Clear any existing error
            self._state_error = None

            # Set target
            self._transition_pending = pending_transition

            if wait:
                self._state_lock.wait()

                # Validate state change
                if self._state_error is not None:
                    raise self._state_error

    def _thread_manager(self):
        # Flag indication if Hardware operations were performed on a given loop, used to prevent fast idle loops
        operation_flag = False

        # Record start time of loop
        time_run_start = time.time()

        with self._state_lock:
            # Fetch current state
            current_state = self.get_state()

            # If shutdown has been triggered then stop, cleanup, and disconnect hardware
            if self._test_stop():
                if current_state.is_error():
                    self._logger.error('Ignoring hardware in error state during shutdown')
                    return

                try:
                    with self._hardware.hardware_lock():
                        # If currently running then stop
                        if current_state == HardwareState.RUNNING:
                            self._logger.info('Stopping running hardware')

                            HardwareTransition.STOP.apply(self._hardware)

                            current_state = self.get_state()

                        # If stopped then cleanup
                        if current_state == HardwareState.CONFIGURED:
                            self._logger.info('Cleaning up hardware')

                            HardwareTransition.CLEANUP.apply(self._hardware)

                            current_state = self.get_state()

                        # If configured then disconnect
                        if current_state == HardwareState.CONFIGURED:
                            self._logger.info('Cleaning up hardware')

                            HardwareTransition.DISCONNECT.apply(self._hardware)
                except LockTimeout:
                    self._logger.exception('Could not acquire hardware lock during shutdown')
                except transitions.MachineError:
                    self._logger.exception('State machine error occurred during shutdown')
                except CommunicationError:
                    self._logger.exception('Hardware communication error occurred during shutdown')
                except HardwareError:
                    self._logger.exception('Hardware reported error during shutdown')

                    # Try and transition to error state
                    self._hardware.transition_error()
                finally:
                    return

            # Handle state transition
            if self._transition_pending is not None:
                initial_state = current_state

                self._logger.debug(f"Processing transition {self._transition_pending}")

                try:
                    # Attempt pending transition
                    with self._hardware.hardware_lock():
                        self._transition_pending.apply(self._hardware)

                    # Indicate a hardware operation occurred
                    operation_flag = True

                    # Explicitly clear error flag
                    self._state_error = None
                except CommunicationError as exc:
                    # Cache error for calling thread
                    self._state_error = exc

                    # Transition to error state
                    HardwareTransition.ERROR.apply(self._hardware)
                except HardwareError as exc:
                    # Cache error for calling thread
                    self._state_error = exc

                    # Transition to error state
                    HardwareTransition.ERROR.apply(self._hardware)
                except transitions.MachineError as exc:
                    # Cache error for calling thread
                    self._state_error = exc
                except Exception as exc:
                    # Cache error for calling thread
                    self._state_error = exc

                    # Set state to error if state was not idle
                    if initial_state.is_connected():
                        HardwareTransition.ERROR.apply(self._hardware)

                    # Raise exception to thread
                    raise
                finally:
                    # Clear pending state
                    self._transition_pending = None

                    # Update current state
                    current_state = self.get_state()

                    # If not an error then set the current state to the target to get back to after reset
                    if not current_state.is_error():
                        self._target_state = current_state

                    if initial_state == current_state:
                        self._logger.warning(f"State did not transition from {current_state.value}", event=True,
                                             notify=False)
                    else:
                        self._logger.info(f"Changed state from {initial_state.value} to {current_state.value}",
                                          event=True)

                    # Notify waiting threads
                    self._state_lock.notify_all()

        # Check for error state and handle automatic reset
        if current_state.is_error():
            if self._reset_time is None:
                # Set timeout for reset if not already set
                self._reset_time = time.time() + self._TIMEOUT_RESET

                self._logger.info(
                    f"Attempting reset at {datetime.fromtimestamp(self._reset_time).strftime('%H:%M:%S')}")
            elif time.time() >= self._reset_time:
                # Handle reset timeout
                self._logger.info(f"Attempting reset", event=True)

                # Attempt reset
                with self._hardware.hardware_lock():
                    try:
                        HardwareTransition.RESET.apply(self._hardware)

                        if self._target_state is HardwareState.CONNECTED or \
                                self._target_state is HardwareState.CONFIGURED or \
                                self._target_state is HardwareState.RUNNING:
                            self._logger.info("Attempting reconnect")
                            HardwareTransition.CONNECT.apply(self._hardware)

                        if self._target_state is HardwareState.CONFIGURED or \
                                self._target_state is HardwareState.RUNNING:
                            self._logger.info("Attempting reconfigure")
                            HardwareTransition.CONFIGURE.apply(self._hardware)

                        if self._target_state is HardwareState.RUNNING:
                            self._logger.info("Attempting restart")
                            HardwareTransition.START.apply(self._hardware)

                        self._logger.info('Reset successful', event=True, notify=True)
                    except CommunicationError:
                        self._logger.error('Hardware communication error occurred during reset', exc_info=True,
                                           event=True, notify=False)

                        # Return to error state
                        HardwareTransition.ERROR.apply(self._hardware)
                    except HardwareError:
                        self._logger.error('Hardware reported error during reset', exc_info=True,
                                           event=True, notify=False)

                        # Return to error state
                        HardwareTransition.ERROR.apply(self._hardware)
                    finally:
                        # Clear reset timeout (if still in error then try again on next loop)
                        self._reset_time = None

                        current_state = self.get_state()

            # Set operation flag to enable delay before next check
            operation_flag = True
        else:
            self._reset_time = None

        # If active then process parameters and measurements
        if current_state.is_active() and len(self._parameter_queue) > 0:
            with self._parameter_lock:
                # Apply pending parameters
                with self._hardware.hardware_lock():
                    while len(self._parameter_queue) > 0:
                        try:
                            parameter_dict = self._parameter_queue.pop(0)
                            self._hardware.set_parameters(parameter_dict)
                        except CommandError as exc:
                            # Cache error
                            self._parameter_error = exc
                        except CommunicationError as exc:
                            # Cache error
                            self._parameter_error = exc

                            # Transition to error state
                            HardwareTransition.ERROR.apply(self._hardware)
                        except HardwareError as exc:
                            # Cache error
                            self._parameter_error = exc

                            # Transition to error state
                            HardwareTransition.ERROR.apply(self._hardware)
                        except Exception as exc:
                            # Cache error for calling thread
                            self._parameter_error = exc

                            # Transition to error state
                            HardwareTransition.ERROR.apply(self._hardware)

                            raise

                # Notify waiting thread(s)
                self._parameter_lock.notify_all()

        # If running then take a measurement_group
        if current_state.is_running():
            with self._hardware.hardware_lock():
                try:
                    operation_flag |= self._hardware.produce_measurement()
                except CommandError:
                    self._logger.exception('Command error occurred while gathering measurements')

                    # Transition to error state
                    HardwareTransition.ERROR.apply(self._hardware)
                except CommunicationError:
                    self._logger.exception('Hardware communication error occurred while gathering measurements')

                    # Transition to error state
                    HardwareTransition.ERROR.apply(self._hardware)
                except HardwareError:
                    self._logger.exception('Hardware reported error while gathering measurements')

                    # Transition to error state
                    HardwareTransition.ERROR.apply(self._hardware)
                except Exception:
                    # Transition to error state
                    HardwareTransition.ERROR.apply(self._hardware)

                    # Bump to main thread
                    raise

        # If no operations occurred then limit rate process is run
        if not operation_flag:
            time_run_delta = time.time() - time_run_start

            if time_run_delta < self._MINIMUM_RUN_PERIOD:
                time.sleep(self._MINIMUM_RUN_PERIOD - time_run_delta)
