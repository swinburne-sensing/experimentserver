from __future__ import annotations

import time
import typing
from datetime import datetime

import transitions
import wrapt

from . import CommandError, Hardware, HardwareError, CommunicationError, ExternalError, NoResetHandler, HardwareState,\
    HardwareTransition
from experimentserver.util.state import ManagedStateMachine, TYPE_STATE, TYPE_TRANSITION
from experimentserver.util.thread import LockTimeout
from experimentserver.util.module import AbstractTracked


# noinspection PyAbstractClass
class HardwareStateWrapper(wrapt.ObjectProxy):
    """ A proxy object required to wrap Hardware objects and handle reset transitions. """
    def __init__(self, wrapped: Hardware):
        super(HardwareStateWrapper, self).__init__(wrapped)

    def _wrapped_transition_error(self, event: transitions.EventData):
        try:
            self.transition_error(event)
        except CommunicationError:
            self.get_logger().exception('Communication error occurred while handling error')
        except ExternalError:
            self.get_logger().exception('Hardware reported error while handling error')

    def _wrapped_transition_reset(self, event: transitions.EventData):
        try:
            self.transition_reset(event)
        except NoResetHandler:
            pass


class HardwareManager(ManagedStateMachine, AbstractTracked):
    """ Hardware manager manager for asynchronous monitoring and control of Hardware objects. """

    # Limit the maximum repetition rate of the management thread when no measurements are made
    _MINIMUM_RUN_PERIOD = 1

    # Timeout for Hardware reset after errors occur
    _TIMEOUT_RESET = 30

    # Internal list of manager instances
    _manager_instances: typing.Mapping[str, HardwareManager] = {}

    def __init__(self, hardware: Hardware):
        """

        :param hardware: Hardware object to manage
        """
        # Wrap Hardware object with additional handling methods
        self._hardware = HardwareStateWrapper(hardware)

        # Setup state machine within wrapped hardware
        ManagedStateMachine.__init__(self, self._hardware.get_hardware_identifier(True), self._hardware, HardwareState,
                                     HardwareTransition, HardwareState.DISCONNECTED)
        AbstractTracked.__init__(self, self._hardware.get_identifier())

        # Reset timeout and target state to restore
        self._reset_time = None
        self._reset_state = HardwareState.DISCONNECTED

    def get_hardware(self) -> Hardware:
        return self._hardware.__wrapped__

    def _handle_transition_exception(self, initial_state: TYPE_STATE, transition: TYPE_TRANSITION, exc: Exception):
        super()._handle_transition_exception(initial_state, transition, exc)

        # Transition to error state if hardware reported error during transition
        if issubclass(type(exc), HardwareError):
            HardwareTransition.ERROR.apply(self._hardware)

    def _thread_manager(self) -> typing.NoReturn:
        # Activity flag to indicate an operation was performed on this loop (prevents high idle CPU usage)
        start_time = time.time()

        with self._state_lock:
            initial_state = self._get_state()

            # Handle shutdown requests
            if self._test_thread_stop():
                if initial_state.is_error():
                    self.get_logger().error('Ignoring hardware in error manager during shutdown')
                    return

                try:
                    with self._hardware.hardware_lock():
                        # If currently running then stop
                        if initial_state == HardwareState.RUNNING:
                            self.get_logger().info('Stopping hardware')

                            HardwareTransition.STOP.apply(self._hardware)

                            initial_state = self._get_state()

                        # If stopped then cleanup
                        if initial_state == HardwareState.CONFIGURED:
                            self.get_logger().info('Cleaning up hardware')

                            HardwareTransition.CLEANUP.apply(self._hardware)

                            initial_state = self._get_state()

                        # If configured then disconnect
                        if initial_state == HardwareState.CONNECTED:
                            self.get_logger().info('Disconnecting hardware')

                            HardwareTransition.DISCONNECT.apply(self._hardware)
                except LockTimeout:
                    self.get_logger().exception('Could not acquire hardware lock during shutdown')
                except transitions.MachineError:
                    self.get_logger().exception('State machine error occurred during shutdown')
                except CommunicationError:
                    self.get_logger().exception('Hardware communication error occurred during shutdown')
                except ExternalError:
                    self.get_logger().exception('Hardware reported error during shutdown')

                    # Try and transition to error state (might contain fail-safe code)
                    HardwareTransition.ERROR.apply(self._hardware)
                finally:
                    # Break out of loop
                    return

            # Process pending transitions and ff state has changed then set activity flag
            activity_flag, current_state = self._process_transition()

        # Check for error state and handle automatic reset
        if current_state.is_error():
            if self._reset_time is None:
                # Set timeout for reset if not already set
                self._reset_time = time.time() + self._TIMEOUT_RESET

                self.get_logger().info(
                    f"Attempting reset at {datetime.fromtimestamp(self._reset_time).strftime('%H:%M:%S')}")
            elif time.time() >= self._reset_time:
                # Attempt to reset hardware after timeout
                self.get_logger().info('Attempting reset', event=True)

                with self._hardware.hardware_lock():
                    try:
                        HardwareTransition.RESET.apply(self._hardware)

                        # Attempt to restore state
                        if self._reset_state is HardwareState.CONNECTED or \
                                self._reset_state is HardwareState.CONFIGURED or \
                                self._reset_state is HardwareState.RUNNING:
                            self.get_logger().info("Attempting reconnect")
                            HardwareTransition.CONNECT.apply(self._hardware)

                        if self._reset_state is HardwareState.CONFIGURED or \
                                self._reset_state is HardwareState.RUNNING:
                            self.get_logger().info("Attempting reconfigure")
                            HardwareTransition.CONFIGURE.apply(self._hardware)

                        if self._reset_state is HardwareState.RUNNING:
                            self.get_logger().info("Attempting restart")
                            HardwareTransition.START.apply(self._hardware)

                        self.get_logger().info('Reset successful', event=True, notify=True)
                    except CommunicationError:
                        self.get_logger().error('Hardware communication error occurred during reset', exc_info=True,
                                                event=True, notify=False)

                        # Return to error manager
                        HardwareTransition.ERROR.apply(self._hardware)
                    except ExternalError:
                        self.get_logger().error('Hardware reported error during reset', exc_info=True,
                                                event=True, notify=False)

                        # Return to error manager
                        HardwareTransition.ERROR.apply(self._hardware)
                    finally:
                        # Clear reset timeout (if still in error then try again on next loop)
                        self._reset_time = None

                        current_state = self.get_state()

            # Set operation flag to enable delay before next check
            activity_flag = True
        else:
            # Clear reset timer
            self._reset_time = None

            # Store current state for future resets
            self._reset_state = current_state

        # If running then attempt to take measurement
        if current_state.is_running():
            with self._hardware.hardware_lock():
                try:
                    activity_flag |= self._hardware.produce_measurement()
                except CommandError:
                    self.get_logger().exception('Command error occurred while gathering measurements')

                    # Transition to error manager
                    HardwareTransition.ERROR.apply(self._hardware)
                except CommunicationError:
                    self.get_logger().exception('Hardware communication error occurred while gathering measurements')

                    # Transition to error manager
                    HardwareTransition.ERROR.apply(self._hardware)
                except ExternalError:
                    self.get_logger().exception('Hardware reported error while gathering measurements')

                    # Transition to error manager
                    HardwareTransition.ERROR.apply(self._hardware)
                except Exception:
                    # Transition to error manager
                    HardwareTransition.ERROR.apply(self._hardware)

                    # Bump to main thread
                    raise

        # If no operations occurred then limit rate process is run
        if not activity_flag:
            delta_time = time.time() - start_time

            if delta_time < self._MINIMUM_RUN_PERIOD:
                time.sleep(self._MINIMUM_RUN_PERIOD - delta_time)
