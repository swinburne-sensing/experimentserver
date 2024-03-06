from __future__ import annotations

import time
import threading
from datetime import datetime
from typing import Mapping, MutableMapping, Optional, Union

import transitions
import wrapt

from .base.core import Hardware
from .control import HardwareState, HardwareTransition
from .error import HardwareError, HardwareIdentifierError, CommunicationError, CommandError, ExternalError, \
    NoResetHandler
from experimentserver.util.state import ManagedStateMachine
from experimentserver.util.thread import LockTimeout


# noinspection PyAbstractClass
class HardwareStateWrapper(wrapt.ObjectProxy):
    """ A proxy object required to wrap Hardware objects and handle reset transitions. """
    def __init__(self, wrapped: Hardware):
        super(HardwareStateWrapper, self).__init__(wrapped)

    def _wrapped_transition_error(self, event: transitions.EventData) -> None:
        try:
            self.transition_error(event)
        except CommunicationError:
            self.logger().exception('Communication error occurred while handling error')
        except ExternalError:
            self.logger().exception('Hardware reported error while handling error')

    def _wrapped_transition_reset(self, event: transitions.EventData) -> None:
        try:
            self.transition_reset(event)
        except NoResetHandler:
            pass


class HardwareManager(ManagedStateMachine[HardwareState, HardwareTransition]):
    """ Hardware manager manager for asynchronous monitoring and control of Hardware objects. """

    # Limit the maximum repetition rate of the management thread when no measurements are made
    _MINIMUM_RUN_PERIOD = 1

    # Timeout for Hardware reset after errors occur
    _TIMEOUT_RESET = 30.0

    _WATCHDOG_RESET = 300.0

    # List of active hardware manager instances
    _MANAGER_LUT: MutableMapping[str, HardwareManager] = {}
    _MANAGER_LUT_LOCK = threading.RLock()

    def __init__(self, hardware: Hardware):
        """

        :param hardware: Hardware object to manage
        """
        # Wrap Hardware object with additional handling methods
        self._hardware: Union[HardwareStateWrapper, Hardware] = HardwareStateWrapper(hardware)

        # Setup state machine within wrapped hardware
        ManagedStateMachine.__init__(self, self._hardware.get_hardware_identifier(True), self._hardware, HardwareState,
                                     HardwareTransition, HardwareState.DISCONNECTED)

        # Reset timeout and target state to restore
        self._reset_time: Optional[float] = None
        self._reset_state = HardwareState.DISCONNECTED

        # Watchdog
        self._watchdog = threading.Event()

        # Save instance, conflict management already handled by Hardware
        with self._MANAGER_LUT_LOCK:
            self._MANAGER_LUT[self._hardware.get_hardware_identifier()] = self

    @classmethod
    def get_instance(cls, hardware_identifier: str) -> HardwareManager:
        with cls._MANAGER_LUT_LOCK:
            if hardware_identifier not in cls._MANAGER_LUT:
                raise HardwareIdentifierError(f"Unknown hardware identifier \"{hardware_identifier}\"")

            return cls._MANAGER_LUT[hardware_identifier]

    @classmethod
    def get_all_instances(cls) -> Mapping[str, HardwareManager]:
        with cls._MANAGER_LUT_LOCK:
            return dict(cls._MANAGER_LUT)

    def get_hardware(self) -> Hardware:
        return self._hardware.__wrapped__

    def check_watchdog(self) -> None:
        if not self.is_thread_alive():
            raise HardwareError('Thread stopped')

        # Check watchdog flag is getting set
        self._watchdog.clear()

        if not self._watchdog.wait(self._WATCHDOG_RESET):
            raise HardwareError('Watchdog timeout')

    def force_disconnect(self) -> None:
        """ Force hardware to disconnected state regardless of current state. """
        # Get current state
        with self._state_lock:
            # Clear pending transitions
            self.clear_transition()

            # Check current state
            hardware_state = self._get_state()

            if hardware_state == HardwareState.DISCONNECTED:
                self.logger().debug('Already disconnected disconnect')
                return
            
            self.logger().warning('Forcing disconnect')

            try:
                with self._hardware.hardware_lock('force_disconnect'):
                    if hardware_state.is_error():
                        self.logger().error('Hardware in error state during forced disconnect')

                        # Clear error state
                        HardwareTransition.RESET.apply(self._hardware)
                    else:
                        # If currently running then stop
                        if hardware_state == HardwareState.RUNNING:
                            self.logger().info('Stopping hardware')

                            HardwareTransition.STOP.apply(self._hardware)

                            hardware_state = self._get_state()

                        # If stopped then cleanup
                        if hardware_state == HardwareState.CONFIGURED:
                            self.logger().info('Cleaning up hardware')

                            HardwareTransition.CLEANUP.apply(self._hardware)

                            hardware_state = self._get_state()

                        # If configured then disconnect
                        if hardware_state == HardwareState.CONNECTED:
                            self.logger().info('Disconnecting hardware')

                            HardwareTransition.DISCONNECT.apply(self._hardware)
            except CommunicationError:
                self.logger().exception('Hardware communication error occurred during disconnect')
            except ExternalError:
                self.logger().exception('Hardware reported error during during disconnect')

                # Try and transition to error state (might contain fail-safe code)
                HardwareTransition.ERROR.apply(self._hardware)

                # Finally reset to clear error state
                HardwareTransition.RESET.apply(self._hardware)

    def quick_start(self, connect: bool = True, configure: bool = True, start: bool = True) -> None:
        if connect:
            self.queue_transition(HardwareTransition.CONNECT)

        if configure:
            self.queue_transition(HardwareTransition.CONFIGURE)

        if start:
            self.queue_transition(HardwareTransition.START)

    def quick_stop(self, stop: bool = True, cleanup: bool = True, disconnect: bool = True) -> None:
        if stop:
            self.queue_transition(HardwareTransition.STOP)

        if cleanup:
            self.queue_transition(HardwareTransition.CLEANUP)

        if disconnect:
            self.queue_transition(HardwareTransition.DISCONNECT)
    
    def _handle_transition_exception(self, initial_state: HardwareState, transition: HardwareTransition, exc: Exception) -> None:
        super()._handle_transition_exception(initial_state, transition, exc)

        # Transition to error state if hardware reported error during transition
        if isinstance(exc, HardwareError):
            HardwareTransition.ERROR.apply(self._hardware)

    def _thread_manager(self) -> None:
        # Activity flag to indicate an operation was performed on this loop (prevents high idle CPU usage)
        start_time = time.time()

        with self._state_lock:
            # Handle shutdown requests
            if self.thread_stop_requested():
                try:
                    self.force_disconnect()
                except LockTimeout:
                    self.logger().exception('Could not acquire hardware lock during shutdown')
                except transitions.MachineError:
                    self.logger().exception('State machine error occurred during shutdown')

                # Exit from thread
                return

            # Process pending transitions and ff state has changed then set activity flag
            activity_flag, _ = self._process_transition()
            
        # Check for error state and handle automatic reset
        if current_state.is_error():
            if self._reset_time is None:
                # Set timeout for reset if not already set
                self._reset_time = time.time() + self._TIMEOUT_RESET

                self.logger().info(f"Reset at {datetime.fromtimestamp(self._reset_time).strftime('%H:%M:%S')}")
            elif time.time() >= self._reset_time:
                # Attempt to reset hardware after timeout
                self.logger().info('Attempting hardware reset', event=True)

                with self._hardware.hardware_lock('_thread_manager'):
                    try:
                        HardwareTransition.RESET.apply(self._hardware)

                        # Attempt to restore state
                        if self._reset_state is HardwareState.CONNECTED or \
                                self._reset_state is HardwareState.CONFIGURED or \
                                self._reset_state is HardwareState.RUNNING:
                            self.logger().info("Attempting hardware reconnect")
                            HardwareTransition.CONNECT.apply(self._hardware)

                        if self._reset_state is HardwareState.CONFIGURED or \
                                self._reset_state is HardwareState.RUNNING:
                            self.logger().info("Attempting hardware reconfigure")
                            HardwareTransition.CONFIGURE.apply(self._hardware)

                        if self._reset_state is HardwareState.RUNNING:
                            self.logger().info("Attempting hardware restart")
                            HardwareTransition.START.apply(self._hardware)

                        self.logger().info('Successfully reset hardware', event=True, notify=True)
                    except CommunicationError:
                        self.logger().error('Hardware communication error occurred during reset', exc_info=True)

                        # Return to error state
                        HardwareTransition.ERROR.apply(self._hardware)
                    except ExternalError:
                        self.logger().error('Hardware reported error during reset', exc_info=True)

                        # Return to error state
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
        running_flag = current_state.is_running()

        if running_flag:
            try:
                activity_flag |= self._hardware.produce_measurement()
            except CommandError:
                self.logger().exception('Command error occurred while gathering measurements')

                # Transition to error state
                HardwareTransition.ERROR.apply(self._hardware)
            except CommunicationError:
                self.logger().exception('Hardware communication error occurred while gathering measurements')

                # Transition to error state
                HardwareTransition.ERROR.apply(self._hardware)
            except ExternalError:
                self.logger().exception('Hardware reported error while gathering measurements')

                # Transition to error state
                HardwareTransition.ERROR.apply(self._hardware)
            except Exception:
                # Transition to error state
                HardwareTransition.ERROR.apply(self._hardware)

                # Bump to main thread
                raise

        # Kick watchdog
        self._watchdog.set()

        # If no operations occurred then limit rate process is run
        if not activity_flag:
            delta_time = time.time() - start_time


    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(hardware={self._hardware!r})"
