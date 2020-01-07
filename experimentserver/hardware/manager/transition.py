from __future__ import annotations

import enum
import typing

from transitions import EventData
from wrapt import ObjectProxy

from .. import Hardware, CommunicationError, HardwareError, NoEventHandler


class HardwareState(enum.Enum):
    """ Enum representing Hardware state machine states. """

    # Disconnected state, Hardware is not connected in any way
    DISCONNECTED = 'disconnected'

    # Hardware is connected but requires configuration
    CONNECTED = 'connected'

    # Hardware is connected and configured, ready to run
    CONFIGURED = 'configured'

    # Hardware is producing measurements
    RUNNING = 'running'

    # Error state
    ERROR = 'error'

    def is_active(self) -> bool:
        """ Test if Hardware is available for use.

        :return: True when hardware will have commands processed, False otherwise
        """
        return self == self.CONFIGURED or self == self.RUNNING

    def is_connected(self) -> bool:
        """ Test if Hardware is connected to external instrument.

        :return: True when connected, False otherwise
        """
        return not self.is_error() and self != self.DISCONNECTED

    def is_configured(self) -> bool:
        return not self.is_error() and self != self.DISCONNECTED and self != self.CONNECTED

    def is_error(self) -> bool:
        """ Test if Hardware is in error state.

        :return: True when in error state, False otherwise
        """
        return self == self.ERROR

    def is_running(self) -> bool:
        """

        :return:
        """
        return self == self.RUNNING


class HardwareTransition(enum.Enum):
    """ Enum representing Hardware state machine transitions. """

    # Connect and disconnect from hardware
    CONNECT = 'transition_connect'
    DISCONNECT = 'transition_disconnect'

    # Configure hardware before use and cleanup after use
    CONFIGURE = 'transition_configure'
    CLEANUP = 'transition_cleanup'

    # Start and stop measurements
    START = 'transition_start'
    STOP = 'transition_stop'

    # Move to error state
    ERROR = 'transition_error'

    # Reset from error state (and possibly resume operation)
    RESET = 'transition_reset'

    def apply(self, model) -> bool:
        # Fetch method
        method = getattr(model, self.value)

        return method()

    @staticmethod
    def get_transitions():
        return [
            # Connect
            {
                # From disconnected
                'trigger': HardwareTransition.CONNECT.value,
                'source': HardwareState.DISCONNECTED.value,
                'dest': HardwareState.CONNECTED.value,
                'before': 'handle_connect'
            },

            # Disconnect
            {
                # From connected
                'trigger': HardwareTransition.DISCONNECT.value,
                'source': HardwareState.CONNECTED.value,
                'dest': HardwareState.DISCONNECTED.value,
                'before': 'handle_disconnect'
            },

            # Configure
            {
                # From connected
                'trigger': HardwareTransition.CONFIGURE.value,
                'source': HardwareState.CONNECTED.value,
                'dest': HardwareState.CONFIGURED.value,
                'before': 'handle_configure'
            },

            # Cleanup
            {
                # From configured
                'trigger': HardwareTransition.CLEANUP.value,
                'source': HardwareState.CONFIGURED.value,
                'dest': HardwareState.CONNECTED.value,
                'before': 'handle_cleanup'
            },

            # Start
            {
                # From configured
                'trigger': HardwareTransition.START.value,
                'source': HardwareState.CONFIGURED.value,
                'dest': HardwareState.RUNNING.value,
                'before': 'handle_start'
            },

            # Stop
            {
                # From running
                'trigger': HardwareTransition.STOP.value,
                'source': HardwareState.RUNNING.value,
                'dest': HardwareState.CONFIGURED.value,
                'before': 'handle_stop'
            },

            # Error transitions
            {
                # From disconnected, do nothing
                'trigger': HardwareTransition.ERROR.value,
                'source': HardwareState.DISCONNECTED.value,
                'dest': HardwareState.DISCONNECTED.value
            },
            {
                # From connected
                'trigger': HardwareTransition.ERROR.value,
                'source': [HardwareState.CONNECTED.value, HardwareState.CONFIGURED.value, HardwareState.RUNNING.value],
                'dest': HardwareState.ERROR.value,
                'before': '_save_state',
                'after': '_wrapped_handle_error'
            },

            # Reset transitions
            {
                # from reconnect
                'trigger': HardwareTransition.RESET.value,
                'source': HardwareState.ERROR.value,
                'dest': HardwareState.DISCONNECTED.value,
                'before': '_wrapped_handle_reset',
                'after': '_restore_state'
            }
        ]


# noinspection PyAbstractClass
class HardwareStateWrapper(ObjectProxy):
    """ A proxy object required to wrap Hardware objects and handle reset transitions. """
    def __init__(self, wrapped: Hardware):
        super(HardwareStateWrapper, self).__init__(wrapped)

        # Storage for state prior to triggering an error
        self._state_error: typing.Optional[HardwareState] = None

    def _save_state(self, event: EventData):
        # Store current state before entering error state
        self._state_error = HardwareState(event.state.name)

    def _restore_state(self, event: EventData):
        assert self._state_error is not None

        if self._state_error is HardwareState.RUNNING or self._state_error is HardwareState.CONFIGURED \
                or self._state_error is HardwareState.CONNECTED:
            HardwareTransition.CONNECT.apply(event.model)

        if self._state_error is HardwareState.RUNNING or self._state_error is HardwareState.CONFIGURED:
            HardwareTransition.CONFIGURE.apply(event.model)

        if self._state_error is HardwareState.RUNNING:
            HardwareTransition.START.apply(event.model)

        # Clear error state
        self._state_error = None

    def _wrapped_handle_error(self, event: EventData):
        try:
            self.handle_error(event)
        except CommunicationError:
            self._logger.exception('Communication error occurred while handling error')
        except HardwareError:
            self._logger.exception('Hardware reported error while handling error')

    def _wrapped_handle_reset(self, event: EventData):
        try:
            self.handle_reset(event)
        except NoEventHandler:
            pass
