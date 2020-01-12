from __future__ import annotations

import enum


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
    CONNECT = 'connect'
    DISCONNECT = 'disconnect'

    # Configure hardware before use and cleanup after use
    CONFIGURE = 'configure'
    CLEANUP = 'cleanup'

    # Start and stop measurements
    START = 'start'
    STOP = 'stop'

    # Move to error state
    ERROR = 'error'

    # Reset from error state (and possibly resume operation)
    RESET = 'reset'

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
                'before': 'transition_' + HardwareTransition.CONNECT.value
            },

            # Disconnect
            {
                # From connected
                'trigger': HardwareTransition.DISCONNECT.value,
                'source': HardwareState.CONNECTED.value,
                'dest': HardwareState.DISCONNECTED.value,
                'before': 'transition_' + HardwareTransition.DISCONNECT.value
            },
            {
                # From error
                'trigger': HardwareTransition.DISCONNECT.value,
                'source': HardwareState.ERROR.value,
                'dest': HardwareState.DISCONNECTED.value
            },

            # Configure
            {
                # From connected
                'trigger': HardwareTransition.CONFIGURE.value,
                'source': HardwareState.CONNECTED.value,
                'dest': HardwareState.CONFIGURED.value,
                'before': 'transition_' + HardwareTransition.CONFIGURE.value
            },

            # Cleanup
            {
                # From configured
                'trigger': HardwareTransition.CLEANUP.value,
                'source': HardwareState.CONFIGURED.value,
                'dest': HardwareState.CONNECTED.value,
                'before': 'transition_' + HardwareTransition.CLEANUP.value
            },

            # Start
            {
                # From configured
                'trigger': HardwareTransition.START.value,
                'source': HardwareState.CONFIGURED.value,
                'dest': HardwareState.RUNNING.value,
                'before': 'transition_' + HardwareTransition.START.value
            },

            # Stop
            {
                # From running
                'trigger': HardwareTransition.STOP.value,
                'source': HardwareState.RUNNING.value,
                'dest': HardwareState.CONFIGURED.value,
                'before': 'transition_' + HardwareTransition.STOP.value
            },

            # Error transitions
            {
                # From disconnected
                'trigger': HardwareTransition.ERROR.value,
                'source': HardwareState.DISCONNECTED.value,
                'dest': HardwareState.ERROR.value
            },
            {
                # From connected
                'trigger': HardwareTransition.ERROR.value,
                'source': [HardwareState.CONNECTED.value, HardwareState.CONFIGURED.value, HardwareState.RUNNING.value],
                'dest': HardwareState.ERROR.value,
                'after': '_wrapped_transition_error'
            },

            # Reset transitions
            {
                # from reconnect
                'trigger': HardwareTransition.RESET.value,
                'source': HardwareState.ERROR.value,
                'dest': HardwareState.DISCONNECTED.value,
                'before': '_wrapped_transition_reset',
            }
        ]
