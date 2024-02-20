import typing

from ..util.state import ManagedState, ManagedTransition


class HardwareState(ManagedState):
    """ Enum representing Hardware manager machine states. """

    # Disconnected manager, Hardware is not connected in any way
    DISCONNECTED = 'disconnected'

    # Hardware is connected but requires configuration
    CONNECTED = 'connected'

    # Hardware is connected and configured, ready to run
    CONFIGURED = 'configured'

    # Hardware is producing measurements
    RUNNING = 'running'

    # Error manager
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
        """ Test if Hardware is in error manager.

        :return: True when in error manager, False otherwise
        """
        return self == self.ERROR

    def is_running(self) -> bool:
        """

        :return:
        """
        return self == self.RUNNING


class HardwareTransition(ManagedTransition):
    """ Enum representing Hardware manager machine transitions. """

    # Connect and disconnect from hardware
    CONNECT = 'connect'
    DISCONNECT = 'disconnect'

    # Configure hardware before use and cleanup after use
    CONFIGURE = 'configure'
    CLEANUP = 'cleanup'

    # Start and stop measurements
    START = 'start'
    STOP = 'stop'

    # Move to error manager
    ERROR = 'error'

    # Reset from error manager (and possibly resume operation)
    RESET = 'reset'

    # Process parameters
    PARAMETER = 'parameter'

    @classmethod
    def get_transitions(cls) -> typing.List[typing.Dict[str, typing.Union[str, typing.Sequence[str]]]]:
        return [
            # Connect
            {
                # From disconnected
                'trigger': cls.CONNECT.value,
                'source': HardwareState.DISCONNECTED.value,
                'dest': HardwareState.CONNECTED.value,
                'before': 'transition_' + cls.CONNECT.value
            },

            # Disconnect
            {
                # From connected
                'trigger': cls.DISCONNECT.value,
                'source': HardwareState.CONNECTED.value,
                'dest': HardwareState.DISCONNECTED.value,
                'before': 'transition_' + cls.DISCONNECT.value
            },
            {
                # From error
                'trigger': cls.DISCONNECT.value,
                'source': HardwareState.ERROR.value,
                'dest': HardwareState.DISCONNECTED.value
            },

            # Configure
            {
                # From connected
                'trigger': cls.CONFIGURE.value,
                'source': HardwareState.CONNECTED.value,
                'dest': HardwareState.CONFIGURED.value,
                'before': 'transition_' + cls.CONFIGURE.value
            },

            # Cleanup
            {
                # From configured
                'trigger': cls.CLEANUP.value,
                'source': HardwareState.CONFIGURED.value,
                'dest': HardwareState.CONNECTED.value,
                'before': 'transition_' + cls.CLEANUP.value
            },

            # Start
            {
                # From configured
                'trigger': cls.START.value,
                'source': HardwareState.CONFIGURED.value,
                'dest': HardwareState.RUNNING.value,
                'before': 'transition_' + cls.START.value
            },

            # Stop
            {
                # From running
                'trigger': cls.STOP.value,
                'source': HardwareState.RUNNING.value,
                'dest': HardwareState.CONFIGURED.value,
                'before': 'transition_' + cls.STOP.value
            },

            # Error transitions
            {
                # From disconnected
                'trigger': cls.ERROR.value,
                'source': HardwareState.DISCONNECTED.value,
                'dest': HardwareState.DISCONNECTED.value
            },
            {
                # From connected
                'trigger': cls.ERROR.value,
                'source': [HardwareState.CONNECTED.value, HardwareState.CONFIGURED.value, HardwareState.RUNNING.value],
                'dest': HardwareState.ERROR.value,
                'after': '_wrapped_transition_error'
            },

            # Reset transitions
            {
                # from reconnect
                'trigger': cls.RESET.value,
                'source': HardwareState.ERROR.value,
                'dest': HardwareState.DISCONNECTED.value,
                'before': '_wrapped_transition_reset',
            },

            # Parameter transitions
            {
                'trigger': cls.PARAMETER.value,
                'source': [HardwareState.CONFIGURED.value, HardwareState.RUNNING.value],
                'dest': '=',
                'after': '_handle_parameter'
            },
            {
                'trigger': cls.PARAMETER.value,
                'source': HardwareState.ERROR.value,
                'dest': '=',
                'after': '_buffer_parameters'
            }
        ]
