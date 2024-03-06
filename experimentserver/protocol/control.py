import typing

from experimentserver.util.state import ManagedState, ManagedTransition


class ProcedureState(ManagedState):
    """ Procedure state machine state. """
    # Unverified procedure
    SETUP = 'setup'

    # Ready verified procedure
    READY = 'ready'

    # Running procedure
    RUNNING = 'running'

    # Paused procedure
    PAUSED = 'paused'

    def is_valid(self) -> bool:
        return self != self.SETUP

    def is_running(self) -> bool:
        return self == self.RUNNING or self == self.PAUSED


class ProcedureTransition(ManagedTransition):
    """ Procedure state machine state transitions. """
    VALIDATE = 'validate'
    START = 'start'
    PAUSE = 'pause'
    STOP = 'stop'
    NEXT = 'next'
    PREVIOUS = 'previous'
    REPEAT = 'repeat'
    GOTO = 'goto'
    FINISH = 'finish'
    ERROR = 'error'

    @classmethod
    def get_transitions(cls) -> typing.List[typing.Dict[str, typing.Union[str, typing.Sequence[str]]]]:
        return [
            # Validate procedure
            {
                'trigger': cls.VALIDATE.value,
                'source': ProcedureState.SETUP.value,
                'dest': ProcedureState.READY.value,
                'before': '_procedure_validate'
            },

            # Start validated process and resume
            {
                'trigger': cls.START.value,
                'source': ProcedureState.READY.value,
                'dest': ProcedureState.RUNNING.value,
                'before': '_procedure_start'
            },
            {
                'trigger': cls.START.value,
                'source': ProcedureState.PAUSED.value,
                'dest': ProcedureState.RUNNING.value,
                'before': '_procedure_resume'
            },

            # Pause
            {
                'trigger': cls.PAUSE.value,
                'source': ProcedureState.RUNNING.value,
                'dest': ProcedureState.PAUSED.value,
                'before': '_procedure_pause'
            },

            {
                'trigger': cls.STOP.value,
                'source': [ProcedureState.RUNNING.value, ProcedureState.PAUSED.value],
                'dest': ProcedureState.SETUP.value,
                'before': '_procedure_stop'
            },
            {
                'trigger': cls.STOP.value,
                'source': ProcedureState.READY.value,
                'dest': ProcedureState.SETUP.value
            },

            {
                'trigger': cls.NEXT.value,
                'source': [ProcedureState.RUNNING.value, ProcedureState.PAUSED.value],
                'dest': '=',
                'before': '_stage_next'
            },
            {
                'trigger': cls.PREVIOUS.value,
                'source': [ProcedureState.RUNNING.value, ProcedureState.PAUSED.value],
                'dest': '=',
                'before': '_stage_previous'
            },
            {
                'trigger': cls.REPEAT.value,
                'source': [ProcedureState.RUNNING.value, ProcedureState.PAUSED.value],
                'dest': '=',
                'before': '_stage_repeat'
            },
            {
                'trigger': cls.FINISH.value,
                'source': [ProcedureState.RUNNING.value, ProcedureState.PAUSED.value],
                'dest': '=',
                'before': '_stage_finish'
            },
            {
                'trigger': cls.GOTO.value,
                'source': [ProcedureState.RUNNING.value, ProcedureState.PAUSED.value],
                'dest': '=',
                'before': '_stage_goto'
            },

            {
                'trigger': cls.ERROR.value,
                'source': '*',
                'dest': ProcedureState.SETUP.value,
                'before': '_handle_error'
            }
        ]
