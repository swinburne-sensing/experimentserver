from __future__ import annotations

import abc
import enum
import typing

import transitions

import experimentserver
from .lock import LockTimeout, MonitoredCondition
from .thread import CallbackThread


class ManagedStateMachineError(experimentserver.ApplicationException):
    pass


class TransitionDiscarded(ManagedStateMachineError):
    """ Error raised when pending transition queue is emptied. """
    pass


class ManagedState(enum.Enum):
    """  """
    pass


class ManagedTransition(enum.Enum):
    """  """
    def apply(self, model: typing.Any, *args: typing.Any, **kwargs: typing.Any) -> bool:
        """ Apply transition to provided model.

        :param model:
        :return:
        """
        # Fetch method
        method = getattr(model, self.value)

        return method(*args, **kwargs)

    @classmethod
    @abc.abstractmethod
    def get_transitions(cls) -> typing.List[typing.Dict[str, typing.Union[str, typing.Sequence[str]]]]:
        """

        :return:
        """
        raise NotImplementedError()


# Type hinting definitions
TYPE_STATE = typing.TypeVar('TYPE_STATE', bound=ManagedState)
TYPE_TRANSITION = typing.TypeVar('TYPE_TRANSITION', bound=ManagedTransition)


class PendingStateTransition(experimentserver.ApplicationException):
    def __init__(self, current_state: TYPE_STATE, pending_states: typing.Sequence[TYPE_TRANSITION]):
        super().__init__()

        self.current_state = current_state
        self.pending_states = pending_states


class _SupportsState(typing.Protocol, typing.Generic[TYPE_STATE]):
    state: TYPE_STATE


class ManagedStateMachine(typing.Generic[TYPE_STATE, TYPE_TRANSITION], CallbackThread):
    """  """

    def __init__(self, name: str, model: _SupportsState[TYPE_STATE],
                 state_type: typing.Type[TYPE_STATE], transition_type: typing.Type[TYPE_TRANSITION],
                 initial_state: TYPE_STATE, queued_transitions: bool = False):
        """

        :param model:
        :param state_type:
        :param transition_type:
        :param initial_state:
        :param queued_transitions:
        """
        CallbackThread.__init__(self, name, self._thread_manager, run_final=True)
        
        # Store model and types
        self._model = model
        self._state_type = state_type
        self._transition_type = transition_type

        # State lock
        self._state_condition = MonitoredCondition(name + '.state', 10.0)

        # Queue for pending transitions
        self._transition_pending_queue: typing.List[
            typing.Tuple[
                TYPE_TRANSITION,
                typing.Tuple[typing.Any, ...],
                typing.Dict[str, typing.Any]
            ]
        ] = []

        # Queue for resulting errors
        self._transition_error_queue: typing.List[Exception] = []

        # State machine
        self._machine = transitions.Machine(model=model, states=[x.value for x in self._state_type],
                                            transitions=self._transition_type.get_transitions(),
                                            initial=initial_state.value, send_event=True, queued=queued_transitions)

    def _get_state(self) -> TYPE_STATE:
        return self._state_type(self._model.state)

    def get_state(self, timeout: typing.Optional[float] = None, raise_pending: bool = False, wait_pending: bool = True) \
            -> TYPE_STATE:
        """

        :return:
        """
        if not self.is_thread_alive():
            raise ManagedStateMachineError('State machine thread not running')

        with self._state_condition.lock(timeout, frame_offset=1, reason='get_state') as state_lock_request:
            if len(self._transition_pending_queue) > 0:
                if raise_pending:
                    raise PendingStateTransition(self._get_state(), [x[0] for x in self._transition_pending_queue])

                if wait_pending:
                    # Wait for pending transitions to be applied
                    state_lock_request.wait_for(lambda: len(self._transition_pending_queue) == 0, timeout)

            return self._get_state()
    
    def get_state_str(self, timeout: typing.Optional[float] = None) -> str:
        try:
            state = self.get_state(timeout, raise_pending=True)
            return str(state.value)
        except PendingStateTransition as exc:
            pending_states = [f"{state.value!s}" for state in exc.pending_states]
            return f"{exc.current_state.value!s} -> {' -> '.join(pending_states)}"
        except ManagedStateMachineError:
            return 'Software Error'
        except LockTimeout:
            return 'unknown'

    def get_error(self, timeout: typing.Optional[float] = None, raise_exception: bool = True) \
            -> typing.Optional[Exception]:
        with self._state_condition.lock(timeout):
            # Wait while transitions are pending
            self.get_state()

            if len(self._transition_error_queue) > 0:
                # Empty exception queue then raise exception
                exc_list = self._transition_error_queue.copy()
                self._transition_error_queue.clear()

                if len(exc_list) == 1:
                    if raise_exception:
                        raise exc_list[0]
                    else:
                        return exc_list[0]
                else:
                    multi_exc = experimentserver.MultipleException(
                        'Multiple exceptions thrown while processing manager transition',
                        exc_list
                    )

                    if raise_exception:
                        raise multi_exc
                    else:
                        return multi_exc

        return None

    def clear_transition(self) -> None:
        """ Clear pending transition queue.

        :return:
        """
        with self._state_condition.lock() as state_lock_request:
            # Clear pending errors
            self._transition_error_queue.clear()

            # Clear pending transitions
            while len(self._transition_pending_queue) > 0:
                transition = self._transition_pending_queue.pop(0)

                # Notify waiting threads that pending transitions were discarded
                self._transition_error_queue.append(TransitionDiscarded(f"Transition {transition} discarded"))
            
            state_lock_request.notify_all()

    def queue_transition(self, transition: TYPE_TRANSITION, *args: typing.Any, block: bool = True,
                         timeout: typing.Optional[float] = None, raise_exception: bool = True, **kwargs: typing.Any) \
            -> typing.Optional[ManagedState]:
        """ Queue transition for manager machine.

        :param transition:
        :param block:
        :param timeout:
        :param raise_exception:
        """
        if not self.is_thread_alive():
            raise ManagedStateMachineError('State machine thread not running')

        with self._state_condition.lock():
            self.logger().debug(f"Queueing transition {transition} ({'blocking' if block else 'non-blocking'})")
            self._transition_pending_queue.append((transition, args, kwargs))

            if block:
                # Wait for notification of completion (this also blocks waiting for the transition to occur)
                error = self.get_error(timeout, raise_exception)

                if error is not None:
                    self.logger().warning(f"Exception thrown during transition", exc_info=error)

                return self.get_state()
            else:
                return None

    @abc.abstractmethod
    def _thread_manager(self) -> None:
        """ Thread code for handling machine state. No locks are held upon entry! """
        pass

    def _handle_transition_exception(self, initial_state: TYPE_STATE, transition: TYPE_TRANSITION, exc: Exception) \
            -> None:
        self._transition_error_queue.append(exc)

        # Clear pending transitions
        while len(self._transition_pending_queue) > 0:
            (queued_transition, _, _) = self._transition_pending_queue.pop(0)
            self.logger().warning(f"Discarding pending transition {queued_transition.value}")

    def _process_transition(self) -> typing.Tuple[bool, TYPE_STATE]:
        """ Handle pending transitions.

        :return: tuple containing True when state has changed, false otherwise; and current state
        """
        state_change = False

        with self._state_condition.lock(silent=True) as state_lock_request:
            # Fetch current state
            state = self._get_state()

            if len(self._transition_pending_queue) > 0:
                try:
                    state_queue = [state]

                    while len(self._transition_pending_queue) > 0:
                        # Get pending transition
                        (queued_transition, queued_args, queued_kwargs) = self._transition_pending_queue.pop(0)
                        queued_args = queued_args
                        queued_args_str = ', '.join(map(str, queued_args))
                        queued_kwargs = queued_kwargs
                        queued_kwargs_str = ', '.join((f"{k}={v}" for k, v in queued_kwargs.items()))

                        self.logger().info(f"Performing transition {queued_transition.value} from state {state.value} "
                                           f"(args: {queued_args_str}, kwargs: {queued_kwargs_str})")

                        # Attempt to apply requested transition
                        try:
                            queued_transition.apply(self._model, *queued_args, **queued_kwargs)
                        except (transitions.MachineError, experimentserver.ApplicationException) as exc:
                            # Pass to handler
                            self._handle_transition_exception(state, queued_transition, exc)
                        except Exception as exc:
                            # Pass to handler
                            self._handle_transition_exception(state, queued_transition, exc)

                            raise
                        finally:
                            current_state = self._get_state()

                            if current_state != state_queue[-1]:
                                state_queue.append(current_state)
                finally:
                    if len(state_queue) > 1:
                        self.logger().info(f"State updated {' -> '.join((x.value for x in state_queue))}")
                        state_change = True
                    else:
                        self.logger().debug('State unchanged')

                    # Notify any waiting threads
                    state_lock_request.notify_all()

        return state_change, state
