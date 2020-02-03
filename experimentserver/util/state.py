from __future__ import annotations

import abc
import enum
import threading
import typing

import transitions

import experimentserver
from .thread import CallbackThread


class ManagedState(enum.Enum):
    """  """
    pass


class ManagedTransition(enum.Enum):
    """  """
    def apply(self, model, *args, **kwargs) -> bool:
        """ Apply transition to provided model.

        :param model:
        :return:
        """
        # Fetch method
        method = getattr(model, self.value)

        return method(*args, **kwargs)

    @classmethod
    @abc.abstractmethod
    def get_transitions(cls) -> typing.List[typing.Dict[str, str]]:
        """

        :return:
        """
        raise NotImplementedError()


class ManagedStateMachine(CallbackThread):
    """  """

    def __init__(self, name: typing.Optional[str], model: object, state_type: typing.Type[TYPE_STATE],
                 transition_type: typing.Type[TYPE_TRANSITION], initial_state: TYPE_STATE,
                 queued_transitions: bool = False):
        """

        :param model:
        :param state_type:
        :param transition_type:
        :param initial_state:
        :param queued_transitions:
        """
        super(ManagedStateMachine, self).__init__(name, self._thread_manager, run_final=True)
        
        # Store model and types
        self._model = model
        self._state_type = state_type
        self._transition_type = transition_type

        # State lock
        self._state_lock = threading.Condition()

        # Queue for pending transitions
        self._transition_pending_queue: typing.List[typing.Tuple[TYPE_TRANSITION, typing.Sequence, typing.Mapping]] = []

        # Queue for resulting errors
        self._transition_error_queue: typing.List[Exception] = []

        # State machine
        self._machine = transitions.Machine(model=model, states=[x.value for x in self._state_type],
                                            transitions=self._transition_type.get_transitions(),
                                            initial=initial_state.value, send_event=True, queued=queued_transitions)

    def _get_state(self) -> TYPE_STATE:
        return self._state_type(self._model.state)

    def get_state(self, timeout: typing.Optional[float] = None) -> TYPE_STATE:
        """

        :return:
        """
        with self._state_lock:
            # Wait for pending transitions to be applied
            while len(self._transition_pending_queue) > 0:
                self._state_lock.wait(timeout)

            return self._get_state()

    def get_error(self, timeout: typing.Optional[float] = None, raise_exception: bool = True) \
            -> typing.Optional[Exception]:
        with self._state_lock:
            while len(self._transition_pending_queue) > 0:
                self._state_lock.wait(timeout)

            if len(self._transition_error_queue) > 0:
                # Clear exception queue then raise exception
                exc = self._transition_error_queue.copy()
                self._transition_error_queue.clear()

                if len(exc) == 1:
                    exc = exc[0]
                else:
                    exc = experimentserver.MultipleException('Multiple exceptions thrown while processing manager '
                                                             'transition', exc)

                if raise_exception:
                    raise exc
                else:
                    return exc

        return None

    def queue_transition(self, transition: TYPE_TRANSITION, *args, block: bool = True,
                         timeout: typing.Optional[float] = None, raise_exception: bool = True, **kwargs) \
            -> typing.Optional[TYPE_STATE]:
        """ Queue transition for manager machine.

        :param transition:
        :param block:
        :param timeout:
        :param raise_exception:
        """
        with self._state_lock:
            self.get_logger().info(f"Queueing transition {transition}")
            self._transition_pending_queue.append((transition, args, kwargs))

            if block:
                # Wait for notification of completion
                error = self.get_error(timeout, raise_exception)

                if error is not None:
                    self.get_logger().warning(f"Exception thrown during transition", exc_info=error)

                return self.get_state()
            else:
                return None

    @abc.abstractmethod
    def _thread_manager(self) -> typing.NoReturn:
        """

        """
        pass

    def _handle_transition_exception(self, initial_state: TYPE_STATE, transition: TYPE_TRANSITION, exc: Exception):
        self._transition_error_queue.append(exc)

        # Clear pending transitions
        while len(self._transition_pending_queue) > 0:
            (queued_transition, _, _) = self._transition_pending_queue.pop(0)
            self.get_logger().warning(f"Discarding pending transition {queued_transition.value}")

    def _process_transition(self) -> typing.Tuple[bool, ManagedState]:
        """

        :return:
        """
        state_change = False

        with self._state_lock:
            # Fetch current manager
            state = self._get_state()

            if len(self._transition_pending_queue) > 0:
                try:
                    state_queue = [state]

                    while len(self._transition_pending_queue) > 0:
                        # Get pending transition
                        (queued_transition, queued_args, queued_kwargs) = self._transition_pending_queue.pop(0)
                        queued_args = queued_args or []
                        queued_kwargs = queued_kwargs or {}

                        self.get_logger().info(f"Performing transition {queued_transition.value} from {state.value} "
                                               f"(args: {queued_args}, kwargs: {queued_kwargs})")

                        # Attempt to apply requested transition
                        try:
                            queued_transition.apply(self._model, *queued_args, **queued_kwargs)
                        except (transitions.MachineError, experimentserver.ApplicationException) as exc:
                            # Pass to handler
                            self._handle_transition_exception(state, queued_transition, exc)
                        finally:
                            current_state = self._get_state()

                            if current_state != state_queue[-1]:
                                state_queue.append(current_state)
                finally:
                    if len(state_queue) > 1:
                        self.get_logger().info(f"State updated {' -> '.join((x.value for x in state_queue))}")
                        state_change = True
                    else:
                        self.get_logger().debug('State unchanged')

                    state = current_state

                    # Notify any waiting threads
                    self._state_lock.notify_all()

            return state_change, state


# Type hinting definitions
TYPE_STATE = typing.TypeVar('TYPE_STATE', bound=ManagedState)
TYPE_TRANSITION = typing.TypeVar('TYPE_TRANSITION', bound=ManagedTransition)
