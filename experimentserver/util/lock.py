from __future__ import annotations

import enum
import os
import threading
import sys
import typing
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import FrameType

from experimentlib.logging.classes import Logged
from experimentlib.util.time import now

import experimentserver


class LockTimeout(experimentserver.ApplicationException):
    pass


def _format_list(in_list: typing.List[typing.Any]) -> str:
    if len(in_list) == 0:
        return '<none>'
    else:
        return ', '.join(map(str, in_list))


TMonitoredLock = typing.TypeVar('TMonitoredLock', bound='_MonitoredLock')
TRequest = typing.TypeVar('TRequest', bound='_Request')


class _LockState(enum.Enum):
    REQUESTED = 'requested'
    LOCKED = 'lock'
    WAITING = 'wait'
    RELEASED = 'unlock'
    TIMEOUT_REQUEST = 'timeout_request'
    TIMEOUT_WAIT = 'timeout_wait'

    @property
    def is_held(self) -> bool:
        return self == self.LOCKED

    @property
    def is_requested(self) -> bool:
        return self == self.REQUESTED or self == self.WAITING


class _MonitoredLock(typing.Generic[TRequest], Logged):
    def __init__(self, request_cls: typing.Type[TRequest], name: str, timeout: float, silent: bool = False):
        super().__init__(name)

        self._request_cls = request_cls
        self._name = name
        self._timeout = timeout
        self._silent = silent

        # Wrapped lock
        self._lock = threading.RLock()

        # Internal state
        self._internal_lock = threading.RLock()
        self._lock_requests: typing.Dict[int, TRequest] = {}
    
    @property
    def name(self) -> str:
        return self._name

    @property
    def timeout(self) -> float:
        return self._timeout

    @property
    def silent(self) -> bool:
        return self._silent
    
    @property
    def is_held(self) -> bool:
        # Attempt non-blocking call to get lock
        if self._lock.acquire(False):
            # Got lock Lock is held, release and return
            self._lock.release()

            return False
        else:
            # Lock held
            return True
    
    @property
    def holders(self) -> typing.List[TRequest]:
        with self._internal_lock:
            return list(req for req in self._lock_requests.values() if req.state.is_held)
        
    @property
    def requests(self) -> typing.List[TRequest]:
        with self._internal_lock:
            return list(req for req in self._lock_requests.values() if req.state.is_requested)

    @property
    def all_requests(self) -> typing.List[TRequest]:
        with self._internal_lock:
            return list(self._lock_requests.values())
    
    def log(self, msg: str) -> None:
        if not self.silent:
            self.logger().lock(msg)
    
    def acquire(self, timeout: typing.Optional[float] = None, silent: typing.Optional[bool] = None,
                frame_offset: int = 0, **kwargs: typing.Any) -> TRequest:
        timeout = timeout or self._timeout
        silent = silent or self._silent

        # Create request
        request = self._request_cls.from_caller(self, silent, frame_offset=frame_offset + 1, **kwargs)

        with self._internal_lock:
            self._lock_requests[request.uid] = request

        # Attempt (fast) non-blocking call to get lock
        if not self._lock.acquire(False):
            if not silent:
                with self._internal_lock:
                    self.log(f"{request.caller} waiting ({timeout} s) for lock held by {_format_list(self.holders)} (requests: {_format_list(self.requests)})")

            if not self._lock.acquire(timeout=timeout):
                request.state = _LockState.TIMEOUT_REQUEST
                raise LockTimeout(
                    f"{request.caller} timed out ({timeout} s) getting {self!s} held by {', '.join(map(str, self.holders))} (requests: {_format_list(self.requests)})"
                )

        with self._internal_lock:
            # Update request
            request.state = _LockState.LOCKED
            request.metadata['locked'] = now()
        
        if not silent:
            self.log(f"Locked by {request.caller}")

        return request

    def release(self, request: TRequest, silent: typing.Optional[bool] = None) -> None:
        silent = silent or self._silent

        with self._internal_lock:
            # Unlock now, will raise exception if lock not actuall held
            self._lock.release()

            # Pull internal request off stack
            internal_req = self._lock_requests.pop(request.uid)

            # Update state and metadata
            internal_req.state = request.state = _LockState.RELEASED
            internal_req.metadata['unlocked'] = request.metadata['unlocked'] = now()

            if not silent:
                self.logger().lock(f"Unlocked by {internal_req.caller}")
    
    def __enter__(self) -> typing.NoReturn:
        raise NotImplementedError()

    @contextmanager
    def lock(self, timeout: typing.Optional[float] = None, silent: typing.Optional[bool] = None, frame_offset: int = 0, **kwargs: typing.Any) -> typing.Generator[TRequest, None, None]:
        # Get lock
        request = self.acquire(timeout, silent, frame_offset=frame_offset + 2, **kwargs)

        try:
            yield request
        finally:
            self.release(request, silent)


@dataclass
class _Request(typing.Generic[TMonitoredLock]):
    parent: TMonitoredLock
    state: _LockState
    caller_thread: threading.Thread
    caller_frame: FrameType

    metadata: typing.Dict[str, typing.Any] = field(default_factory=dict)
    silent: bool = field(default=False)

    _request_uid: typing.ClassVar[int] = 0
    _request_uid_lock: typing.ClassVar[threading.Lock] = threading.Lock()

    def __post_init__(self) -> None:
        self.uid = self._next_uid()
        self.metadata['created'] = now()
    
    @property
    def age(self) -> timedelta:
        assert isinstance(self.metadata['created'], datetime)
        return datetime.now() - self.metadata['created']
    
    @property
    def caller(self) -> str:
        filename = self.caller_frame.f_code.co_filename

        if experimentserver.APP_PATH in filename:
            filename = filename.split(experimentserver.APP_PATH, 1)[1][1:].replace(os.pathsep, '.')
        else:
            filename = os.path.basename(filename)

        return f"{self.caller_thread.name} {self.caller_frame.f_code.co_name}@{filename}:{self.caller_frame.f_lineno}"
    
    def __str__(self) -> str:
        meta_str_list = [f"{k}={v}" for k, v in self.metadata.items() if isinstance(v, str)]
        meta_str = ', '.join(meta_str_list) or '<none>'
        return f"#{self.uid} {self.caller} ({self.state.name} {meta_str})"

    def release(self) -> None:
        self.parent.release(self, self.silent)

    @classmethod
    def _next_uid(cls) -> int:
        with cls._request_uid_lock:
            cls._request_uid += 1
            return cls._request_uid

    @classmethod
    def from_caller(cls: typing.Type[TRequest], parent: TMonitoredLock, silent: bool = False,
                    state: _LockState = _LockState.REQUESTED, frame_offset: int = 0, **kwargs: typing.Any) -> TRequest:
        thread = threading.current_thread()
        caller = sys._getframe(frame_offset + 1)

        return cls(
            parent=parent,
            state=state,
            caller_thread=thread,
            caller_frame=caller,
            metadata=kwargs,
            silent=silent
        )


class MonitoredLock(_MonitoredLock['LockRequest']):
    def __init__(self, name: str, timeout: float, silent: bool = False):
        super().__init__(LockRequest, name, timeout, silent)
    
    def __str__(self) -> str:
        return f"Lock {self.name}"


@dataclass
class LockRequest(_Request[MonitoredLock]):
    pass


class MonitoredCondition(_MonitoredLock['ConditionRequest']):
    def __init__(self, name: str, timeout: float, silent: bool = False):
        super().__init__(ConditionRequest, name, timeout, silent)

        self._condition = threading.Condition(self._lock)
    
    @property
    def waiters(self) -> typing.List[ConditionRequest]:
        with self._internal_lock:
            return list(req for req in self._lock_requests.values() if req.state == _LockState.WAITING)
    
    def __str__(self) -> str:
        return f"Condition {self.name}"


@dataclass
class ConditionRequest(_Request[MonitoredCondition]):
    def notify(self, n: int) -> None:
        self.parent.log(f"{self!s} notified {n} thread(s) (waiting: {_format_list(self.parent.waiters)})")
        self.parent._condition.notify(n)

    def notify_all(self) -> None:
        self.parent.log(f"{self!s} notified all thread(s) (waiting: {_format_list(self.parent.waiters)})")
        self.parent._condition.notify_all()

    def wait(self, timeout: typing.Optional[float] = None) -> None:
        if timeout is None:
            timeout = self.parent.timeout

        self.parent.log(f"{self!s} waiting")

        self.state = _LockState.WAITING

        if not self.parent._condition.wait(timeout):
            self.state = _LockState.TIMEOUT_WAIT
            raise LockTimeout(
                f"{self!s} timed out ({timeout} s) waiting for {self.parent!s} (all requests: {_format_list(self.parent.all_requests)})"
            )

    def wait_for(self, predicate: typing.Callable[[], bool], timeout: typing.Optional[float] = None) -> None:
        if timeout is None:
            timeout = self.parent.timeout

        self.parent.log(f"{self!s} waiting for test")
        self.state = _LockState.WAITING

        if not self.parent._condition.wait_for(predicate, timeout):
            self.state = _LockState.TIMEOUT_WAIT
            raise LockTimeout(
                f"{self!s} timed out ({timeout} s) waiting for lock (all requests: {_format_list(self.parent.all_requests)})"
            )
