from __future__ import annotations
import abc
import contextlib
import enum
import queue
import _thread
import threading
import time
import typing

import experimentserver
from .logging import LoggerObject


class LockTimeout(experimentserver.ApplicationException):
    pass


class ThreadLock(LoggerObject):
    def __init__(self, identifier: str, timeout_default: float):
        super(ThreadLock, self).__init__(logger_name_postfix=f":{identifier}")

        self._identifier = identifier

        self._depth = 0
        self._timeout_default = timeout_default
        self._lock = threading.RLock()

    def acquire(self, timeout: typing.Optional[float] = None, quiet: bool = False) -> bool:
        timeout = timeout or self._timeout_default

        # Attempt non-blocking call to get lock
        locked = self._lock.acquire(False)

        if not locked:
            if not quiet:
                self._logger.debug('Waiting for lock')

            # Try again with timeout
            locked = self._lock.acquire(timeout=timeout)

            if not locked:
                raise LockTimeout(f"Unable to acquire lock {self._identifier} before timeout")

        self._depth += 1

        if not quiet:
            self._logger.debug(f"Lock {self._identifier} acquired (depth: {self._depth})")

        return True

    def release(self, quiet: bool = False):
        self._depth -= 1

        if not quiet:
            self._logger.debug(f"Lock {self._identifier} released (depth: {self._depth})")

        self._lock.release()

    def is_held(self):
        # Attempt non-blocking call to get lock
        locked = self._lock.acquire(False)

        if locked:
            # Lock is held, check current depth
            status = self._depth != 0

            # Release current hold on lock
            self._lock.release()

            return status
        else:
            # Lock not held
            return False

    @contextlib.contextmanager
    def lock(self, timeout: typing.Optional[float] = None, quiet: bool = False) -> int:
        # Get lock
        self.acquire(timeout, quiet)

        try:
            yield self._depth
        finally:
            self.release(quiet)


class ThreadException(experimentserver.ApplicationException):
    pass


class ManagedThread(LoggerObject, metaclass=abc.ABCMeta):
    """ Base class for threads with a managed lifecycle. Managed thread can be asked to exit cleanly.

    Note that all managed threads run as daemons, so if the worst happens and everything crashes they are not left
    running in the background (potentially preventing relaunching).
    """
    _thread_instances: typing.List[ManagedThread] = []

    def __init__(self, thread_target: typing.Callable,
                 thread_target_args: typing.Optional[typing.List[typing.Any]] = None,
                 thread_target_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                 thread_name_prefix: typing.Optional[str] = None, thread_name: typing.Optional[str] = None,
                 thread_name_postfix: typing.Optional[str] = None, thread_daemon: bool = False,
                 exception_threshold: typing.Optional[int] = 10, exception_timeout: typing.Optional[float] = 60.0,
                 run_final: bool = False):
        """

        :param thread_target:
        :param thread_target_args:
        :param thread_target_kwargs:
        :param thread_name_prefix:
        :param thread_name:
        :param thread_name_postfix:
        :param thread_daemon:
        :param exception_threshold:
        :param exception_timeout:
        """
        # Thread name
        self._thread_name = (thread_name_prefix or '') + (thread_name or self.__class__.__name__) + \
                            (thread_name_postfix or '')

        super().__init__(logger_name=self._thread_name)

        self._logger.debug(f"Created thread {self._thread_name}")

        # Thread target and arguments
        self._thread_target = thread_target
        self._thread_target_args = thread_target_args or []
        self._thread_target_kwargs = thread_target_kwargs or {}

        self._thread_daemon = thread_daemon

        # Thread exception handling
        self._exception_threshold = exception_threshold
        self._exception_timeout = exception_timeout

        self._run_final = run_final

        # Thread object
        self._thread = threading.Thread(name=self._thread_name, target=self.__thread_target_wrapper,
                                        daemon=True)

        # Add self to list of instances
        self._thread_instances.append(self)

    def is_alive(self) -> bool:
        """

        :return:
        """
        return self._thread.is_alive()

    def start(self) -> typing.NoReturn:
        """

        :return:
        """
        self._logger.debug('Requesting start')
        self._thread.start()

    @abc.abstractmethod
    def stop(self, *args, **kwargs) -> typing.NoReturn:
        """

        :param args:
        :param kwargs:
        :return:
        """
        self._logger.debug('Requesting stop')

    def join(self, timeout: typing.Optional[float] = None):
        self._thread.join(timeout)

    @abc.abstractmethod
    def _test_stop(self) -> bool:
        """

        :return:
        """
        pass

    def _handle_thread_exception(self, exc: Exception):
        pass

    @classmethod
    def join_all(cls, timeout: typing.Optional[float] = None) -> typing.NoReturn:
        """

        :param timeout:
        :return:
        """
        for instance in cls._thread_instances:
            if instance.is_alive():
                if instance._thread_daemon:
                    cls._get_class_logger().debug(f"Ignoring daemon {instance._thread_name}")
                else:
                    cls._get_class_logger().debug(f"Joining {instance._thread_name}")
                    instance._thread.join(timeout)
            else:
                cls._get_class_logger().debug(f"Thread {instance._thread_name} is not running")

    @classmethod
    def stop_all(cls, *args, **kwargs) -> typing.NoReturn:
        """

        :param args:
        :param kwargs:
        :return:
        """
        for instance in cls._thread_instances:
            if instance.is_alive():
                instance.stop(*args, **kwargs)
            else:
                cls._get_class_logger().debug(f"Thread {instance._thread_name} is already stopped")

    def __thread_target_wrapper(self) -> typing.NoReturn:
        self._logger.info('Thread started')

        exception_count = 0
        exception_time = None

        try:
            while not self._test_stop():
                if not threading.main_thread().is_alive():
                    self._logger.error('Main thread has stopped, stopping child thread')
                    break

                # Clear exception counter if timeout has expired
                if exception_time is not None and time.time() > exception_time:
                    self._logger.info('Resetting exception counter')
                    exception_count = 0
                    exception_time = None

                try:
                    self._thread_target(*self._thread_target_args, **self._thread_target_kwargs)
                except Exception as exc:
                    self._handle_thread_exception(exc)

                    if self._exception_threshold is not None:
                        # Handle exception threshold
                        exception_count += 1

                        self._logger.exception(f"Unhandled exception in thread ({exception_count} of "
                                               f"{self._exception_threshold} allowed)")

                        exception_time = time.time() + self._exception_timeout

                        if exception_count >= self._exception_threshold:
                            self._logger.error(f"Number of exceptions exceeds configured threshold "
                                               f"({self._exception_threshold}), interrupting main thread")

                            _thread.interrupt_main()

                            self._logger.error('Thread halted')

                            return
                    else:
                        self._logger.exception('Unhandled exception in thread')
        finally:
            # Final call to target function (allows for cleanup)
            if self._run_final:
                try:
                    self._thread_target(*self._thread_target_args, **self._thread_target_kwargs)
                except Exception:
                    self._logger.exception('Unhandled exception in thread during shutdown')

            self._logger.info('Thread stopped')


class CallbackThread(ManagedThread):
    def __init__(self, name: str, callback: typing.Callable,
                 callback_args: typing.Optional[typing.List[typing.Any]] = None,
                 callback_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None, thread_daemon: bool = False,
                 run_final: bool = False):
        super().__init__(thread_name_postfix=':' + name, thread_target=callback, thread_target_args=callback_args or [],
                         thread_target_kwargs=callback_kwargs or {}, thread_daemon=thread_daemon, run_final=run_final)

        # Thread stop flag
        self._thread_stop = threading.Event()

    def start(self) -> typing.NoReturn:
        # Clear existing stop flag if any
        self._thread_stop.clear()

        super().start()

    def stop(self, *args, **kwargs) -> typing.NoReturn:
        super().stop()

        self._thread_stop.set()

    def _test_stop(self) -> bool:
        return self._thread_stop.is_set()


class _QueueCommand(enum.IntEnum):
    STOP = 0
    PROCESS = 1
    FINISH = 2


class _QueueEntry:
    def __init__(self, command: _QueueCommand, data: typing.Optional[typing.Any]):
        self.command = command
        self.data = data

    def __lt__(self, other):
        return self.command.value < other.command.value

    def __str__(self):
        return f"({self.command.name}, {self.data})"


class QueueThread(ManagedThread):
    QUEUE_TIMEOUT = 1

    def __init__(self, name: str, event_callback: typing.Callable,
                 event_callback_args: typing.Optional[typing.List[typing.Any]] = None,
                 event_callback_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None):
        super().__init__(thread_name_postfix=':' + name, thread_target=self._thread_queue, run_final=True)

        # Event callback
        self._event_callback = event_callback
        self._event_callback_args = event_callback_args or []
        self._event_callback_kwargs = event_callback_kwargs or {}

        # Queue for command objects
        self._queue = queue.PriorityQueue()

        # Thread stop flag
        self._thread_stop = False

    def stop(self, *args, **kwargs) -> typing.NoReturn:
        self._append(_QueueCommand.FINISH)

    def _test_stop(self) -> bool:
        return self._thread_stop

    def _handle_thread_exception(self, exc: Exception):
        # Raise exception, any exceptions caused by event handlers should be handled in wrapper
        raise exc

    def append(self, obj):
        if not self._thread.is_alive():
            raise ThreadException('Queue thread is not running')

        self._append(_QueueCommand.PROCESS, obj)

    def _append(self, obj_type: _QueueCommand, obj=None):
        # Insert into priority queue
        self._queue.put(_QueueEntry(obj_type, obj))

    def _thread_queue(self):
        while not self._queue.empty() or not self._thread_stop:
            try:
                # Get event from queue
                queue_entry = self._queue.get(timeout=self.QUEUE_TIMEOUT)

                if queue_entry.command is _QueueCommand.STOP:
                    self._logger.info('Immediate stop requested')
                    self._thread_stop = True
                    break
                elif queue_entry.command is _QueueCommand.PROCESS:
                    # Pass object to callback
                    self._event_callback(queue_entry.data, *self._event_callback_args, **self._event_callback_kwargs)
                elif queue_entry.command is _QueueCommand.FINISH:
                    self._logger.info('Delayed stop requested')

                    # Send empty call to callback to finalise process
                    try:
                        self._event_callback(None, *self._event_callback_args, **self._event_callback_kwargs)
                    except Exception:
                        self._logger.exception(f"Unhandled exception in queue callback when processing cleanup")
                    finally:
                        self._thread_stop = True
            except queue.Empty:
                pass


def stop_all(join: bool = True, timeout: typing.Optional[float] = None):
    ManagedThread.stop_all()

    if join:
        ManagedThread.join_all(timeout)
