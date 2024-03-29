from __future__ import annotations
import abc
import atexit
import contextlib
import enum
import queue
import _thread
import sys
import threading
import time
import traceback
import typing
from io import StringIO

from experimentlib.logging.classes import LoggedAbstract

import experimentserver


class ThreadException(experimentserver.ApplicationException):
    pass


class ManagedThread(LoggedAbstract):
    """ Base class for threads with a managed lifecycle. Managed thread can be asked to exit cleanly.

    Note that all managed threads run as daemons, so if the worst happens and everything crashes they are not left
    running in the background (potentially holding resources, preventing relaunching).
    """
    _thread_instances: typing.List[ManagedThread] = []

    def __init__(self, thread_target: typing.Callable[..., None],
                 thread_target_args: typing.Optional[typing.List[typing.Any]] = None,
                 thread_target_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                 thread_name_prefix: typing.Optional[str] = None, thread_name: typing.Optional[str] = None,
                 thread_name_postfix: typing.Optional[str] = None, thread_daemon: bool = False,
                 exception_threshold: typing.Optional[int] = 3, exception_timeout: typing.Optional[float] = 30.0,
                 run_final: bool = False):
        """ Instantiate a managed thread.

        :param thread_target: target callable to run in thread
        :param thread_target_args: arguments passed to target callable when run
        :param thread_target_kwargs: keyword arguments passed to target callable when run
        :param thread_name_prefix:
        :param thread_name:
        :param thread_name_postfix:
        :param thread_daemon: if True then run thread as daemon
        :param exception_threshold: allowable number of unhandled exceptions before killing thread
        :param exception_timeout: timeout before exception counter is reset
        """
        # Thread name
        self._thread_name = (thread_name_prefix or '') + (thread_name or self.__class__.__name__) + \
                            (thread_name_postfix or '')

        LoggedAbstract.__init__(self, self._thread_name)

        self.logger().debug(f"Created thread {self._thread_name}")

        # Thread target and arguments
        self._thread_target = thread_target
        self._thread_target_args = thread_target_args or []
        self._thread_target_kwargs = thread_target_kwargs or {}

        self._thread_daemon = thread_daemon

        # Thread exception handling
        if exception_threshold is None != exception_timeout is None:
            raise ValueError('exception_threshold and exception_timeout must both be None or provide values')

        self._exception_threshold = exception_threshold
        self._exception_timeout = exception_timeout

        self._run_final = run_final

        # Thread object
        self._thread = threading.Thread(name=self._thread_name, target=self.__thread_target_wrapper, daemon=True)

        # Add self to list of instances
        self._thread_instances.append(self)

    def is_thread_alive(self) -> bool:
        """

        :return:
        """
        return self._thread.is_alive()

    def thread_start(self) -> None:
        """

        :return:
        """
        self.logger().info('Managed thread starting')
        self._thread.start()

    @abc.abstractmethod
    def thread_stop(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """

        :param args:
        :param kwargs:
        :return:
        """
        self.logger().info('Managed thread stopping')

    def thread_join(self, timeout: typing.Optional[float] = None) -> None:
        if self.is_thread_alive():
            self._thread.join(timeout)

    @abc.abstractmethod
    def thread_stop_requested(self) -> bool:
        """

        :return:
        """
        pass

    def _handle_thread_exception(self, exc: Exception) -> None:
        pass

    @classmethod
    def join_all_thread(cls, timeout: typing.Optional[float] = None) -> None:
        """

        :param timeout:
        :return:
        """
        for instance in cls._thread_instances:
            if instance.is_thread_alive():
                if instance._thread_daemon:
                    cls.logger().debug(f"Ignoring daemon {instance._thread_name}")
                else:
                    cls.logger().debug(f"Joining {instance._thread_name}")

                    if instance._thread.ident is not None:
                        try:
                            str_io = StringIO()
                            frame = sys._current_frames()[instance._thread.ident]
                            traceback.print_stack(frame, file=str_io)

                            cls.logger().trace(f"Thread traceback: {str_io.getvalue()!s}")
                        except KeyError:
                            cls.logger().debug(f"Couldn't get {instance._thread_name} trace")

                    instance._thread.join(timeout)
            else:
                cls.logger().debug(f"Thread {instance._thread_name} is not running")

    @classmethod
    def stop_all_thread(cls, *args: typing.Any, **kwargs: typing.Any) -> None:
        """

        :param args:
        :param kwargs:
        :return:
        """
        for instance in cls._thread_instances:
            if instance.is_thread_alive():
                instance.thread_stop(*args, **kwargs)
            else:
                cls.logger().debug(f"Thread {instance._thread_name} is already stopped")

    def __thread_target_wrapper(self) -> None:
        self.logger().info('Thread started')

        exception_count = 0
        exception_time = None

        try:
            while not self.thread_stop_requested():
                if not threading.main_thread().is_alive():
                    self.logger().error('Main thread has stopped, stopping child thread')
                    break

                # Clear exception counter if timeout has expired
                if exception_time is not None and time.time() > exception_time:
                    self.logger().info('Resetting exception counter')
                    exception_count = 0
                    exception_time = None

                try:
                    self._thread_target(*self._thread_target_args, **self._thread_target_kwargs)
                except experimentserver.ApplicationException as exc:
                    # Raise fatal exceptions
                    if exc.fatal:
                        # Disable final run
                        self._run_final = False

                        raise

                    self._handle_thread_exception(exc)

                    if self._exception_threshold is not None:
                        assert self._exception_timeout is not None

                        # Handle exception threshold
                        exception_count += 1

                        self.logger().exception(f"Unhandled application exception in thread ({exception_count} of "
                                                f"{self._exception_threshold} allowed)")

                        exception_time = time.time() + self._exception_timeout

                        if exception_count >= self._exception_threshold:
                            self.logger().error(f"Number of application exceptions exceeds configured threshold "
                                                f"({self._exception_threshold}), interrupting main thread")

                            _thread.interrupt_main()

                            self.logger().error('Thread halted')

                            raise
                    else:
                        self.logger().exception('Unhandled application exception in thread')
                except Exception:
                    # Disable final run
                    self._run_final = False

                    self.logger().exception('Unhandled base exception in thread')
                    raise
        finally:
            # Final call to target function (allows for cleanup)
            if self._run_final:
                # noinspection PyBroadException
                try:
                    self._thread_target(*self._thread_target_args, **self._thread_target_kwargs)
                except Exception:
                    self.logger().exception('Unhandled exception in thread during shutdown')

            self.logger().info('Thread stopped')


class CallbackThread(ManagedThread):
    """  """

    def __init__(self, name: typing.Optional[str], callback: typing.Callable[..., None],
                 callback_args: typing.Optional[typing.List[typing.Any]] = None,
                 callback_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None, thread_daemon: bool = False,
                 run_final: bool = False):
        """

        :param name:
        :param callback:
        :param callback_args:
        :param callback_kwargs:
        :param thread_daemon:
        :param run_final:
        """
        if name is not None:
            name = ':' + name

        ManagedThread.__init__(self, callback, callback_args or [], callback_kwargs or {}, thread_name_postfix=name,
                               thread_daemon=thread_daemon, run_final=run_final)

        # Thread stop flag
        self._thread_stop = threading.Event()

    def thread_start(self) -> None:
        # Clear existing stop flag if any
        self._thread_stop.clear()

        super().thread_start()

    def thread_stop(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        super().thread_stop()

        self._thread_stop.set()

    def thread_stop_requested(self) -> bool:
        return self._thread_stop.is_set()


class _QueueCommand(enum.IntEnum):
    STOP = 0
    PROCESS = 1
    FINISH = 2


class _QueueEntry:
    def __init__(self, command: _QueueCommand, data: typing.Optional[typing.Any]):
        self.command = command
        self.data = data

    def __lt__(self, other: typing.Any) -> bool:
        assert isinstance(other, _QueueEntry)
        return self.command.value < other.command.value

    def __str__(self) -> str:
        return f"({self.command.name}, {self.data})"


class QueueThread(ManagedThread):
    QUEUE_TIMEOUT = 1

    def __init__(self, name: str, event_callback: typing.Callable[..., None],
                 event_callback_args: typing.Optional[typing.List[typing.Any]] = None,
                 event_callback_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None):
        super().__init__(thread_name_postfix=':' + name, thread_target=self._thread_queue, run_final=True)

        # Event callback
        self._event_callback = event_callback
        self._event_callback_args = event_callback_args or []
        self._event_callback_kwargs = event_callback_kwargs or {}

        # Queue for command objects
        self._queue: queue.PriorityQueue[_QueueEntry] = queue.PriorityQueue()

        # Thread stop flag
        self._thread_stop = False

    def thread_stop(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        self._append(_QueueCommand.FINISH)

    def thread_stop_requested(self) -> bool:
        return self._thread_stop

    def _handle_thread_exception(self, exc: Exception) -> typing.NoReturn:
        # Raise exception, any exceptions caused by event handlers should be handled in wrapper
        raise exc

    def append(self, obj: typing.Any) -> None:
        """ Append data to process to the command queue.

        :param obj:
        :return:
        """
        if not self._thread.is_alive():
            raise ThreadException('Queue thread is not running')

        self._append(_QueueCommand.PROCESS, obj)

    def _append(self, obj_type: _QueueCommand, obj: typing.Optional[typing.Any] = None) -> None:
        # Insert into priority queue
        self._queue.put(_QueueEntry(obj_type, obj))

    def _thread_queue(self) -> None:
        while not self._queue.empty() or not self._thread_stop:
            try:
                # Get event from queue
                queue_entry = self._queue.get(timeout=self.QUEUE_TIMEOUT)

                if queue_entry.command is _QueueCommand.STOP:
                    self.logger().info('Immediate stop requested')
                    self._thread_stop = True
                    break
                elif queue_entry.command is _QueueCommand.PROCESS:
                    # Pass object to callback
                    self._event_callback(queue_entry.data, *self._event_callback_args, **self._event_callback_kwargs)
                elif queue_entry.command is _QueueCommand.FINISH:
                    self.logger().info('Delayed stop requested')

                    # Send empty call to callback to finalise process
                    # noinspection PyBroadException
                    try:
                        self._event_callback(None, *self._event_callback_args, **self._event_callback_kwargs)
                    except Exception:
                        self.logger().exception(f"Unhandled exception in queue callback when processing cleanup")
                    finally:
                        self._thread_stop = True
            except queue.Empty:
                pass


def stop_all(join: bool = True, timeout: typing.Optional[float] = None) -> None:
    ManagedThread.stop_all_thread()

    if join:
        ManagedThread.join_all_thread(timeout)


atexit.register(stop_all)
