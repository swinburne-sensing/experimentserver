from __future__ import annotations
import enum
import queue
import threading
import typing

from .logging import LoggerObject


class _QueueObject(enum.Enum):
    STOP = 0
    PROCESS = 1
    FINISH = 2


class QueueThread(LoggerObject):
    _instances: typing.List[QueueThread] = []

    def __init__(self, name: str, callback: typing.Callable,
                 callback_kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None,
                 name_append: typing.Optional[str] = None):
        super().__init__()

        name_append = name_append or ''

        # Callback
        self._callback = callback
        self._callback_kwargs = callback_kwargs or {}

        # Queue for objects
        self._queue = queue.PriorityQueue()

        # Child thread
        self._thread = threading.Thread(name=name + self.__class__.__name__ + name_append, target=self._thread_queue)

        # Add self to instance list
        self._instances.append(self)

    def append(self, obj):
        self._append(_QueueObject.PROCESS, obj)

    def _append(self, obj_type: _QueueObject, obj=None):
        # Insert into priority queue
        self._queue.put((obj_type.value, obj_type, obj))

    def start(self):
        self._thread.start()

    def stop(self, immediate: bool = False):
        # Don't bother if thread is stopped
        if not self._thread.is_alive():
            return

        # Queue a stop event
        if immediate:
            self._append(_QueueObject.STOP)
        else:
            self._append(_QueueObject.FINISH)

    @classmethod
    def stop_all(cls, immediate: bool = False):
        for instance in cls._instances:
            instance.stop(immediate)

        for instance in cls._instances:
            instance._thread.join()

    def _thread_queue(self):
        self._logger.info('Started')

        flag_run = True

        while flag_run:
            # Get event from queue
            (_, obj_type, obj) = self._queue.get()

            if obj_type is _QueueObject.STOP:
                self._logger.info('Immediate stop requested')
                break
            elif obj_type is _QueueObject.PROCESS:
                try:
                    # Pass object to callback
                    self._callback(obj, **self._callback_kwargs)
                except Exception:
                    self._logger.exception(f"Unhandled exception in queue callback when processing: {obj!r}")
            elif obj_type is _QueueObject.FINISH:
                self._logger.info('Delayed stop requested')

                # Send empty call to callback to finalise process
                self._callback(None, **self._callback_kwargs)

                flag_run = False

        self._logger.info('Stopped')
