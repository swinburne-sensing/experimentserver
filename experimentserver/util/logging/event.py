import logging
import sys
import threading
import typing
from datetime import datetime

from experimentserver.database import get_database
from experimentserver.data.measurement import Measurement, MeasurementSource, MeasurementTarget, MeasurementGroup


def filter_event(record: logging.LogRecord):
    # Ignore records without the event flag or if flag is not set
    # noinspection PyUnresolvedReferences
    if not hasattr(record, 'event') or not record.event:
        return False

    return True


class DatabaseEventHandler(logging.Handler, MeasurementSource):
    def __init__(self, target: typing.Optional[str] = None, enable_filter: bool = True):
        super().__init__()

        # Dont get database instance yet, since configuration happens after logging is set up
        self._db_client = None
        self._db_target = target

        # Add event filter
        if enable_filter:
            self.addFilter(filter_event)

    def get_export_source_name(self) -> str:
        return 'event'

    def emit(self, record: logging.LogRecord):
        # Attempt to get database client
        if self._db_client is None:
            try:
                self._db_client = get_database(self._db_target)
            except KeyError:
                print(f"Database client not available, discarding record: {record}", file=sys.stderr)
                return

        record_payload = {
            'message': record.msg
        }

        record_tags = {
            'log_level': record.levelname,
            'log_level_number': int(record.levelno),
            'log_function': record.funcName,
            'log_line_number': int(record.lineno)
        }

        # Get optional _fields
        if hasattr(record, 'thread'):
            record_tags['log_thread_id'] = int(record.thread)

        if hasattr(record, 'threadName'):
            record_tags['log_thread'] = record.threadName

        if hasattr(record, 'process'):
            record_tags['log_process_id'] = int(record.process)

        if hasattr(record, 'processName'):
            record_tags['log_process'] = record.processName

        # Save record to database
        MeasurementTarget.record(Measurement(self, MeasurementGroup.EVENT, record_payload,
                                             datetime.fromtimestamp(record.created), record_tags))


class EventBufferHandler(logging.Handler):
    """  """

    def __init__(self, max_length: int = 100, enable_filter: bool = True):
        """

        :param max_length:
        :param enable_filter:
        """
        super().__init__()

        # Storage for records
        self._max_length = max_length

        self._record_lock = threading.RLock()
        self._records: typing.List[logging.LogRecord] = []

        # Add event filter
        if enable_filter:
            self.addFilter(filter_event)

    def emit(self, record):
        with self._record_lock:
            self._records.append(record)

            # Pop oldest records until under the limit
            while len(self._records) > self._max_length:
                self._records.pop(0)

    def clear(self) -> typing.NoReturn:
        """ Flush event buffer. """
        with self._record_lock:
            self._records.clear()

    def get_events(self, since: typing.Optional[float] = None):
        """ Get event records from the buffer.

        :param since:
        :return: list
        """
        with self._record_lock:
            if since is None:
                return self._records.copy()
            else:
                record_buffer = []

                for record in self._records:
                    if record.created >= since:
                        record_buffer.append(record)

                return record_buffer
