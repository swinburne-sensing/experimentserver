import logging
import sys
import threading
import typing
from datetime import datetime

from experimentserver.data import MeasurementGroup
from experimentserver.data.database import get_database
from experimentserver.data.export import ExporterSource, record_measurement


def filter_event(record: logging.LogRecord):
    # Ignore records without the event flag or if flag is not set
    # noinspection PyUnresolvedReferences
    if not hasattr(record, 'event') or not record.event:
        return False

    return True


class DatabaseEventHandler(logging.Handler, ExporterSource):
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
        # Attempt to
        if self._db_client is None:
            try:
                self._db_client = get_database(self._db_target)
            except KeyError:
                print('Database client not ready', file=sys.stderr)
                return

        record_payload = {
            'message': record.msg
        }

        record_tags = {
            'level': record.levelname,
            'level_number': int(record.levelno),
            'function': record.funcName,
            'line_number': int(record.lineno)
        }

        # Get optional fields
        if hasattr(record, 'thread'):
            record_tags['thread_id'] = int(record.thread)

        if hasattr(record, 'threadName'):
            record_tags['thread'] = record.threadName

        if hasattr(record, 'process'):
            record_tags['process_id'] = int(record.process)

        if hasattr(record, 'processName'):
            record_tags['process'] = record.processName

        # Save record to database
        record_measurement(datetime.fromtimestamp(record.created), self, MeasurementGroup.EVENT, record_payload,
                           record_tags)


class EventBufferHandler(logging.Handler):
    def __init__(self, enable_filter: bool = True):
        super().__init__()

        # Storage for records
        self._record_lock = threading.RLock()
        self._records: typing.List[logging.LogRecord] = []

        # Add event filter
        if enable_filter:
            self.addFilter(filter_event)

    def emit(self, record):
        with self._record_lock:
            self._records.append(record)

    def clear(self):
        with self._record_lock:
            self._records.clear()

    def get_events(self, since: typing.Optional[float] = None):
        with self._record_lock:
            return self._records.copy()
