import logging
import sys
import typing
from datetime import datetime

from experimentserver.data import RecordType
from experimentserver.data.database import get_database


def filter_event(record: logging.LogRecord):
    # Ignore records without the event flag or if flag is not set
    if not hasattr(record, 'event') or not record.event:
        return False

    return True


class DatabaseEventHandler(logging.Handler):
    def __init__(self, target: typing.Optional[str] = None):
        super().__init__()

        # Dont get database instance yet, since configuration happens after logging is set up
        self._db_client = None
        self._db_target = target

        # Add event filter
        self.addFilter(filter_event)

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
            'level_number': record.levelno,
            'function': record.funcName,
            'line_number': record.funcName
        }

        # Get optional fields
        if hasattr(record, 'thread'):
            record_tags['thread_id'] = record.thread

        if hasattr(record, 'threadName'):
            record_tags['thread'] = record.threadName

        if hasattr(record, 'process'):
            record_tags['process_id'] = record.process

        if hasattr(record, 'processName'):
            record_tags['process'] = record.processName

        # Save record to database
        self._db_client.record(datetime.fromtimestamp(record.created), RecordType.EVENT, record_payload, record_tags)
