import abc
import copy
import threading
import typing
from datetime import datetime

from . import TYPING_RECORD, TYPING_TAGS, RecordType
from experimentserver.util.logging import LoggerObject


TYPING_DYNAMIC_FIELD = typing.Callable[[datetime, RecordType, TYPING_RECORD, TYPING_TAGS], typing.Any]


def dynamic_field_time_delta(initial_timestamp: float) -> TYPING_DYNAMIC_FIELD:
    def func(timestamp: datetime, record_type: RecordType, record: TYPING_RECORD, tags: TYPING_TAGS):
        return timestamp.timestamp() - initial_timestamp

    return func


class Exporter(metaclass=abc.ABCMeta):
    _metadata_lock = threading.RLock()

    # Storage for dynamic fields
    _dynamic_field: typing.Dict[str, TYPING_DYNAMIC_FIELD] = {}

    # Storage for tags
    _tags = {}

    # Tag stack
    _metadata_stack = []

    def __init__(self):
        super().__init__()

    @classmethod
    def add_dynamic_field(cls, name, callback: TYPING_DYNAMIC_FIELD):
        with cls._metadata_lock:
            cls._dynamic_field[name] = callback

    @classmethod
    def add_tag(cls, tag, value):
        with cls._metadata_lock:
            cls._tags[tag] = value

    @classmethod
    def add_tags(cls, tags: TYPING_TAGS):
        with cls._metadata_lock:
            cls._tags.update(tags)

    @classmethod
    def push_metadata(cls):
        with cls._metadata_lock:
            cls._metadata_stack.append((cls._dynamic_field, cls._tags))

    @classmethod
    def pop_metadata(cls):
        if len(cls._metadata_stack):
            return

        with cls._metadata_lock:
            (cls._dynamic_field, cls._tags) = cls._metadata_stack.pop()

    def record(self, timestamp: datetime, record_type: RecordType, record: TYPING_RECORD,
               tags: typing.Optional[TYPING_TAGS] = None):
        inst_tags = {}

        with self._metadata_lock:
            # Add shared tags
            inst_tags.update(copy.deepcopy(self._tags))

            # Fetch dynamic fields while lock is held
            dynamic_fields = copy.deepcopy(self._dynamic_field)

        # Add additional tags (overwrite existing fields)
        inst_tags.update(tags)

        # Update dynamic tags
        for tag_key, tag_value in inst_tags.items():
            if callable(tag_value):
                inst_tags[tag_key] = tag_value()

        # Process and dynamic fields
        inst_record = copy.deepcopy(record)

        for dynamic_field, dynamic_field_callback in dynamic_fields.items():
            inst_record[dynamic_field] = dynamic_field_callback(timestamp, record_type, inst_record, inst_tags)

        # Pass to child class
        self._record(timestamp, record_type, inst_record, inst_tags)

    @abc.abstractmethod
    def _record(self, timestamp: datetime, record_type: RecordType, record: TYPING_RECORD, tags: TYPING_TAGS):
        pass


class ExporterProxy(LoggerObject, Exporter):
    def __init__(self):
        super().__init__()

        self._exporters: typing.List[Exporter] = []

    def add_exporter(self, exporter):
        if exporter not in self._exporters:
            self._exporters.append(exporter)

    def clear_exporter(self):
        self._exporters.clear()

    def _record(self, timestamp: datetime, record_type: RecordType, record: TYPING_RECORD,
                tags: typing.Optional[TYPING_TAGS] = None):
        for exporter in self._exporters:
            exporter.record(timestamp, record_type, record, tags)
