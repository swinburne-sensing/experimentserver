from __future__ import annotations

import abc
import copy
import threading
import typing
from datetime import datetime

from . import TYPING_FIELD, TYPING_TAG, MeasurementGroup
from experimentserver.util.logging import get_logger


_LOGGER = get_logger(__name__)


EXPORTER_DEFAULT = 'default'


class ExporterSource(metaclass=abc.ABCMeta):
    """  """
    @abc.abstractmethod
    def get_export_source_name(self) -> str:
        """

        :return:
        """
        pass


# Locks
_exporter_target_lock = threading.RLock()
_metadata_lock = threading.RLock()


# Global storage for exporter targets
_exporter_target: typing.Dict[str, typing.List[ExporterTarget]] = {}


class ExporterTarget(metaclass=abc.ABCMeta):
    """  """

    def __init__(self, exporter_target_name: typing.Optional[str] = None, **kwargs):
        """

        :param exporter_target_name:
        """
        super().__init__(**kwargs)
        
        if exporter_target_name is None:
            exporter_target_name = EXPORTER_DEFAULT

        self._exporter_target_name = exporter_target_name

        # Register self into list of target exporters
        with _exporter_target_lock:
            if self._exporter_target_name in _exporter_target:
                _exporter_target[self._exporter_target_name].append(self)
            else:
                _exporter_target[self._exporter_target_name] = [self]

    @abc.abstractmethod
    def record(self, timestamp: datetime, measurement: MeasurementGroup, fields: TYPING_FIELD, tags: TYPING_TAG) \
            -> typing.NoReturn:
        pass


# Definition and storage for dynamic field callable
TYPING_DYNAMIC_FIELD = typing.Callable[[datetime, MeasurementGroup, TYPING_FIELD, TYPING_TAG], typing.Any]
_dynamic_field: typing.Dict[str, TYPING_DYNAMIC_FIELD] = {}

# Storage for tags and tag stack
_tags = {}
_metadata_stack = []

# Remapping of exporter sources to targets
_exporter_remap: typing.Dict[str, str] = {}


def add_dynamic_field(name, callback: TYPING_DYNAMIC_FIELD):
    with _metadata_lock:
        _dynamic_field[name] = callback

        _LOGGER.debug(f"Added dynamic field {name}")


def add_tag(tag, value):
    with _metadata_lock:
        _tags[tag] = value

        _LOGGER.debug(f"Added tag {tag}: {value!r}")


def add_tags(tags: TYPING_TAG):
    with _metadata_lock:
        _tags.update(tags)

        _LOGGER.debug(f"Added tags {tags!r}")


def push_metadata():
    with _metadata_lock:
        _metadata_stack.append((_dynamic_field, _tags))

        _LOGGER.debug(f"Pushed tags (current depth: {len(_metadata_stack)})")


def pop_metadata():
    global _dynamic_field, _tags

    if len(_metadata_stack):
        return

    with _metadata_lock:
        (_dynamic_field, _tags) = _metadata_stack.pop()

        _LOGGER.debug(f"Popped tags (current depth: {len(_metadata_stack)})")


def exporter_remap(source: str, target: str):
    with _metadata_lock:
        _exporter_remap[source] = target


def record_measurement(timestamp: datetime, source: ExporterSource, measurement: MeasurementGroup, fields: TYPING_FIELD,
                       extra_tags: typing.Optional[TYPING_TAG] = None):
    # Get source
    source_name = source.get_export_source_name()

    inst_tags = {
        'source': source_name
    }

    # Set default exporter target
    target_name = EXPORTER_DEFAULT

    with _metadata_lock:
        # Add shared tags
        inst_tags.update(copy.deepcopy(_tags))

        # Fetch dynamic fields while lock is held
        dynamic_fields = copy.deepcopy(_dynamic_field)

        # Remap exporter target if remap is set
        if source_name in _exporter_remap:
            target_name = _exporter_remap[source_name]

    # Add additional tags (overwrite existing fields)
    if extra_tags is not None:
        inst_tags.update(extra_tags)

    # Update dynamic tags
    for tag_key, tag_value in inst_tags.items():
        if callable(tag_value):
            inst_tags[tag_key] = tag_value()

    # Process dynamic fields
    inst_fields = copy.deepcopy(fields)

    for dynamic_field, dynamic_field_callback in dynamic_fields.items():
        inst_fields[dynamic_field] = dynamic_field_callback(timestamp, measurement, inst_fields, inst_tags)

    # Pass to exporter(s)
    with _exporter_target_lock:
        for exporter in _exporter_target[target_name]:
            exporter.record(timestamp, measurement, inst_fields, inst_tags)


# Basic dynamic fields
def dynamic_field_time_delta(initial_timestamp: float) -> TYPING_DYNAMIC_FIELD:
    # noinspection PyUnusedLocal
    def func(timestamp: datetime, record_type: MeasurementGroup, fields: TYPING_FIELD, tags: TYPING_TAG):
        return timestamp.timestamp() - initial_timestamp

    return func
