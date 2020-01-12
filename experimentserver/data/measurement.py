from __future__ import annotations

import abc
import threading
import typing
from datetime import datetime

import experimentserver
from experimentserver.util.logging import LoggerClass
from . import TYPE_FIELD_DICT, TYPE_TAG_DICT, MeasurementGroup, is_unit


class MeasurementTargetRemappingException(experimentserver.ApplicationException):
    pass


class MeasurementSource(metaclass=abc.ABCMeta):
    """  """
    @abc.abstractmethod
    def get_export_source_name(self) -> str:
        """

        :return:
        """
        pass


class Measurement(LoggerClass):
    """  """
    TYPE_DYNAMIC_FIELD = typing.Callable[['Measurement'], typing.Any]

    _metadata_lock = threading.RLock()

    _metadata_global_tags = {}
    _metadata_global_tags_stack = []

    _metadata_dynamic_fields: typing.Dict[str, TYPE_DYNAMIC_FIELD] = {}

    def __init__(self, source: MeasurementSource, measurement_group: MeasurementGroup, fields: TYPE_FIELD_DICT,
                 timestamp: typing.Optional[datetime] = None, tags: typing.Optional[TYPE_TAG_DICT] = None):
        """ Create new Measurement.

        :param source:
        :param measurement_group:
        :param fields:
        :param timestamp: if None then datetime.now() is used
        :param tags:
        """
        self.source = source
        self.measurement_group = measurement_group
        self._fields = fields.copy()
        self.timestamp = timestamp or datetime.now()
        self._tags = {}

        with self._metadata_lock:
            # Default to global _tags
            self._tags.update(self._metadata_global_tags)

            # Fetch dynamic _fields
            dynamic_field = self._metadata_dynamic_fields.copy()

        # Overwrite with instance _fields and _tags
        if tags is not None:
            self._tags.update(tags)

        # Append source name to _tags
        self._tags['source'] = self.source.get_export_source_name()

        # Apply dynamic _fields and _tags
        for tag_key, tag_value in self._tags.items():
            if callable(tag_value):
                self._tags[tag_key] = tag_value()

        for field_key, field_callable in dynamic_field.items():
            self._fields[field_key] = field_callable(self)

    def update_tags(self, tags: TYPE_TAG_DICT) -> typing.NoReturn:
        """

        :param tags:
        :return:
        """
        self._tags.update(tags)

    def update_fields(self, fields: TYPE_FIELD_DICT) -> typing.NoReturn:
        """

        :param fields:
        :return:
        """
        self._fields.update(fields)

    def get_fields(self, quantity_support: bool = True):
        """

        :param quantity_support:
        :return:
        """
        if quantity_support:
            return self._fields
        else:
            return {key: field.magnitude if is_unit(field) else field for key, field in self._fields.items()}

    def get_tags(self, quantity_support: bool = True):
        """ TODO

        :param quantity_support:
        :return:
        """
        if quantity_support:
            return self._tags
        else:
            return {key: tag.magnitude if is_unit(tag) else tag for key, tag in self._tags.items()}

    @staticmethod
    def generate_source_hash(source: str, measurement_group: MeasurementGroup) -> int:
        """ TODO

        :param source:
        :param measurement_group:
        :return:
        """
        return hash((source, measurement_group))

    def get_source_hash(self) -> int:
        """ TODO

        :return:
        """
        return self.generate_source_hash(self.source.get_export_source_name(), self.measurement_group)

    def __str__(self) -> str:
        fields = {k: f"{v}" if is_unit(v) else v for k, v in self._fields.items()}

        return f"Measurement(fields={fields}, tags={self._tags})"

    @classmethod
    def add_dynamic_field(cls, name, callback: TYPE_DYNAMIC_FIELD) -> typing.NoReturn:
        """ TODO

        :param name:
        :param callback:
        """
        with cls._metadata_lock:
            cls._metadata_dynamic_fields[name] = callback

            cls._get_class_logger().debug(f"Registered dynamic field {name} = {callback!r}")

    @classmethod
    def add_tag(cls, tag, value) -> typing.NoReturn:
        """ TODO

        :param tag:
        :param value:
        """
        with cls._metadata_lock:
            cls._metadata_global_tags[tag] = value

            cls._get_class_logger().debug(f"Registered tag {tag} = {value!r}")

    @classmethod
    def add_tags(cls, tags: TYPE_TAG_DICT) -> typing.NoReturn:
        """ TODO

        :param tags:
        """
        with cls._metadata_lock:
            cls._metadata_global_tags.update(tags)

            cls._get_class_logger().debug(f"Registered tags {tags!r}")

    @classmethod
    def push_metadata(cls) -> typing.NoReturn:
        """ TODO """
        with cls._metadata_lock:
            cls._metadata_global_tags_stack.append((cls._metadata_dynamic_fields.copy(),
                                                    cls._metadata_global_tags.copy()))

            cls._get_class_logger().debug(f"Pushed tag stack (depth: {len(cls._metadata_global_tags_stack)})")

    @classmethod
    def pop_metadata(cls) -> typing.NoReturn:
        """ TODO

        :return:
        """
        if len(cls._metadata_global_tags_stack):
            return

        with cls._metadata_lock:
            (cls._metadata_dynamic_fields, cls._metadata_global_tags) = cls._metadata_global_tags_stack.pop()

            cls._get_class_logger().debug(f"Popped tag stack (depth: {len(cls._metadata_global_tags_stack)})")


class MeasurementTarget(metaclass=abc.ABCMeta):
    """  """
    # Default exporter name
    MEASUREMENT_TARGET_DEFAULT = 'default'

    # Lock
    _measurement_metadata_lock = threading.RLock()

    # List of all targets
    _measurement_target_list: typing.Dict[str, typing.List[MeasurementTarget]] = {}

    # Remapping of exporter sources to targets
    _measurement_target_remap: typing.Dict[str, str] = {}

    # Cache for measurements
    _measurement_cache: typing.Dict[int, Measurement] = {}

    def __init__(self, measurement_target_name: typing.Optional[str] = None, **kwargs):
        """ Instantiate a new ExporterTarget.

        :param measurement_target_name:
        """
        super().__init__(**kwargs)

        self._exporter_target_name = measurement_target_name or self.MEASUREMENT_TARGET_DEFAULT

        # Register self into list of target exporters
        with self._measurement_metadata_lock:
            if self._exporter_target_name in self._measurement_target_list:
                self._measurement_target_list[self._exporter_target_name].append(self)
            else:
                self._measurement_target_list[self._exporter_target_name] = [self]

    @classmethod
    def get_cached_measurement(cls, source: str, measurement_group: MeasurementGroup) -> typing.Optional[Measurement]:
        measurement_hash = Measurement.generate_source_hash(source, measurement_group)

        with cls._measurement_metadata_lock:
            if measurement_hash not in cls._measurement_cache:
                return None

            return cls._measurement_cache[measurement_hash]

    @classmethod
    def measurement_target_remap(cls, source: str, destination: str):
        with cls._measurement_metadata_lock:
            cls._measurement_target_remap[source] = destination

    @abc.abstractmethod
    def _record(self, measurement: Measurement) -> typing.NoReturn:
        """ Record a Measurement object.

        :param measurement:
        """
        pass

    @classmethod
    def record(cls, measurement: Measurement) -> typing.NoReturn:
        # Set default exporter target
        source_name = measurement.source.get_export_source_name()
        target_name = cls.MEASUREMENT_TARGET_DEFAULT

        with cls._measurement_metadata_lock:
            # Cache measurement
            cls._measurement_cache[measurement.get_source_hash()] = measurement

            # Remap exporter target if remap is set
            if measurement.source.get_export_source_name() in cls._measurement_target_remap:
                target_name = cls._measurement_target_remap[source_name]

            if target_name not in cls._measurement_target_list or len(cls._measurement_target_list[target_name]) == 0:
                raise MeasurementTargetRemappingException(f"Measurement source {source_name} has no valid targets")

            targets = cls._measurement_target_list[target_name]

        for target in targets:
            target._record(measurement)


class DummyTarget(LoggerClass, MeasurementTarget):
    def __init__(self):
        super(DummyTarget, self).__init__()

    def _record(self, measurement: Measurement) -> typing.NoReturn:
        self._get_class_logger().info(measurement)


# Basic dynamic fields
def dynamic_field_time_delta(initial_timestamp: float) -> Measurement.TYPE_DYNAMIC_FIELD:
    def func(measurement: Measurement):
        return measurement.timestamp.timestamp() - initial_timestamp

    return func