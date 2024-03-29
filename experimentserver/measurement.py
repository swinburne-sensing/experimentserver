from __future__ import annotations

import abc
import enum
import json
import queue
import re
import time
import threading
import typing
import yaml
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from experimentlib.data.humidity import unit_abs
from experimentlib.data.unit import Quantity, dimensionless, registry
from experimentlib.logging.classes import LoggedAbstract, Logged
from experimentlib.util.constant import FORMAT_TIMESTAMP_FILENAME
from experimentlib.util.time import now, get_localzone
import pandas as pd
import pint

import experimentserver
from experimentserver.hardware.base.enum import HardwareEnum
from experimentserver.util import lock, thread


# Type definitions
T_FIELD_NAME = str
T_FIELD_VALUE = typing.Union[str, int, float, bool, Quantity, timedelta]

T_DYNAMIC_FIELD = typing.Callable[['Measurement'], T_FIELD_VALUE]

T_TAG_NAME = str
T_TAG_VALUE = typing.Union[str, int, bool, datetime, timedelta, Quantity, HardwareEnum]

T_FIELD_MAP = typing.Mapping[T_FIELD_NAME, T_FIELD_VALUE]
T_DYNAMIC_FIELD_MAP = typing.MutableMapping[str, T_DYNAMIC_FIELD]
T_TAG_MAP = typing.Mapping[T_TAG_NAME, T_TAG_VALUE]

_T_FIELD_TAG_MAP = typing.TypeVar('_T_FIELD_TAG_MAP', bound=typing.Union[T_FIELD_MAP, T_TAG_MAP])


class MeasurementTargetRemappingException(experimentserver.ApplicationException):
    """ Error thrown when measurement remapping fails. """
    pass


class MeasurementSource(metaclass=abc.ABCMeta):
    """ Measurement sources are classes that generate Measurement objects. This interface is used to define a method
    for fetching a source name for Measurement generation. """
    @abc.abstractmethod
    def get_export_source_name(self) -> str:
        """

        :return:
        """
        pass


class Measurement(Logged):
    """ Measurements capture data generated by Hardware for processing/storage. They are generic structures that
    contain a number of fields, optional tags, a measurement type/classification, and a timestamp. Measurements may
    contain dynamic fields (calculated on initialisation) and inherit globally configured tags. Tags and dynamic fields
    are metadata which is held in a stack. The app_metadata stack may be pused to or pulled from as necessary. """

    metadata_global_lock = lock.MonitoredLock('Measurement.metadata_global_lock', 5.0, True)

    _metadata_global_dynamic_fields: typing.MutableMapping[T_FIELD_NAME, T_DYNAMIC_FIELD] = {}
    _metadata_global_tags: typing.MutableMapping[T_TAG_NAME, T_TAG_VALUE] = {}

    _metadata_global_metadata_stack: typing.List[typing.Tuple[typing.MutableMapping[T_FIELD_NAME, T_DYNAMIC_FIELD],
                                                              typing.MutableMapping[T_TAG_NAME, T_TAG_VALUE]]] = []

    # Base units in various measurement systems
    _BASE_UNITS = {unit_abs, registry.sccm, registry.degC, registry.m, registry.V, registry.A, registry.W, registry.ohm,
                   registry.F, registry.H, registry.Pa, registry.Hz, registry.s}
    
    # Units convertable directly to magnitude
    _PASS_UNITS = {registry.delta_degC / registry.min, registry.rpm}

    def __init__(self, source: MeasurementSource, measurement_group: MeasurementGroup, fields: T_FIELD_MAP,
                 timestamp: typing.Optional[datetime] = None,
                 dynamic_fields: typing.Optional[T_DYNAMIC_FIELD_MAP] = None,
                 tags: typing.Optional[T_TAG_MAP] = None):
        """ Create new Measurement.

        :param source:
        :param measurement_group:
        :param fields:
        :param timestamp: if None then now() is used
        :param dynamic_fields:
        :param tags:
        """
        Logged.__init__(self)

        self.source = source
        self.measurement_group = measurement_group
        self._fields: typing.Dict[str, typing.Any] = dict(fields)
        self.timestamp = timestamp or now()
        self._tags: typing.Dict[str, typing.Any] = {}

        # Fix/replace pint Quantities
        for field_name, field_value in self._fields.items():
            if isinstance(field_value, pint.Quantity):
                self._fields[field_name] = Quantity(field_value.magnitude, field_value.units)

        for tag_name, tag_value in self._tags.items():
            if isinstance(tag_value, pint.Quantity):
                self._tags[tag_name] = Quantity(tag_value.magnitude, field_value.units)

        dynamic_fields = dict(dynamic_fields) if dynamic_fields is not None else {}

        with self.metadata_global_lock.lock():
            # Default to global tags
            self._tags.update(self._metadata_global_tags)

            # Fetch global dynamic fields
            dynamic_fields.update(self._metadata_global_dynamic_fields)

        # Overwrite with instance fields and tags
        if tags is not None:
            self._tags.update(tags)

        # Append source name to tags
        self._tags['hardware'] = self.source.get_export_source_name()
        self._tags['hardware_class'] = str(self.source.__class__.__name__)

        # Apply dynamic fields and tags
        for tag_key, tag_value in self._tags.items():
            if callable(tag_value):
                self._tags[tag_key] = tag_value()

        for field_key, field_callable in dynamic_fields.items():
            self._fields[field_key] = field_callable(self)
    
    def _as_base_units(self, data_dict: _T_FIELD_TAG_MAP) -> _T_FIELD_TAG_MAP:
        for key, value in data_dict.items():
            if isinstance(value, Quantity):
                if value.is_compatible_with(dimensionless) or value.units in self._PASS_UNITS:
                    self.logger().trace(f"Passing key {key} = {value!s}")
                else:
                    unit_match = False

                    for base_unit in self._BASE_UNITS:
                        if value.is_compatible_with(base_unit):
                            self.logger().trace(f"Convert key {key} = {value!s} to base unit "
                                                f"{base_unit}")
                            value.ito(base_unit)
                            unit_match = True
                            break
                    
                    if not unit_match:
                        self.logger().warning(f"Unexpected unit system in key {key} = {value!s}")
        
        return data_dict

    def add_field(self, name: T_FIELD_NAME, value: T_FIELD_VALUE) -> None:
        """ TODO

        :param name:
        :param value:
        :return:
        """
        self._fields[name] = value

    def get_fields(self, as_base_units: bool = False) -> T_FIELD_MAP:
        """ TODO

        :return:
        """
        fields = self._fields.copy()

        if as_base_units:
            fields = self._as_base_units(fields)

        return fields

    def add_tag(self, name: T_TAG_NAME, value: T_FIELD_VALUE) -> None:
        """

        :param name:
        :param value:
        :return:
        """
        self._tags[name] = value

    def add_tags(self, tags: T_TAG_MAP) -> None:
        """

        :param tags:
        :return:
        """
        self._tags.update(tags)

    def as_dict(self, as_base_units: bool = False) -> \
            typing.MutableMapping[str, typing.Union[str, float, T_FIELD_MAP, T_TAG_MAP]]:
        return {
            'source': self.source.get_export_source_name(),
            'group': self.measurement_group.value,
            'timestamp': self.timestamp.isoformat(),
            'timestamp_epoch': self.timestamp.timestamp(),
            'fields': self.get_fields(as_base_units),
            'tags': self.get_tags(as_base_units)
        }

    def get_tags(self, as_base_units: bool = False) -> T_TAG_MAP:
        """ TODO

        :return:
        """
        tags = self._tags.copy()

        if as_base_units:
            tags = self._as_base_units(tags)

        return tags

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

    def __getitem__(self, item: typing.Any) -> typing.Union[T_FIELD_VALUE, T_TAG_VALUE]:
        assert isinstance(item, str)

        if item in self._fields:
            return self._fields[item]

        if item in self._tags:
            return self._tags[item]

        raise ValueError(f"{item} not a valid field or tag")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self.source}, measurement_group={self.measurement_group}, " \
               f"fields={self.get_fields()!r}, timestamp={self.timestamp}, tags={self.get_tags()})"

    def __str__(self) -> str:
        fields = {k: str(v) for k, v in self._fields.items()}
        tags = {k: str(v) for k, v in self._tags.items()}

        return f"Measurement(source={self.source.get_export_source_name()}, " \
               f"measurement_group={self.measurement_group}, fields={fields}, " \
               f"timestamp={self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}, " \
               f"tags={tags})"

    @classmethod
    def add_global_dynamic_field(cls, name: str, callback: T_DYNAMIC_FIELD) -> None:
        """ TODO

        :param name:
        :param callback:
        """
        with cls.metadata_global_lock.lock():
            cls._metadata_global_dynamic_fields[name] = callback

            cls.logger().info(f"Registered global dynamic field {name} = {callback!r}")

    @classmethod
    def add_global_tag(cls, tag: str, value: T_TAG_VALUE) -> None:
        """ TODO

        :param tag:
        :param value:
        """
        with cls.metadata_global_lock.lock():
            cls._metadata_global_tags[tag] = value

            cls.logger().info(f"Registered global tag {tag} = {value!r}")

    @classmethod
    def add_global_tags(cls, tags: T_TAG_MAP) -> None:
        """ TODO

        :param tags:
        """
        if len(tags) == 0:
            return

        with cls.metadata_global_lock.lock():
            cls._metadata_global_tags.update(tags)

            cls.logger().info(f"Registered global tags {tags!r}")
    
    @classmethod
    def get_global_tags(cls) -> T_TAG_MAP:
        """ TODO

        :param tags:
        """
        with cls.metadata_global_lock.lock():
            return dict(cls._metadata_global_tags)


    @classmethod
    def push_global_metadata(cls) -> None:
        """ Pop metadata stack (save current metadata). """
        with cls.metadata_global_lock.lock():
            cls._metadata_global_metadata_stack.append((dict(cls._metadata_global_dynamic_fields),
                                                        dict(cls._metadata_global_tags)))

            cls.logger().info(f"Pushed global tag stack (depth: {len(cls._metadata_global_metadata_stack)})")

    @classmethod
    def pop_global_metadata(cls) -> None:
        """ Pop metadata stack (retrieve previous metadata). """
        if len(cls._metadata_global_metadata_stack) > 0:
            return

        with cls.metadata_global_lock.lock():
            (cls._metadata_global_dynamic_fields, cls._metadata_global_tags) = cls._metadata_global_metadata_stack.pop()

            cls.logger().info(f"Popped global tag stack (depth: {len(cls._metadata_global_metadata_stack)})")

    @classmethod
    def flush_global_metadata(cls) -> None:
        """ Clear metadata stack. """
        with cls.metadata_global_lock.lock():
            if len(cls._metadata_global_metadata_stack) > 0:
                (cls._metadata_global_dynamic_fields,
                 cls._metadata_global_tags) = cls._metadata_global_metadata_stack.pop(0)
                cls._metadata_global_metadata_stack.clear()

                cls.logger().info(f"Flushed global tag stack")


class MeasurementTarget(metaclass=abc.ABCMeta):
    """ Measurement targets process or store Measurement objects. Measurement objects from one source may be remapped
    to a target as necessary, allowing some data to be stored in public/private databases. """

    # Default exporter name
    MEASUREMENT_TARGET_DEFAULT = 'default'

    # Lock
    _measurement_metadata_lock = threading.RLock()

    # List of all targets
    _measurement_target_list: typing.Dict[str, typing.List[MeasurementTarget]] = defaultdict(list)

    # Remapping of exporter sources to targets
    _measurement_target_remap: typing.List[typing.Tuple[typing.Pattern[str], str]] = []

    # Cache for measurements
    _measurement_cache: typing.Dict[int, Measurement] = {}

    def __init__(self, measurement_target_name: typing.Optional[str] = None, **kwargs: typing.Any):
        """ Instantiate a new ExporterTarget.

        :param measurement_target_name:
        """
        super().__init__(**kwargs)

        self._exporter_target_name = measurement_target_name or self.MEASUREMENT_TARGET_DEFAULT

        # Register self into list of target exporters
        with self._measurement_metadata_lock:
            self._measurement_target_list[self._exporter_target_name].append(self)
    
    @classmethod
    def trigger_start(cls, **addon_metadata: typing.Any) -> None:
        """ Trigger the start of a new group of measurements
        """
        with cls._measurement_metadata_lock:
            for target_list in cls._measurement_target_list.values():
                for target in target_list:
                    target._trigger_start(**addon_metadata)
    
    @classmethod
    def trigger_stop(cls, **addon_metadata: typing.Any) -> None:
        """ Trigger the stop of a group of measurements
        """
        with cls._measurement_metadata_lock:
            for target_list in cls._measurement_target_list.values():
                for target in target_list:
                    target._trigger_stop(**addon_metadata)

    @classmethod
    def get_cached_measurement(cls, source: str, measurement_group: MeasurementGroup) -> typing.Optional[Measurement]:
        """ Fetch the last Measurement generated from a given source in a given MeasurementGroup.

        :param source:
        :param measurement_group:
        :return:
        """
        measurement_hash = Measurement.generate_source_hash(source, measurement_group)

        with cls._measurement_metadata_lock:
            if measurement_hash not in cls._measurement_cache:
                return None

            return cls._measurement_cache[measurement_hash]

    @classmethod
    def measurement_target_remap(cls, source: str, destination: str) -> None:
        """ TODO

        :param source:
        :param destination:
        """
        # Compile regex pattern
        source = source.replace('*', '.*')
        source_re = re.compile(f"^{source}$")

        with cls._measurement_metadata_lock:
            cls._measurement_target_remap.append((source_re, destination))

    @abc.abstractmethod
    def _record(self, measurement: Measurement) -> None:
        """ Inner abstract definition for Measurement object handling.

        :param measurement:
        """
        pass

    def _trigger_start(self, **addon_metadata: typing.Any) -> None:
        """ Trigger the start of a new group of measurements
        """
        pass
    
    def _trigger_stop(self, **addon_metadata: typing.Any) -> None:
        """ Trigger the stop of a group of measurements
        """
        pass

    @classmethod
    def record(cls, measurement: Measurement) -> None:
        """ Record a Measurement.

        :param measurement:
        """
        # Set default exporter target
        source_name = measurement.source.get_export_source_name()
        target_name = cls.MEASUREMENT_TARGET_DEFAULT

        with cls._measurement_metadata_lock:
            # Cache measurement
            cls._measurement_cache[measurement.get_source_hash()] = measurement

            # Remap exporter target if remap is set
            for pattern, new_target_name in cls._measurement_target_remap:
                if pattern.match(source_name) is not None:
                    target_name = new_target_name
                    break

            if target_name not in cls._measurement_target_list or len(cls._measurement_target_list[target_name]) == 0:
                raise MeasurementTargetRemappingException(f"Measurement source {source_name} has no valid targets")

            targets = cls._measurement_target_list[target_name]

        for target in targets:
            target._record(measurement)


class CSVTarget(LoggedAbstract, MeasurementTarget):
    _SLEEP_PERIOD = 1.0

    def __init__(self, root_dir: Path, write_period: float = 300.0):
        LoggedAbstract.__init__(self)
        MeasurementTarget.__init__(self)

        self._root_dir = root_dir.absolute()
        self._write_period = write_period

        # Ensure root directory exists
        self._root_dir.mkdir(parents=True, exist_ok=True)
        self._target_dir: typing.Optional[Path] = None

        self._measurement_queue: queue.Queue[Measurement] = queue.Queue()
        self._write_enabled: bool = False

        self._file_write_request = threading.Event()
        self._file_write_thread = thread.CallbackThread(
            name='csv_write',
            callback=self._write_queue,
            run_final=True
        )
        self._file_write_thread.thread_start()

        self._init_datetime: typing.Optional[datetime] = None
    
    def _record(self, measurement: Measurement) -> None:
        if self._write_enabled:
            self._measurement_queue.put(measurement)
        else:
            # Discard measurements while not started
            self.logger().debug(f"Discarding measurement {measurement!r}")

    def _save_metadata(self, **addon_metadata: typing.Any) -> None:
        # Dump metadata
        metadata = dict(Measurement.get_global_tags())
        metadata.update(addon_metadata)

        # Create summary file
        assert self._target_dir is not None
        with open(self._target_dir.joinpath('summary.yaml'), 'w') as summary_file:
            yaml.dump(metadata, summary_file)

    def _trigger_start(self, **addon_metadata: typing.Any) -> None:
        super()._trigger_start(**addon_metadata)

        # Create new directory for experiment
        self._target_dir = self._root_dir.joinpath(f"data_{now().strftime(FORMAT_TIMESTAMP_FILENAME)}")
        self._target_dir.mkdir()

        self._save_metadata(**addon_metadata, experiment_state='start')
        
        self.logger().info(f"Saving CSV measurements to {self._target_dir.absolute()}")
        self._write_enabled = True
    
    def _trigger_stop(self, **addon_metadata: typing.Any) -> None:
        self._save_metadata(**addon_metadata, experiment_state='stop')

        self._file_write_request.set()
        self._write_enabled = False
    
    def _write_queue(self) -> None:
        write_buffer = defaultdict(list)

        try:
            while True:
                measurement = self._measurement_queue.get(False)

                measurement_timestamp = measurement.timestamp.replace(tzinfo=None)

                if self._init_datetime is None:
                    self._init_datetime = measurement_timestamp

                row_dict = {
                    'datetime': measurement_timestamp,
                    'tag_source': measurement.source.get_export_source_name(),
                    'tag_group': measurement.measurement_group.value
                }

                for tag_name, tag_value in measurement.get_tags(True).items():
                    if isinstance(tag_value, Quantity):
                        row_dict['tag_' + tag_name] = tag_value.magnitude
                    elif isinstance(tag_value, timedelta):
                        row_dict['tag_' + tag_name] = tag_value.total_seconds()
                    else:
                        row_dict['tag_' + tag_name] = str(tag_value)

                for field_name, field_value in measurement.get_fields(True).items():
                    if isinstance(field_value, Quantity):
                        row_dict['field_' + field_name] = field_value.magnitude
                    elif isinstance(field_value, timedelta):
                        row_dict['field_' + field_name] = field_value.total_seconds()
                    elif isinstance(field_value, str):
                        # Discard string fields
                        pass
                    else:
                        row_dict['field_' + field_name] = field_value

                write_buffer[measurement.source.get_export_source_name()].append(row_dict)
        except queue.Empty:
            self._file_write_request.clear()

        if len(write_buffer) > 0:
            for source_name, source_buffer in write_buffer.items():
                file_path = self._target_dir.joinpath(f"{source_name}_{now().strftime(FORMAT_TIMESTAMP_FILENAME)}.csv")

                # Create dataframe and write CSV
                df = pd.DataFrame.from_dict(source_buffer)

                # Group rows into reasonable intervals
                df_grouped = df.groupby(
                    [pd.Grouper(key='datetime', freq='1s')] + 
                    [c for c in df.columns if c.startswith('tag_')],
                    dropna=False
                ).mean()

                # Add timestamp offset field
                df_grouped.insert(
                    0,
                    'timestamp',
                    (
                        df_grouped.index.get_level_values('datetime') - self._init_datetime
                    ).total_seconds()
                )

                df_grouped.to_csv(file_path)

                self.logger().info(f"Wrote {file_path.absolute()!s}")
        
        time_resume = time.time() + self._write_period
        
        while not self._file_write_thread.thread_stop_requested() and not self._file_write_request.is_set() \
                and time.time() < time_resume:
            self.sleep(self._SLEEP_PERIOD, 'waiting for buffer', silent=True)


class DummyTarget(LoggedAbstract, MeasurementTarget):
    """ A dumb target that just logs all Measurements received. Used for testing only."""

    def __init__(self) -> None:
        LoggedAbstract.__init__(self)
        MeasurementTarget.__init__(self)

    def _record(self, measurement: Measurement) -> None:
        self.logger().info(measurement)


class JSONTarget(LoggedAbstract, MeasurementTarget):
    _SLEEP_PERIOD = 1.0

    def __init__(self, target_file: Path, write_period: float = 10.0):
        LoggedAbstract.__init__(self)
        MeasurementTarget.__init__(self)

        self._target_file = target_file
        self._write_period = write_period

        self._measurement_queue: queue.Queue[Measurement] = queue.Queue()

        self._file_write_thread = thread.CallbackThread(
            name='json_write',
            callback=self._write_queue,
            run_final=True
        )
        self._file_write_thread.thread_start()
    
    def _record(self, measurement: Measurement) -> None:
        self._measurement_queue.put(measurement)

    def _write_queue(self) -> None:
        write_buffer = []

        try:
            while True:
                measurement = self._measurement_queue.get(False)

                try:
                    measurement_dict = measurement.as_dict(True)

                    field_dict = measurement_dict['fields']
                    tag_dict = measurement_dict['tags']

                    assert isinstance(field_dict, dict)
                    for field_name, field_value in field_dict.items():
                        if isinstance(field_value, Quantity):
                            field_dict[field_name] = {
                                'value': field_value.magnitude,
                                'units': '{:~}'.format(field_value.units)  # type: ignore
                            }
                    
                    assert isinstance(tag_dict, dict)
                    for tag_name, tag_value in tag_dict.items():
                        if isinstance(tag_value, Quantity):
                            tag_dict[tag_name] = {
                                'value': tag_value.magnitude,
                                'units': '{:~}'.format(tag_value.units)  # type: ignore
                            }
                        elif isinstance(tag_value, timedelta):
                            tag_dict[tag_name] = tag_value.total_seconds()
                        elif isinstance(tag_value, datetime):
                            tag_dict[tag_name] = tag_value.astimezone(get_localzone()).isoformat()
                        elif isinstance(tag_value, (bool, float, int, str)):
                            pass
                        else:
                            tag_dict[tag_name] = str(tag_value)

                    write_buffer.append(json.dumps(measurement_dict))
                except TypeError as exc:
                    self.logger().error(f"Measurement {measurement!s} could not be serialized (error: {exc!s})")
        except queue.Empty:
            pass

        if len(write_buffer) > 0:
            with self._target_file.open('a', encoding='utf8') as target_file:
                target_file.writelines(line +'\n' for line in write_buffer)
            
            self.logger().debug(f"Wrote {len(write_buffer)} JSON objects to {self._target_file.absolute()!s}")
        
        time_resume = time.time() + self._write_period
        
        while not self._file_write_thread.thread_stop_requested() and time.time() < time_resume:
            self.sleep(self._SLEEP_PERIOD, 'waiting for buffer', silent=True)


# Type hinting definitions for measurements
T_MEASUREMENT_SEQUENCE = typing.Sequence[Measurement]
T_MEASUREMENT_RETURN = typing.Union[Measurement, T_MEASUREMENT_SEQUENCE, T_FIELD_MAP]


# Basic dynamic fields
def dynamic_field_time_delta(initial_time: datetime) -> T_DYNAMIC_FIELD:
    def func(measurement: Measurement) -> float:
        return round((measurement.timestamp - initial_time).total_seconds(), 6)

    return func


class MeasurementGroup(enum.Enum):
    """ Definition for known types of hardware or measurements. """

    # Raw data
    RAW = 'raw'

    # Metadata
    EVENT = 'event'
    STATUS = 'status'

    # Gas flow
    MFC = 'mfc'

    # Calculated gas concentrations
    GAS = 'gas'

    # Gas combustion measurements
    COMBUSTION = 'combust'

    # Valves
    VALVE = 'valve'

    # Internal
    DEBUG = 'debug'

    # Motion
    ACCELERATION = 'acceleration'
    VELOCITY = 'velocity'
    POSITION = 'position'

    # Electrical measurements
    VOLTAGE = 'voltage'
    CURRENT = 'current'
    RESISTANCE = 'resistance'
    IMPEDANCE = 'impedance'
    POWER = 'power'

    # LCR
    CAPACITANCE = 'capacitance'
    INDUCTANCE = 'inductance'
    DISSIPATION = 'dissipation'
    QUALITY = 'quality'

    # Measure current, supply voltage
    CONDUCTOMETRIC_IV = 'conductometric_iv'

    # Measure voltage, supply current
    CONDUCTOMETRIC_VI = 'conductometric_vi'

    # Environmental conditions
    TEMPERATURE = 'temperature'
    HUMIDITY = 'humidity'

    # CV/CC power supply
    SUPPLY = 'supply'

    # Frequency
    FREQUENCY = 'frequency'
    PERIOD = 'period'

    # Complex signals
    TIME_DOMAIN_SAMPLE = 'timedomain'
    FREQUENCY_DOMAIN_SAMPLE = 'freqdomain'
    TIME_FREQUENCY_SAMPLE = 'tfdomain'

    # Particle counts
    PARTICLE_COUNT = 'pn'
    PARTICLE_MASS = 'pm'
