import typing
from datetime import datetime, timedelta, timezone

import influxdb_client
import urllib3
from experimentlib.data.gas import GasProperties, Component, Mixture
from experimentlib.data.humidity import unit_abs
from experimentlib.data.unit import Quantity, registry, parse, dimensionless
from experimentlib.logging.classes import LoggedAbstract
from experimentlib.util.time import get_localzone
from influxdb_client.client.write_api import ASYNCHRONOUS

import experimentserver
from experimentserver.hardware.base.enum import HardwareEnum
from experimentserver.measurement import Measurement, MeasurementTarget


# Suppress HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DatabaseError(experimentserver.ApplicationException):
    pass


class _InfluxDBv2Client(LoggedAbstract, MeasurementTarget):
    _INFLUXDB_RETRY = urllib3.Retry()

    _TAG_STR_SAFE = {int, GasProperties, HardwareEnum, Component, Mixture, Quantity}

    def __init__(self, identifier: str, connect_args: typing.MutableMapping[str, typing.Any]):
        self._identifier = identifier

        LoggedAbstract.__init__(self, self._identifier)
        MeasurementTarget.__init__(self, self._identifier)

        self.bucket = connect_args.pop('bucket')
        self.client = influxdb_client.InfluxDBClient(**connect_args, retries=self._INFLUXDB_RETRY)

        # Test connection
        health = self.client.health()

        if health.status == 'pass':
            self.logger().info(f"InfluxDB health: OK {health.version}")
        else:
            raise DatabaseError(f"InfluxDB failed health check with status {health}")

        # Setup write API
        self.write_api: influxdb_client.WriteApi = self.client.write_api(write_options=ASYNCHRONOUS)

    def __del__(self) -> None:
        if hasattr(self, 'write_api') and self.write_api is not None:
            self.write_api.close()
            self.write_api = None

    def _record(self, measurement: Measurement) -> None:
        point_list: typing.List[influxdb_client.Point] = []
        tag_dict: typing.Dict[str, str] = {}

        # Convert tags to strings
        for tag_name, tag_value in measurement.get_tags().items():
            if isinstance(tag_value, str):
                # Already OK
                pass
            elif isinstance(tag_value, float):
                raise DatabaseError(f"Tag {tag_name} has floating point value {tag_value}, convert to string or "
                                    f"Quantity for storage")
            elif isinstance(tag_value, bool):
                # InfluxDB style boolean
                tag_value = 'true' if tag_value else 'false'
            elif isinstance(tag_value, timedelta):
                tag_value = str(parse(tag_value.total_seconds(), registry.s))
            elif isinstance(tag_value, datetime):
                # Add UTC referenced tag
                tag_dict[tag_name + '_utc'] = tag_value.astimezone(timezone.utc).isoformat()

                # Use local time for tag value
                tag_value = tag_value.astimezone(get_localzone()).isoformat()
            elif any(isinstance(tag_value, t) for t in self._TAG_STR_SAFE):
                # Known safe conversion to string
                tag_value = str(tag_value)
            else:
                self.logger().warning(f"Casting tag {tag_name} (repr: {tag_value!r}, type: {type(tag_value)}) to "
                                      f"string \"{tag_value!s}\", conversion may not be stable")
                tag_value = str(tag_value)

            assert isinstance(tag_value, str), 'Tag values must be converted to strings'

            # Add non-empty tags to list
            if len(tag_value) > 0:
                tag_dict[tag_name] = tag_value

        # Add fields
        for field_name, field_value in measurement.get_fields().items():
            units_tag = None

            if isinstance(field_value, Quantity):
                units_tag = str(field_value.units)

                self.logger().trace(f"Quantity {field_name} = {field_value} (units: {field_value.units!r})")

                if field_value.units == dimensionless:
                    # Get absolute magnitude
                    self.logger().debug(f"Convert Quantity {field_name} = {field_value} to absolute magnitude")
                    field_value = field_value.m_as(dimensionless)
                elif field_value.is_compatible_with(dimensionless) or field_value.units in self._PASS_UNITS:
                    self.logger().debug(f"Convert Quantity {field_name} = {field_value} to magnitude (pass)")
                    field_value = field_value.magnitude
                elif field_value.units == registry.rpm:
                    self.logger().debug(f"Convert Quantity {field_name} = {field_value} to rpm")
                    field_value = field_value.m_as(registry.rpm)
                elif field_value.units.is_compatible_with(registry.Hz):
                    self.logger().debug(f"Convert Quantity {field_name} = {field_value} to Hz")
                    field_value = field_value.m_as(registry.Hz)
                elif field_value.is_compatible_with(registry.s):
                    self.logger().debug(f"Convert Quantity {field_name} = {field_value} to seconds")
                    field_value = field_value.m_as(registry.s)
                else:
                    unit_match = False

                    for base_unit in self._BASE_UNITS:
                        if field_value.is_compatible_with(base_unit):
                            self.logger().debug(f"Convert Quantity {field_name} = {field_value} to base unit "
                                                f"{base_unit}")

                            unit_match = True
                            field_value = field_value.m_as(field_value)
                            break

                    if not unit_match:
                        # Take magnitude of existing units
                        self.logger().warning(f"Convert Quantity {field_name} = {field_value} to magnitude (unhandled "
                                              f"unit)")
                        field_value = field_value.magnitude
            elif isinstance(field_value, timedelta):
                self.logger().debug(f"Converting timedelta {field_name} = {field_value} to seconds")
                field_value = field_value.total_seconds()
            elif any((isinstance(field_value, t) for t in (bool, int, float, str))):
                self.logger().debug(f"Unconverted {field_name} = {field_value!r} (type: {type(field_value)})")
            else:
                raise DatabaseError(f"Field {field_name} has unsupported value type (repr: {field_value!r}, "
                                    f"type: {type(field_value)})")

            # Create point for each field
            point = influxdb_client.Point(measurement.measurement_group.value).time(measurement.timestamp)\
                .field(field_name, field_value)

            if units_tag is not None:
                # Add tag with quantity units
                point.tag('units', units_tag)

            for tag_name, tag_value in tag_dict.items():
                point.tag(tag_name, tag_value)

            point_list.append(point)

        point_summary = ', '.join((point.to_line_protocol() for point in point_list))
        self.logger().trace(f"Writing {measurement} as [{point_summary}]")
        self.write_api.write(self.bucket, record=point_list)


_db_client: typing.Dict[str, _InfluxDBv2Client] = {}


def setup_database(identifier: str, connect_args: typing.MutableMapping[str, typing.Any], debug: bool) -> _InfluxDBv2Client:
    if identifier in _db_client:
        raise KeyError(f"Database connection with identifier {identifier} already exists")

    if debug:
        connect_args['bucket'] = connect_args['bucket'] + '_dev'

    _db_client[identifier] = _InfluxDBv2Client(identifier, connect_args)

    return _db_client[identifier]


def get_database(identifier: typing.Optional[str] = None) -> _InfluxDBv2Client:
    if identifier is None:
        identifier = MeasurementTarget.MEASUREMENT_TARGET_DEFAULT

    if identifier not in _db_client:
        raise KeyError(f"Unknown database identifier {identifier}")

    return _db_client[identifier]
