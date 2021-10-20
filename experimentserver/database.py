import typing
from datetime import datetime, timedelta, timezone

import influxdb_client
import urllib3
from experimentlib.logging.classes import LoggedAbstract
from influxdb_client.client.write_api import ASYNCHRONOUS

import experimentserver
from experimentserver.data import to_unit, Quantity
from experimentserver.data.measurement import Measurement, MeasurementTarget


# Suppress HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DatabaseError(experimentserver.ApplicationException):
    pass


class _InfluxDBv2Client(LoggedAbstract, MeasurementTarget):
    _INFLUXDB_RETRY = urllib3.Retry()

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

    def __del__(self):
        if hasattr(self, 'write_api') and self.write_api is not None:
            self.write_api.close()
            self.write_api = None

    def _record(self, measurement: Measurement) -> typing.NoReturn:
        point = influxdb_client.Point(measurement.measurement_group.value).time(measurement.timestamp)

        # Add fields
        for field, value in measurement.get_fields().items():
            point.field(field, value)

        # Convert tags to strings
        for tag, value in measurement.get_tags().items():
            if isinstance(value, bool):
                # InfluxDB style boolean
                value = 'true' if value else 'false'
            elif isinstance(value, Quantity):
                value = str(value)
            elif isinstance(value, timedelta):
                value = str(to_unit(value.total_seconds(), 's'))
            elif isinstance(value, datetime):
                # Add UTC referenced tag
                point.tag(tag + '_utc', value.astimezone(timezone.utc).isoformat())

                value = value.isoformat()
            elif not isinstance(value, str):
                self.logger().warning(f"Casting tag {tag} (repr: {value!r}, type: {type(value)}) to string")
                value = str(value)

            if len(value) > 0:
                point.tag(tag, value)

        self.write_api.write(self.bucket, record=point)


_db_client: typing.Dict[str, _InfluxDBv2Client] = {}


def setup_database(identifier: str, connect_args: typing.MutableMapping[str, typing.Any], debug: bool):
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
