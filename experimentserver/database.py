import typing

import influxdb_client
import urllib3
from influxdb_client.client.write_api import ASYNCHRONOUS

import experimentserver
from experimentserver.data.measurement import Measurement
from experimentserver.util.logging import LoggerObject
from experimentserver.data.measurement import MeasurementTarget


# Suppress HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DatabaseError(experimentserver.ApplicationException):
    pass


class _InfluxDBv2Client(LoggerObject, MeasurementTarget):
    _INFLUXDB_RETRY = urllib3.Retry()

    def __init__(self, identifier: str, connect_args: typing.MutableMapping[str, typing.Any]):
        self._identifier = identifier

        LoggerObject.__init__(self, logger_name_postfix=f":{self._identifier}")
        MeasurementTarget.__init__(self, identifier)

        self.bucket = connect_args.pop('bucket')
        self.client = influxdb_client.InfluxDBClient(**connect_args, retries=self._INFLUXDB_RETRY)

        # Test connection
        health = self.client.health()

        if health.status == 'pass':
            self.get_logger().info(f"InfluxDB health: OK {health.version}")
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
        for field, value in measurement.get_fields(False, False).items():
            point.field(field, value)

        # Convert tags to strings
        for tag, value in measurement.get_tags().items():
            if isinstance(value, bool):
                # InfluxDB style boolean
                point.tag(tag, 'true' if value else 'false')
            elif not isinstance(value, str):
                point.tag(tag, str(value))
            else:
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
