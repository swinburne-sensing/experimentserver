import time
import typing

import influxdb
import influxdb.exceptions
import requests.exceptions
import urllib3

import experimentserver
from experimentserver.data.measurement import Measurement
from experimentserver.util.logging import LoggerObject
from experimentserver.util.thread import QueueThread
from experimentserver.data.measurement import MeasurementTarget


# Suppress HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DatabaseException(experimentserver.ApplicationException):
    pass


class _DatabaseClient(LoggerObject, MeasurementTarget):
    _BUFFER_TIMEOUT = 5

    def __init__(self, identifier: str, connect_args: typing.Dict[str, typing.Any]):
        self._identifier = identifier

        LoggerObject.__init__(self, logger_name_postfix=f":{self._identifier}")
        MeasurementTarget.__init__(self, identifier)

        # Client to InfluxDB instance
        self.client = influxdb.InfluxDBClient(**connect_args)

        # Test connection
        try:
            self.get_logger().info(f"Influx DB {self.client.ping()}")
        except requests.exceptions.ConnectionError as exc:
            raise DatabaseException(f"Connection to database server (connection: {self._identifier}) could not be "
                                    f"established, the server or local internet connection may be "
                                    f"unavailable") from exc

        # Buffer for points
        self._point_buffer = []
        self._point_timeout = time.time() + self._BUFFER_TIMEOUT

        # Thread for database writing
        self._event_thread = QueueThread(f"Database:{self._identifier}", event_callback=self._influxdb_event_handle)
        self._event_thread.thread_start()

    def _record(self, measurement: Measurement) -> typing.NoReturn:
        point = {
            'measurement': measurement.measurement_group.value,
            'tags': measurement.get_tags(False),
            'time': int(1000000 * measurement.timestamp.timestamp()),
            'fields': measurement.get_fields(False, False)
        }

        self._event_thread.append(point)

    def _influxdb_event_handle(self, obj):
        if obj is not None:
            # Append to point buffer
            self._point_buffer.append(obj)

        if obj is None or time.time() > self._point_timeout:
            # Update timeout
            self._point_timeout = time.time() + self._BUFFER_TIMEOUT

            if len(self._point_buffer) > 0:
                try:
                    # Write points to database
                    if self.client.write_points(self._point_buffer, time_precision='u'):
                        # Flush buffer
                        self._point_buffer = []
                except influxdb.exceptions.InfluxDBServerError:
                    # Problem server side, retry later
                    self.get_logger().warning(f"Unable to write data points to database (server error)", exc_info=True,
                                              event=False, notify=False)
                except influxdb.exceptions.InfluxDBClientError:
                    # Dump response, will include possibly bad payload
                    self.get_logger().error(f"Unable to write data points to database (client error)", exc_info=True,
                                            event=False, notify=True)

                    # Flush buffer
                    self._point_buffer = []


_db_client: typing.Dict[str, _DatabaseClient] = {}


def setup_database(identifier: str, connect_args):
    if identifier in _db_client:
        raise KeyError(f"Database connection with identifier {identifier} already exists")

    _db_client[identifier] = _DatabaseClient(identifier, connect_args)

    return _db_client[identifier]


def get_database(identifier: typing.Optional[str] = None) -> _DatabaseClient:
    if identifier is None:
        identifier = MeasurementTarget.MEASUREMENT_TARGET_DEFAULT

    if identifier not in _db_client:
        raise KeyError(f"Unknown database identifier {identifier}")

    return _db_client[identifier]