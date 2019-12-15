import time
import threading
import typing
from datetime import datetime

import influxdb
import influxdb.exceptions
import urllib3

from experimentserver.data import TYPE_FIELD_DICT, TYPE_TAG_DICT, MeasurementGroup, is_unit
from experimentserver.util.logging import LoggerObject
from experimentserver.util.thread import QueueThread
from .export import EXPORTER_DEFAULT, ExporterTarget


# Suppress HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class _DatabaseClient(LoggerObject, ExporterTarget):
    _BUFFER_TIMEOUT = 5

    def __init__(self, identifier: str, connect_args: typing.Dict[str, typing.Any]):
        self._identifier = identifier

        super().__init__(logger_name_postfix=f":{self._identifier}", exporter_target_name=identifier)

        # Client to InfluxDB instance
        self.client = influxdb.InfluxDBClient(**connect_args)

        # # Hack to fix HTTPS issues
        # if 'https://' in self.client._session.adapters:
        #     pool_kwargs = self.client._session.adapters['https://'].poolmanager.connection_pool_kw
        #
        #     pool_kwargs.update({
        #         'cert_reqs': 'CERT_REQUIRED',
        #         'ca_certs': certifi.where()
        #     })

        # Buffer for points
        self._point_buffer = []
        self._point_timeout = time.time() + self._BUFFER_TIMEOUT
        self._point_lock = threading.RLock()

        # Thread for database writing
        self._event_thread = QueueThread(f"Database:{self._identifier}", event_callback=self._influxdb_event_handle)
        self._event_thread.start()

    def record(self, timestamp: datetime, measurement: MeasurementGroup, fields: TYPE_FIELD_DICT, tags: TYPE_TAG_DICT):
        # Sanitise fields for insertion to database
        fields = {key: field.magnitude if is_unit(field) else field for key, field in fields.items()}

        point = {
            'measurement': measurement.value,
            'tags': tags,
            'time': int(1000000 * timestamp.timestamp()),
            'fields': fields
        }

        self._event_thread.append(point)

    def _influxdb_event_handle(self, obj):
        with self._point_lock:
            if obj is not None:
                # Append to point buffer
                self._point_buffer.append(obj)

            if obj is None or time.time() > self._point_timeout:
                if len(self._point_buffer) > 0:
                    # Write points to database
                    try:
                        if self.client.write_points(self._point_buffer, time_precision='u'):
                            # Flush buffer
                            self._point_buffer = []
                    except influxdb.exceptions.InfluxDBClientError:
                        self._logger.warning(f"Unable to write data points to database", notify=True)

                # Update timeout
                self._point_timeout = time.time() + self._BUFFER_TIMEOUT


_db_client: typing.Dict[str, _DatabaseClient] = {}


def setup_database(identifier: str, connect_args):
    if identifier in _db_client:
        raise KeyError(f"Database connection with identifier {identifier} already exists")

    _db_client[identifier] = _DatabaseClient(identifier, connect_args)

    return _db_client[identifier]


def get_database(identifier: typing.Optional[str] = None) -> _DatabaseClient:
    if identifier is None:
        identifier = EXPORTER_DEFAULT

    if identifier not in _db_client:
        raise KeyError(f"Unknown database identifier {identifier}")

    return _db_client[identifier]
