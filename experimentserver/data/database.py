import time
import typing
from datetime import datetime

import influxdb
import urllib3

from experimentserver.data import TYPING_RECORD, TYPING_TAGS, RecordType
from experimentserver.util.logging import LoggerObject
from experimentserver.util.thread import QueueThread
from .export import Exporter


# Suppress HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DatabaseClient(LoggerObject, Exporter):
    _BUFFER_TIMEOUT = 5

    def __init__(self, identifier: str, connect_args: typing.Dict[str, typing.Any]):
        self._identifier = identifier

        super().__init__(logger_append=f":{self._identifier}")

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

        # Thread for database writing
        self._client_thread = QueueThread('Database', self._influxdb_queue_handle, name_append=f":{self._identifier}")
        self._client_thread.start()

    def _record(self, timestamp: datetime, record_type: RecordType, record: TYPING_RECORD, tags: TYPING_TAGS):
        point = {
            'measurement': record_type.value,
            'tags': tags,
            'time': int(1000 * timestamp.timestamp()),
            'fields': record
        }

        self._client_thread.append(point)

    def _influxdb_queue_handle(self, obj):
        if obj is not None:
            # Append to point buffer
            self._point_buffer.append(obj)

        if obj is None or time.time() > self._point_timeout:
            if len(self._point_buffer) > 0:
                # Write points to database
                self.client.write_points(self._point_buffer, time_precision='ms')

                # Flush buffer
                self._point_buffer = []

            # Update timeout
            self._point_timeout = time.time() + self._BUFFER_TIMEOUT


_db_client: typing.Dict[str, DatabaseClient] = {}


def setup_database(identifier: str, connect_args):
    if identifier in _db_client:
        raise KeyError(f"Database connection with identifier {identifier} already exists")

    _db_client[identifier] = DatabaseClient(identifier, connect_args)

    return _db_client[identifier]


def get_database(identifier: typing.Optional[str] = None) -> DatabaseClient:
    if identifier is None:
        identifier = 'default'

    if identifier not in _db_client:
        raise KeyError(f"Unknown database identifier {identifier}")

    return _db_client[identifier]
