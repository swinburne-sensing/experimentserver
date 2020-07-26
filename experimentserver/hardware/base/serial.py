import abc
import json
import queue
import threading
import typing
from datetime import datetime

import serial
from transitions import EventData

from .core import Hardware
from ..error import CommunicationError
from ..metadata import TYPE_PARAMETER_DICT
from ...data import Measurement, MeasurementTarget
from ...util.thread import CallbackThread, ThreadLock, QueueThread


class SerialHardware(Hardware, metaclass=abc.ABCMeta):
    """  """

    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 serial_args: typing.Optional[typing.Dict[str, typing.Any]] = None):
        """

        :param identifier:
        :param port:
        :param parameters:
        :param serial_args:
        """
        super().__init__(identifier, parameters)

        # Serial port open arguments
        if serial_args is None:
            serial_args = {}

        serial_args['port'] = port

        self._serial_args = serial_args

        # Serial port connection information
        self._serial_lock = ThreadLock(f"{self.get_hardware_identifier(True)}SerialLock", 5)
        self._serial_port: typing.Optional[serial.SerialBase] = None

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(SerialHardware, self).transition_connect(event)

        with self._serial_lock.lock():
            # Open serial port
            try:
                self.get_logger().debug(f"Opening serial port using args: {self._serial_args}")
                self._serial_port = serial.Serial(**self._serial_args)
            except serial.SerialException as exc:
                raise CommunicationError(f"Unable to open serial port {self._serial_args['port']}") from exc

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        with self._serial_lock.lock():
            # Close serial port
            self._serial_port.close()

            self._serial_port = None

        super(SerialHardware, self).transition_disconnect(event)


class SerialJSONHardware(SerialHardware, metaclass=abc.ABCMeta):
    _BLOCK_SIZE = 32
    _BUFFER_MAX_LENGTH = 4096

    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 serial_args: typing.Optional[typing.Dict[str, typing.Any]] = None):
        # Force timeout for reads
        if serial_args is None:
            serial_args = {}

        serial_args['timeout'] = 1

        super().__init__(identifier, port, parameters, serial_args)

        # Payload queue
        self._payload_queue = queue.Queue()

        self._thread_serial_consumer: typing.Optional[CallbackThread] = None
        self._thread_payload_consumer: typing.Optional[QueueThread] = None

    @abc.abstractmethod
    def _handle_object(self, payload: typing.Any, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        pass

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_connect(event)

        # Start serial thread
        self._thread_serial_consumer = CallbackThread(f"{self.get_hardware_identifier(True)}SerialConsumer",
                                                      self._thread_serial_consumer_callback)
        self._thread_payload_consumer = QueueThread(f"{self.get_hardware_identifier(True)}PayloadConsumer",
                                                    self._thread_payload_consumer_event)

        self._thread_serial_consumer.thread_start()
        self._thread_payload_consumer.thread_start()

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Stop data consumer
        self._thread_serial_consumer.thread_stop()
        self._thread_payload_consumer.thread_stop()
        self._thread_serial_consumer.thread_join()
        self._thread_payload_consumer.thread_join()

        self._thread_serial_consumer = None

        super().transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing
        pass

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing
        pass

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Do nothing
        pass

    def _thread_serial_consumer_callback(self):
        serial_buffer = bytearray()

        while not self._thread_serial_consumer.thread_stop_requested():
            try:
                with self._serial_lock.lock():
                    # Break upon disconnection
                    if self._serial_port is None:
                        return

                    if self._serial_port.in_waiting > 0:
                        self.get_logger().debug(f"Read {self._serial_port.in_waiting} bytes")

                        # Read and append to buffer
                        serial_buffer.extend(filter(lambda x: x != ord('\r'), self._serial_port.read(self._BLOCK_SIZE)))
            except serial.SerialException as exc:
                raise CommunicationError('Error reading from serial port') from exc

            if len(serial_buffer) > self._BUFFER_MAX_LENGTH:
                self.get_logger().warning('Buffer overflow', event=False)
                serial_buffer.clear()
                break

            eol_index = serial_buffer.find(b'\n')

            while eol_index >= 0:
                payload = bytes(serial_buffer[:eol_index]), datetime.now()

                self.get_logger().debug(f"Payload found: {payload!r}")

                # Save payload
                self._thread_payload_consumer.append(payload)
                serial_buffer = serial_buffer[eol_index + 1:]

                eol_index = serial_buffer.find(b'\n')

    def _thread_payload_consumer_event(self, payload: typing.Tuple[bytes, datetime]):
        measurements: typing.List[Measurement] = []

        try:
            payload_obj = json.loads(payload[0].decode())

            payload_measurement = self._handle_object(payload_obj, payload[1])

            if payload_measurement is not None:
                measurements.extend(payload_measurement)
            else:
                self.get_logger().info('No measurement in payload')
        except UnicodeDecodeError as exc:
            self.get_logger().warning(f"Could not decode unicode in \"{payload[0]}\", error: {exc}")
        except json.decoder.JSONDecodeError as exc:
            self.get_logger().warning(f"Could not decode JSON in \"{payload[0]}\", error: {exc}")

        if len(measurements) > 0:
            self.get_logger().debug(f"Decoded {len(measurements)} measurement(s)")

            for measurement in measurements:
                MeasurementTarget.record(measurement)
