import abc
import json
import typing
from datetime import datetime

import serial
from experimentlib.util.time import now
from transitions import EventData

from .core import Hardware
from ..error import CommunicationError
from ..metadata import TYPE_PARAMETER_DICT
from ...util.thread import CallbackThread, ThreadLock, QueueThread, ThreadException
from experimentserver.measurement import Measurement, MeasurementTarget


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

    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super(SerialHardware, self).transition_connect(event)

        with self._serial_lock.lock('transition_connect'):
            # Open serial port
            try:
                self.logger().debug(f"Opening serial port using args: {self._serial_args}")
                self._serial_port = serial.Serial(**self._serial_args)
            except serial.SerialException as exc:
                raise CommunicationError(f"Unable to open serial port {self._serial_args['port']}") from exc

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        with self._serial_lock.lock('transition_disconnect'):
            if self._serial_port:
                # Close serial port
                self._serial_port.close()

                self._serial_port = None

        super(SerialHardware, self).transition_disconnect(event)


class SerialStreamHardware(SerialHardware, metaclass=abc.ABCMeta):
    # Maximum bytes to request in a single read
    _BLOCK_SIZE = 32

    # Maximum buffer length
    _BUFFER_MAX_LENGTH = 4096

    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 serial_args: typing.Optional[typing.Dict[str, typing.Any]] = None, eol_char: bytes = b'\n'):
        self._eol_char = eol_char

        # Force timeout for reads
        if serial_args is None:
            serial_args = {}

        serial_args['timeout'] = 1

        super().__init__(identifier, port, parameters, serial_args)

        self._thread_serial_consumer: typing.Optional[CallbackThread] = None
        self._thread_payload_consumer: typing.Optional[QueueThread] = None

    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_connect(event)

        # Start serial thread
        self._thread_serial_consumer = CallbackThread(f"{self.get_hardware_identifier(True)}SerialConsumer",
                                                      self._thread_serial_consumer_callback)

        # Start consumer thread
        self._thread_payload_consumer = QueueThread(f"{self.get_hardware_identifier(True)}PayloadConsumer",
                                                    self._thread_payload_consumer_event)

        # Event consumer must start before serial thread
        self._thread_payload_consumer.thread_start()
        self._thread_serial_consumer.thread_start()

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        if self._thread_serial_consumer is not None:
            if self._thread_serial_consumer:
                # Stop consumers
                self._thread_serial_consumer.thread_stop()
                self._thread_serial_consumer.thread_join()
                self._thread_serial_consumer = None

            if self._thread_payload_consumer:
                self._thread_payload_consumer.thread_stop()
                self._thread_payload_consumer.thread_join()
                self._thread_payload_consumer = None

        super().transition_disconnect(event)

    def _thread_serial_consumer_callback(self):
        serial_buffer = bytearray()

        while not self._thread_serial_consumer.thread_stop_requested():
            try:
                with self._serial_lock.lock('_thread_serial_consumer_callback'):
                    # Break upon disconnection
                    if self._serial_port is None:
                        return

                    # noinspection PyUnresolvedReferences
                    if self._serial_port.in_waiting > 0:
                        # Read and append to buffer
                        serial_buffer.extend(self._serial_port.read(self._BLOCK_SIZE))
            except serial.SerialException as exc:
                raise CommunicationError('Error reading from serial port') from exc

            if len(serial_buffer) > self._BUFFER_MAX_LENGTH:
                self.logger().warning('Buffer overflow')
                serial_buffer.clear()
                break

            eol_index = serial_buffer.find(self._eol_char)

            while eol_index >= 0:
                payload = bytes(serial_buffer[:eol_index])

                self.logger().debug(f"Payload found: {payload!r}")

                # Place in consumer queue
                try:
                    self._thread_payload_consumer.append((payload, now()))
                except ThreadException:
                    self.logger().warning('Payload consumer thread not running')

                # Trim buffer
                serial_buffer = serial_buffer[eol_index + 1:]

                # Search for another line
                eol_index = serial_buffer.find(b'\n')

    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
        # Do nothing
        pass

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        # Do nothing
        pass

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        # Do nothing
        pass

    @abc.abstractmethod
    def _thread_payload_consumer_event(self, payload: typing.Tuple[bytes, datetime]) -> None:
        pass


class SerialStringHardware(SerialStreamHardware, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _handle_payload(self, payload: str, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        pass

    def _thread_payload_consumer_event(self, payload: typing.Optional[typing.Tuple[bytes, datetime]]) -> None:
        if payload is None:
            self.logger().error('No payload')
            return

        # Attempt to decode payload
        try:
            payload_str = payload[0].decode().strip()

            # Attempt to handle payload
            measurements = self._handle_payload(payload_str, payload[1])

            # If nothing decoded, log that
            if measurements is None:
                self.logger().debug('No measurements generated from payload')
            else:
                self.logger().debug(f"Decoded {len(measurements)} measurement(s)")

                for measurement in measurements:
                    MeasurementTarget.record(measurement)
        except TypeError as exc:
            self.logger().warning(f"Bad type in payload {payload[0]!r}, error: {exc}")
        except UnicodeDecodeError as exc:
            self.logger().warning(f"Could not decode unicode in {payload[0]!r}, error: {exc}")


class SerialJSONHardware(SerialStringHardware, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _handle_object(self, payload: typing.Any, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        pass

    def _handle_payload(self, payload: str, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        try:
            # Attempt to decode JSON
            payload_obj = json.loads(payload)

            return self._handle_object(payload_obj, received)
        except json.decoder.JSONDecodeError as exc:
            self.logger().warning(f"Could not decode JSON in \"{payload[0]}\", error: {exc}")
            return None
