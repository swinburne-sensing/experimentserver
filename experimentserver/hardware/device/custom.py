import time
import typing
from datetime import datetime, timedelta

from transitions import EventData

from .. import SerialHardware, SerialJSONHardware
from ..base.enum import HardwareEnum, TYPE_ENUM_CAST
from ..metadata import TYPE_PARAMETER_DICT
from ...data import TYPE_FIELD_DICT, Measurement, MeasurementGroup, units


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class AccelMonitor(SerialJSONHardware):
    def __init__(self, identifier: str, port: str):
        super(AccelMonitor, self).__init__(identifier, port, serial_args={
            'baudrate': 115200
        })

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Accelerometer and temperature sensor'

    def _handle_object(self, payload: typing.Any, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        measurements = []

        # Handle lists of results
        if type(payload) is list:
            for payload_entry in payload:
                measurements_entry = self._handle_object(payload_entry, received)

                if measurements_entry is not None:
                    measurements.extend(measurements_entry)

            return measurements

        if 'time' not in payload:
            self.get_logger().warning(f"Missing time field in payload: {payload}")
            return None

        if 'acceleration' in payload:
            # Length of payload must be divisible by 3
            assert (len(payload['acceleration']) % 3) == 0

            # Calculate number of points
            rate = payload['rate']
            scale = payload['scale']
            period = timedelta(seconds=1 / rate)
            timestamp = received

            # Get last timestamp in series, then work forwards
            while len(payload['acceleration']) > 0:
                # Get last three values from end of the list
                z = payload['acceleration'].pop(-1) / pow(2, 15) * scale
                y = payload['acceleration'].pop(-1) / pow(2, 15) * scale
                x = payload['acceleration'].pop(-1) / pow(2, 15) * scale

                measurements.append(Measurement(self, MeasurementGroup.ACCELERATION, {
                    'accelerometer_x': x,
                    'accelerometer_y': y,
                    'accelerometer_z': z,
                }, timestamp))

                # Offset timestamp
                timestamp -= period
        elif 'temperature' in payload:
            measurements.append(Measurement(self, MeasurementGroup.TEMPERATURE, {
                'rtd_raw': payload['rtd'],
                'rtd_temperature': payload['temperature']
            }, received))
        else:
            self.get_logger().warning(f"Unrecognised structure in payload: {payload}")

        return measurements


class GenericSerial(SerialHardware):
    def __init__(self, identifier: str, port: str, enable_receive: bool = True, enable_send: bool = True):
        super().__init__(identifier, port, None, {
            'baudrate': 115200
        })

        self._enable_receive = enable_receive
        self._enable_send = enable_send

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Generic Arduino Device'

    @SerialHardware.register_parameter(description='Send raw command')
    def send(self, cmd: str):
        # Encode command
        cmd = bytes(cmd, "utf-8").decode("unicode_escape").encode()

        with self._serial_lock.lock():
            # Send command
            self._serial_port.write(cmd)

            # Wait for response
            response = self._serial_port.read_until()
            response = response.strip()

        self.get_logger().info(f"Response: {response}")

    @SerialHardware.register_measurement(description='Fetch raw data', measurement_group=MeasurementGroup.RAW,
                                         force=True)
    def receive(self) -> TYPE_FIELD_DICT:
        if not self._enable_receive:
            self.sleep(1, 'rate limit, receive disabled')
            return {}

        with self._serial_lock.lock():
            response = self._serial_port.read_until()

        response = response.strip()

        if response == 'ERROR':
            return {}

        response_value = float(response)

        return {
            'value': response_value
        }

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GenericSerial, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GenericSerial, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GenericSerial, self).transition_error(event)


class GrblController(SerialHardware):
    def __init__(self, identifier: str, port: str):
        super().__init__(identifier, port, None, {
            'baudrate': 115200
        })

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'grbl-compatible Motion Controller'

    def send_cmd(self, cmd: str):
        pass

    def increment_axis(self, axis: str, distance: float):
        pass

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GrblController, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GrblController, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GrblController, self).transition_error(event)


class ValvePosition(HardwareEnum):
    POSITION_A = 'A'
    POSITION_B = 'B'

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.List[typing.Any]]]:
        return {
            cls.POSITION_A: ['a', 0, '0', 'direct'],
            cls.POSITION_B: ['b', 1, '1', 'bypass', 'vent']
        }

    @classmethod
    def _get_description_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.POSITION_A: 'Position A',
            cls.POSITION_B: 'Position B'
        }

    @classmethod
    def _get_command_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.POSITION_A: 'A',
            cls.POSITION_B: 'B'
        }


class ValveController(SerialHardware):
    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(ValveController, self).__init__(identifier, port, parameters, {
            'baudrate': 115200
        })

    @SerialHardware.register_parameter(description='Valve position')
    def set_position(self, position: typing.Union[TYPE_ENUM_CAST, ValvePosition]):
        channel = ValvePosition.from_input(position)

        with self._serial_lock.lock():
            # Send command
            self._serial_port.write(channel.get_command().encode())

            # Wait for response
            response = self._serial_port.read_until()
            response = response.strip()

        self.get_logger().info(f"Valve position: {response}")

    @SerialHardware.register_measurement(description='Valve position', measurement_group=MeasurementGroup.VALVE,
                                         force=True)
    def get_position(self) -> TYPE_FIELD_DICT:
        # Slow down sample rate
        self.sleep(0.5, 'rate limit, infrequent change')

        with self._serial_lock.lock():
            self._serial_port.write('?'.encode())
            response = self._serial_port.read_until()
            response = response.strip()

        response_position = ValvePosition.from_input(response)

        field_dict = {
            'valve_position': response_position.value
        }

        for position in ValvePosition:
            field_dict[f"is_{position.value}"] = 1 if response_position == position else 0

        return field_dict

    @staticmethod
    def get_hardware_class_description() -> str:
        return '2-way Valve Controller'

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(ValveController, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(ValveController, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(ValveController, self).transition_error(event)
