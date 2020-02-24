import typing

from transitions import EventData

from ..base.core import Hardware, ParameterError
from experimentserver.data import to_unit, MeasurementGroup, TYPE_FIELD_DICT, TYPE_UNIT
from experimentserver.data.humidity import rel_to_abs, rel_to_dew
from experimentserver.linkam.sdk import LinkamSDK, StageValueType


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class T96ControllerTemperature(Hardware):
    """  """

    def __init__(self, *args, sdk_log_path: typing.Optional[str] = None,
                 sdk_license_path: typing.Optional[str] = None, **kwargs):
        """

        :param args:
        :param sdk_log_path:
        :param sdk_license_path:
        :param kwargs:
        """
        super(T96ControllerTemperature, self).__init__(*args, **kwargs)

        self._sdk_log_path = sdk_log_path
        self._sdk_license_path = sdk_license_path

        # Load Linkam SDK
        self._sdk = None
        self._handle: typing.Optional[LinkamSDK.LinkamConnection] = None

        self._has_heater = False
        self._has_humidity = False

    @Hardware.register_measurement(description='Stage temperature', measurement_group=MeasurementGroup.TEMPERATURE,
                                   force=True)
    def measure_temperature(self) -> TYPE_FIELD_DICT:
        program_state = self._handle.get_program_state()

        return {
            'heater_temperature_ch1': self._handle.get_value(StageValueType.HEATER1_TEMP),
            'heater_temperature_ch2': self._handle.get_value(StageValueType.HEATER2_TEMP),
            'heater_temperature_ch3': self._handle.get_value(StageValueType.HEATER3_TEMP),
            'heater_temperature_ch4': self._handle.get_value(StageValueType.HEATER4_TEMP),
            'heater_temperature_ramp': self._handle.get_value(StageValueType.HEATER_RATE),
            'heater_temperature_setpoint': self._handle.get_value(StageValueType.HEATER_SETPOINT),
            'heater_output_ch1': self._handle.get_value(StageValueType.HEATER1_POWER),
            'heater_output_ch2': self._handle.get_value(StageValueType.HEATER2_POWER),
            'heater_voltage': to_unit(program_state.voltage, 'volt'),
            'heater_current': to_unit(program_state.current, 'amp'),
            'heater_power': to_unit(program_state.pwm, 'watt')
        }

    @Hardware.register_measurement(description='Stage humidity', measurement_group=MeasurementGroup.HUMIDITY)
    def measure_humidity(self) -> TYPE_FIELD_DICT:
        humidity = self._handle.get_humidity_details()

        humidity_fields = {
            'stage_rh': to_unit(humidity.rh, 'pct'),
            'stage_rh_temperature': to_unit(humidity.rhTemp, 'degC'),
            'stage_rh_setpoint': to_unit(humidity.tubeSetpoint, 'pct'),
            'generator_rh': to_unit(humidity.tubePercent, 'pct'),
            'generator_rh_setpoint': to_unit(humidity.rhSetpoint, 'pct'),
            'water_temperature': to_unit(humidity.waterTemp, 'degC'),
            'water_temperature_setpoint': to_unit(humidity.waterSetpoint, 'degC')
        }

        # Calculate absolute humidity
        humidity_fields['stage_abs'] = rel_to_abs(humidity_fields['stage_rh_temperature'],
                                                  humidity_fields['stage_rh'])

        humidity_fields['stage_dew'] = rel_to_dew(humidity_fields['stage_rh_temperature'],
                                                  humidity_fields['stage_rh'])

        return humidity_fields

    @Hardware.register_parameter(description='Stage humidity')
    def set_humidity(self, humidity):
        if not self._has_humidity:
            raise ParameterError('Humidity not supported')

        humidity = to_unit(humidity, 'pct', magnitude=True) * 100

        if humidity >= 0:
            if not self._handle.set_value(StageValueType.HUMIDITY_SETPOINT, humidity):
                raise ParameterError('Cannot configure humidity set point')

            self._handle.enable_humidity(True)
        else:
            self._handle.enable_humidity(False)

    @Hardware.register_parameter(description='Stage temperature')
    def set_temperature(self, temperature: TYPE_UNIT):
        if not self._has_heater:
            raise ParameterError('Heating not supported')

        temperature = to_unit(temperature, 'degC', magnitude=True)

        if temperature > 0:
            if not self._handle.set_value(StageValueType.HEATER_SETPOINT, temperature):
                raise ParameterError('Cannot configure temperature set point')

            if not self._handle.enable_heater(True):
                raise ParameterError('Cannot start heater')
        else:
            self._handle.enable_heater(False)

    @Hardware.register_parameter(description='Stage temperature ramp rate')
    def set_temperature_ramp(self, rate: TYPE_UNIT):
        if not self._has_heater:
            raise ParameterError('Heating not supported')

        rate = to_unit(rate, 'degC/min', magnitude=True)

        if not self._handle.set_value(StageValueType.HEATER_RATE, rate):
            raise ParameterError('Cannot configure temperature ramp rate')

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Linkam T96 Stage Controller'

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(T96ControllerTemperature, self).transition_connect(event)

        # Setup SDK
        self._sdk = LinkamSDK(self._sdk_log_path, self._sdk_license_path)

        # Connect over USB
        self._handle = self._sdk.connect_usb()

        # Get metadata
        self.get_logger().info(f"Controller name: {self._handle.get_controller_name()}")
        self.get_logger().info(f"Controller serial: {self._handle.get_controller_serial()}")
        self.get_logger().info(f"Controller hardware version: {self._handle.get_controller_hardware_version()}")
        self.get_logger().info(f"Controller firmware version: {self._handle.get_controller_firmware_version()}")

        self.get_logger().info(f"Humidity sensor name: {self._handle.get_humidity_controller_sensor_name()}")
        self.get_logger().info(f"Humidity sensor serial: {self._handle.get_humidity_controller_sensor_serial()}")
        self.get_logger().info(f"Humidity sensor hardware version: "
                               f"{self._handle.get_humidity_controller_sensor_hardware_version()}")

        self.get_logger().info(f"Stage name: {self._handle.get_stage_name()}")
        self.get_logger().info(f"Stage serial: {self._handle.get_stage_serial()}")
        self.get_logger().info(f"Stage hardware version: {self._handle.get_stage_hardware_version()}")
        self.get_logger().info(f"Stage firmware version: {self._handle.get_stage_firmware_version()}")

        controller = self._handle.get_controller_config()

        if controller.supportsHeater:
            self.get_logger().info(f"Heating supported")
            self._has_heater = True

        if controller.lnpReady:
            self.get_logger().info(f"Cooling supported")

        if controller.humidityReady:
            self.get_logger().info(f"Humidity supported")
            self._has_humidity = True

        print(1)

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._handle = None
        self._sdk = None

        super(T96ControllerTemperature, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(T96ControllerTemperature, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Stop heating and humidity regulation
        self._handle.enable_heater(False)
        self._handle.enable_humidity(False)

        super(T96ControllerTemperature, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(T96ControllerTemperature, self).transition_error(event)
