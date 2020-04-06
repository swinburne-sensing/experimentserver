import time
import typing

from transitions import EventData

from ..base.core import Hardware, ParameterError
from ...data import Measurement, MeasurementGroup, TYPE_UNIT, TYPE_UNIT_OPTIONAL, TYPE_MEASUREMENT_LIST, to_unit
from ...data.humidity import abs_to_rel, rel_to_abs, rel_to_dew
from ...interface.linkam import LinkamSDK, StageValueType


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class T96Controller(Hardware):
    """ Linkam stage controller. Can interface with humidity generator and LN cooler. """

    def __init__(self, *args, humidity_room_temp: TYPE_UNIT_OPTIONAL = None, sdk_debug: bool = False,
                 sdk_log_path: typing.Optional[str] = None, sdk_license_path: typing.Optional[str] = None, **kwargs):
        """ Create new instance.

        :param args:
        :param sdk_log_path:
        :param sdk_license_path:
        :param kwargs:
        """
        super(T96Controller, self).__init__(*args, **kwargs)

        # Room temperature for humidity calculations
        self._humidity_room_temp = to_unit(humidity_room_temp, 'degC')

        self._sdk_log_path = sdk_log_path
        self._sdk_license_path = sdk_license_path

        # Load Linkam SDK
        self._sdk = LinkamSDK(self._sdk_log_path, self._sdk_license_path, sdk_debug)

        self._handle: typing.Optional[LinkamSDK.LinkamConnection] = None

        self._has_heater = False
        self._has_humidity = False

    @Hardware.register_measurement(description='Temperature/humidity measurements', force=True)
    def measure(self) -> TYPE_MEASUREMENT_LIST:
        assert self._handle is not None

        # Delay since Linkam only produced measurements evert ~0.1s
        time.sleep(0.1)

        payload = []

        controller = self._handle.get_controller_config()
        program_state = self._handle.get_program_state()

        if controller.flags.supportsHeater:
            payload.append(Measurement(self, MeasurementGroup.TEMPERATURE, {
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
            }))

            if not self._has_heater:
                self.get_logger().warning('Heater connected')
                self._has_heater = True
        elif self._has_heater:
            self.get_logger().error('Heater disconnected')
            self._has_heater = False

        if controller.flags.humidityReady:
            humidity = self._handle.get_humidity_details()

            humidity_fields = {
                'stage_rh': to_unit(humidity.rh, 'pct'),
                'stage_rh_temperature': to_unit(humidity.rhTemp, 'degC'),
                'stage_rh_setpoint': to_unit(humidity.rhSetpoint, 'pct'),
                'generator_rh': to_unit(humidity.tubePercent, 'pct'),
                'generator_rh_setpoint': to_unit(humidity.rhSetpoint, 'pct'),
                'water_temperature': to_unit(humidity.waterTemp, 'degC'),
                'water_temperature_setpoint': to_unit(humidity.waterSetpoint, 'degC'),
                'generator_column': humidity.status.flags.colSel,
                'generator_drying': humidity.status.flags.dessicantDryMode,
                'generator_running': humidity.status.flags.started
            }

            humidity_tags = None

            # Calculate absolute humidity
            humidity_fields['stage_abs'] = rel_to_abs(humidity_fields['stage_rh_temperature'],
                                                      humidity_fields['stage_rh'])

            humidity_fields['stage_dew'] = rel_to_dew(humidity_fields['stage_rh_temperature'],
                                                      humidity_fields['stage_rh'])

            # RH at room temperature
            if self._humidity_room_temp is not None:
                humidity_fields['stage_room_rh'] = abs_to_rel(self._humidity_room_temp, humidity_fields['stage_abs'])

                humidity_tags = {
                    'stage_room_temperature': self._humidity_room_temp
                }

            payload.append(Measurement(self, MeasurementGroup.HUMIDITY, humidity_fields, tags=humidity_tags))

            if not self._has_humidity:
                self.get_logger().warning('Humidity measurements available')
                self._has_humidity = True
        elif self._has_humidity:
            self.get_logger().error('Humidity measurement no longer available')
            self._has_humidity = False

        return payload

    @Hardware.register_parameter(description='Stage humidity')
    def set_humidity(self, humidity):
        if not self._has_humidity:
            raise ParameterError('Humidity not supported')

        humidity = to_unit(humidity, 'pct', magnitude=True) * 100

        if not self._handle.set_value(StageValueType.HUMIDITY_SETPOINT, humidity):
            raise ParameterError('Cannot configure humidity set point')

    @Hardware.register_parameter(description='Stage temperature (0 = off)')
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
        super(T96Controller, self).transition_connect(event)

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

        if controller.flags.supportsHeater:
            self.get_logger().info(f"Heating supported")
            self._has_heater = True

        if controller.flags.lnpReady:
            self.get_logger().info(f"Cooling supported")

        if controller.flags.humidityReady:
            self.get_logger().info(f"Humidity supported")
            self._has_humidity = True

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._handle.close()
        self._handle = None

        super(T96Controller, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Force default humidity and enable
        if self._has_humidity:
            self.set_humidity(0)
            self._handle.enable_humidity(True)

        super(T96Controller, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Stop heating and humidity regulation
        self._handle.enable_heater(False)
        self._handle.enable_humidity(False)

        super(T96Controller, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(T96Controller, self).transition_error(event)
