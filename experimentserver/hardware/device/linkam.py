import os.path
import typing

from transitions import EventData
from experimentlib.data.humidity import abs_to_rel, rel_to_abs, rel_to_dew, unit_abs
from experimentlib.data.unit import T_PARSE_QUANTITY, parse, registry

from ..base.core import Hardware, ParameterError
from experimentserver.interface.linkam import LinkamSDK, StageValueType, SDK_PATH
from experimentserver.interface.linkam.license import fetch_license
from experimentserver.measurement import T_MEASUREMENT_SEQUENCE, Measurement, MeasurementGroup


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class T96Controller(Hardware):
    """ Linkam stage controller. Can interface with humidity generator and LN cooler. """

    _REFRESH_PERIOD = 0.25

    def __init__(self, *args, humidity_room_temp: typing.Optional[T_PARSE_QUANTITY] = None, sdk_debug: bool = False,
                 sdk_log_path: typing.Optional[str] = None, sdk_license_path: typing.Optional[str] = None, **kwargs):
        """ Create new instance.

        :param args:
        :param sdk_log_path:
        :param sdk_license_path:
        :param kwargs:
        """
        super(T96Controller, self).__init__(*args, **kwargs)

        # Room temperature for humidity calculations
        self._humidity_room_temp = parse(humidity_room_temp, registry.degC) if humidity_room_temp is not None else None

        self._sdk_log_path = sdk_log_path
        self._sdk_license_path = sdk_license_path

        # Generate temporary license file if none is provided
        if self._sdk_license_path is None:
            license_path = os.path.join(SDK_PATH, 'Linkam.lsk')

            with open(license_path, 'wb') as license_file:
                fetch_license(license_file)

        # Load Linkam SDK
        self._sdk = LinkamSDK(self._sdk_log_path, self._sdk_license_path, sdk_debug)

        self._handle: typing.Optional[LinkamSDK.LinkamConnection] = None

        self._has_heater = False
        self._has_humidity = False

    @Hardware.register_measurement(description='Temperature/humidity measurements', force=True)
    def measure(self) -> T_MEASUREMENT_SEQUENCE:
        assert self._handle is not None

        # Delay since Linkam only produces measurements data every ~0.1s
        self.sleep(self._REFRESH_PERIOD, 'rate limit, infrequent update')

        payload = []

        controller = self._handle.get_controller_config()
        program_state = self._handle.get_program_state()

        if controller.flags.supportsHeater:
            payload.append(Measurement(self, MeasurementGroup.TEMPERATURE, {
                'heater_temperature_ch1': parse(self._handle.get_value(StageValueType.HEATER1_TEMP), registry.degC,
                                                mag_round=2),
                # 'heater_temperature_ch2': self._handle.get_value(StageValueType.HEATER2_TEMP),
                # 'heater_temperature_ch3': self._handle.get_value(StageValueType.HEATER3_TEMP),
                # 'heater_temperature_ch4': self._handle.get_value(StageValueType.HEATER4_TEMP),
                'heater_temperature_ramp': self._handle.get_value(StageValueType.HEATER_RATE),
                'heater_temperature_setpoint': self._handle.get_value(StageValueType.HEATER_SETPOINT),
                'heater_output_ch1': round(self._handle.get_value(StageValueType.HEATER1_POWER), 2),
                # 'heater_output_ch2': self._handle.get_value(StageValueType.HEATER2_POWER),
                'heater_voltage': parse(program_state.voltage, registry.V, mag_round=3),
                'heater_current': parse(program_state.current, registry.A, mag_round=3),
                'heater_power': parse(program_state.pwm, registry.W, mag_round=3)
            }))

            if not self._has_heater:
                self.logger().warning('Heater connected')
                self._has_heater = True
        elif self._has_heater:
            self.logger().error('Heater disconnected')
            self._has_heater = False

        if controller.flags.humidityReady:
            humidity = self._handle.get_humidity_details()

            humidity_fields = {
                'stage_rh': parse(humidity.rh, registry.pct, mag_round=2),
                'stage_rh_temperature': parse(humidity.rhTemp, registry.degC, mag_round=2),
                'stage_rh_setpoint': parse(humidity.rhSetpoint, registry.pct),
                'generator_rh': parse(humidity.tubePercent, registry.pct, mag_round=2),
                'generator_rh_setpoint': parse(humidity.rhSetpoint, registry.pct),
                'water_temperature': parse(humidity.waterTemp, registry.degC, mag_round=2),
                'water_temperature_setpoint': parse(humidity.waterSetpoint, registry.degC),
                # 'generator_column': humidity.status.flags.colSel,
                # 'generator_drying': humidity.status.flags.dessicantDryMode,
                # 'generator_running': humidity.status.flags.started
            }

            humidity_tags = None

            # Calculate absolute humidity
            try:
                if humidity_fields['stage_rh'] > 0:
                    humidity_fields['stage_abs'] = rel_to_abs(humidity_fields['stage_rh_temperature'],
                                                              humidity_fields['stage_rh'])

                    humidity_fields['stage_dew'] = rel_to_dew(humidity_fields['stage_rh_temperature'],
                                                              humidity_fields['stage_rh'])
                else:
                    self.logger().debug("Zero relative humidity")
                    humidity_fields['stage_abs'] = parse(0, unit_abs)
                                                          
                # RH at room temperature
                if self._humidity_room_temp is not None:
                    humidity_fields['stage_room_rh'] = abs_to_rel(self._humidity_room_temp,
                                                                  humidity_fields['stage_abs'])

                    humidity_tags = {
                        'stage_room_temperature': self._humidity_room_temp
                    }
            except (ValueError, NotImplementedError):
                self.logger().warning(f"Error during humidity calculation: {humidity_fields}")
                # raise

            payload.append(Measurement(self, MeasurementGroup.HUMIDITY, humidity_fields, tags=humidity_tags))

            if not self._has_humidity:
                self.logger().warning('Humidity measurements available')
                self._has_humidity = True
        elif self._has_humidity:
            self.logger().error('Humidity measurement no longer available')
            self._has_humidity = False

        return payload

    @Hardware.register_parameter(description='Stage humidity')
    def set_humidity(self, humidity):
        if not self._has_humidity:
            raise ParameterError('Humidity not supported')

        humidity = parse(humidity, registry.pct).magnitude * 100

        if not self._handle.set_value(StageValueType.HUMIDITY_SETPOINT, humidity):
            raise ParameterError('Cannot configure humidity set point')

    @Hardware.register_parameter(description='Stage temperature (0 = off)')
    def set_temperature(self, temperature: T_PARSE_QUANTITY):
        if not self._has_heater:
            raise ParameterError('Heating not supported')

        temperature = parse(temperature, registry.degC).magnitude

        if temperature > 0:
            if not self._handle.set_value(StageValueType.HEATER_SETPOINT, temperature):
                raise ParameterError('Cannot configure temperature set point')

            if not self._handle.enable_heater(True):
                raise ParameterError('Cannot start heater')
        else:
            self._handle.enable_heater(False)

    @Hardware.register_parameter(description='Stage temperature ramp rate')
    def set_temperature_ramp(self, rate: T_PARSE_QUANTITY):
        if not self._has_heater:
            raise ParameterError('Heating not supported')

        rate = parse(rate, 'degC/min').magnitude

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
        self.logger().info(f"Controller name: {self._handle.get_controller_name()}")
        self.logger().info(f"Controller serial: {self._handle.get_controller_serial()}")
        self.logger().info(f"Controller hardware version: {self._handle.get_controller_hardware_version()}")
        self.logger().info(f"Controller firmware version: {self._handle.get_controller_firmware_version()}")

        self.logger().info(f"Humidity sensor name: {self._handle.get_humidity_controller_sensor_name()}")
        self.logger().info(f"Humidity sensor serial: {self._handle.get_humidity_controller_sensor_serial()}")
        self.logger().info(f"Humidity sensor hardware version: "
                           f"{self._handle.get_humidity_controller_sensor_hardware_version()}")

        self.logger().info(f"Stage name: {self._handle.get_stage_name()}")
        self.logger().info(f"Stage serial: {self._handle.get_stage_serial()}")
        self.logger().info(f"Stage hardware version: {self._handle.get_stage_hardware_version()}")
        self.logger().info(f"Stage firmware version: {self._handle.get_stage_firmware_version()}")

        controller = self._handle.get_controller_config()

        if controller.flags.supportsHeater:
            self.logger().info(f"Heating supported")
            self._has_heater = True

        if controller.flags.lnpReady:
            self.logger().info(f"Cooling supported")

        if controller.flags.humidityReady:
            self.logger().info(f"Humidity supported")
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
