import os.path
import typing

from experimentlib.data.humidity import rel_to_abs, rel_to_dew
from experimentlib.data.unit import T_PARSE_QUANTITY, parse, registry
from transitions import EventData
from pylinkam import interface, sdk

from ..base.core import Hardware, ParameterError
from ..error import CommunicationError
from experimentserver.interface.linkam.license import fetch_license
from experimentserver.measurement import T_MEASUREMENT_SEQUENCE, Measurement, MeasurementGroup


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


_DEFAULT_SDK_ROOT = os.path.dirname(os.path.abspath(__file__))


class T96Controller(Hardware):
    # Linkam only produces measurements data every ~0.25s
    _REFRESH_PERIOD = 0.25

    _CONNECT_ATTEMPTS = 3

    def __init__(self, *args: typing.Any, sdk_root_path: typing.Optional[str] = None, sdk_log_path: typing.Optional[str] = None, sdk_license_path: typing.Optional[str] = None,
                 **kwargs: typing.Any):
        """ Create an instance of the class.

        :param sdk_root_path: optional SDK root path for binary loading, defaults to None
        :param sdk_log_path: optional SDK log path, defaults to None
        :param sdk_license_path: optional SDK license path, defaults to None
        """
        super(T96Controller, self).__init__(*args, **kwargs)

        self._measurement_delay = self._REFRESH_PERIOD

        self._sdk_root_path = sdk_root_path or _DEFAULT_SDK_ROOT
        self._sdk_log_path = sdk_log_path
        self._sdk_license_path = sdk_license_path

        # Controller state
        self._has_heater = False
        self._has_humidity = False

        self._enable_humidity = False

        self._heater_ch1_enable = True
        self._heater_ch2_enable = True
        self._heater_ch3_enable = True
        self._heater_ch4_enable = True

        if self._sdk_license_path is None:
            license_path = os.path.join(self._sdk_root_path, 'Linkam.lsk')

            with open(license_path, 'wb') as license_file:
                fetch_license(license_file)
        
        # Create handle to Linkam SDK
        self._sdk = sdk.SDKWrapper(
            self._sdk_root_path,
            sdk_log_path=self._sdk_log_path,
            sdk_license_path=self._sdk_license_path
        )

        self._connection: typing.Optional[sdk.SDKWrapper.Connection] = None
    
    @Hardware.register_measurement(description='Monitor state', force=True)
    def measure(self) -> T_MEASUREMENT_SEQUENCE:
        assert self._connection is not None

        measurements = []

        controller_config = self._connection.get_controller_config()

        if controller_config.flags.supportsHeater:
            program_state = self._connection.get_program_state()

            temperature_fields = {
                'heater_temperature_ramp': self._connection.get_value(interface.StageValueType.HEATER_RATE),
                'heater_temperature_setpoint': self._connection.get_value(interface.StageValueType.HEATER_SETPOINT),
                'heater_voltage': parse(program_state.voltage, registry.V, mag_round=3),
                'heater_current': parse(program_state.current, registry.A, mag_round=3),
                'heater_power': parse(program_state.pwm, registry.W, mag_round=3)
            }

            if self._heater_ch1_enable:
                temperature_fields['heater_temperature_ch1'] = self._connection.get_value(
                    interface.StageValueType.HEATER1_TEMP
                )
                temperature_fields['heater_output_ch1'] = self._connection.get_value(
                    interface.StageValueType.HEATER1_POWER
                )
            
            if self._heater_ch2_enable:
                temperature_fields['heater_temperature_ch2'] = self._connection.get_value(
                    interface.StageValueType.HEATER2_TEMP
                )
                temperature_fields['heater_output_ch2'] = self._connection.get_value(
                    interface.StageValueType.HEATER2_POWER
                )
            
            if self._heater_ch3_enable:
                temperature_fields['heater_temperature_ch3'] = self._connection.get_value(
                    interface.StageValueType.HEATER3_TEMP
                )
            
            if self._heater_ch4_enable:
                temperature_fields['heater_temperature_ch4'] = self._connection.get_value(
                    interface.StageValueType.HEATER4_TEMP
                )

            measurements.append(
                Measurement(
                    self,
                    MeasurementGroup.TEMPERATURE,
                    temperature_fields
                )
            )

            if not self._has_heater:
                self.logger().warning('Heater connected')
                self._has_heater = True
        elif self._has_heater:
            self.logger().error('Heater disconnected')
            self._has_heater = False

        if self._enable_humidity:
            if controller_config.flags.humidityReady:
                humidity_detail = self._connection.get_humidity_details()

                rh = parse(humidity_detail.rh, registry.pct, mag_round=2)
                rh_temp = parse(humidity_detail.rhTemp, registry.degC, mag_round=2)

                humidity_fields = {
                    'stage_rh': rh,
                    'stage_rh_temperature': rh_temp,
                    'stage_rh_setpoint': parse(humidity_detail.rhSetpoint, registry.pct),
                    'generator_rh': parse(humidity_detail.tubePercent, registry.pct, mag_round=2),
                    'generator_rh_setpoint': parse(humidity_detail.rhSetpoint, registry.pct),
                    'water_temperature': parse(humidity_detail.waterTemp, registry.degC, mag_round=2),
                    'water_temperature_setpoint': parse(humidity_detail.waterSetpoint, registry.degC)
                }

                try:
                    humidity_fields['stage_abs'] = rel_to_abs(rh_temp, rh)
                except ValueError:
                    self.logger().warning(f"Error during absolute humidity calculation: {rh!s} @ {rh_temp!s}")

                try:
                    humidity_fields['stage_dew'] = rel_to_dew(rh_temp, rh)
                except ValueError:
                    self.logger().warning(f"Error during dew point calculation: {rh!s} @ {rh_temp!s}")

                measurements.append(
                    Measurement(
                        self,
                        MeasurementGroup.HUMIDITY,
                        humidity_fields
                    )
                )

                if not self._has_humidity:
                    self.logger().warning('Humidity measurements available')
                    self._has_humidity = True
            elif self._has_humidity:
                self.logger().error('Humidity measurement no longer available')
                self._has_humidity = False
            
        return measurements

    @Hardware.register_parameter(description='Stage humidity (0 = off)')
    def set_humidity(self, humidity: T_PARSE_QUANTITY) -> None:
        if not self._connection:
            raise CommunicationError('Not connected to controller')

        if not self._has_humidity:
            raise ParameterError('Humidity not supported')

        # Enable humidity reading if set at least once
        self._read_humidity = True

        if not self._connection.set_value(
            interface.StageValueType.HUMIDITY_SETPOINT,
            parse(humidity, registry.pct)
        ):
            raise ParameterError('Cannot configure humidity set point')

    @Hardware.register_parameter(description='Stage temperature (0 = off)')
    def set_temperature(self, temperature: T_PARSE_QUANTITY) -> None:
        if not self._connection:
            raise CommunicationError('Not connected to controller')

        if not self._has_heater:
            raise ParameterError('Heating not supported')
        
        temperature = parse(temperature, registry.degC).magnitude

        if temperature > 0:
            if not self._connection.set_value(interface.StageValueType.HEATER_SETPOINT, temperature):
                raise ParameterError('Cannot configure temperature set point')

            if not self._connection.enable_heater(True):
                raise ParameterError('Cannot start heater')
        else:
            self._connection.enable_heater(False)

    @Hardware.register_parameter(description='Stage temperature ramp rate')
    def set_temperature_ramp(self, rate: T_PARSE_QUANTITY) -> None:
        if not self._connection:
            raise CommunicationError('Not connected to controller')

        if not self._has_heater:
            raise ParameterError('Heating not supported')

        rate = parse(rate, 'degC/min').magnitude

        if not self._connection.set_value(interface.StageValueType.HEATER_RATE, rate):
            raise ParameterError('Cannot configure temperature ramp rate')

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Linkam T96 Stage Controller'
    
    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super(T96Controller, self).transition_connect(event)

        # Ensure SDK is initialised and connect
        for attempt in range(self._CONNECT_ATTEMPTS):
            try:
                self._connection = self._sdk.connect_usb()

                controller = self._connection.get_controller_config()

                if controller.flags.supportsHeater:
                    self.logger().info(f"Heating supported")
                    self._has_heater = True

                if controller.flags.lnpReady:
                    self.logger().info(f"Cooling supported")

                if controller.flags.humidityReady:
                    self.logger().info(f"Humidity supported")
                    self._has_humidity = True
            
                # Get metadata
                self.logger().info(f"Controller name: {self._connection.get_controller_name()}")
                self.logger().info(f"Controller serial: {self._connection.get_controller_serial()}")
                self.logger().info(f"Controller hardware version: {self._connection.get_controller_hardware_version()}")
                self.logger().info(f"Controller firmware version: {self._connection.get_controller_firmware_version()}")

                if self._has_humidity:
                    self.logger().info(
                        f"Humidity sensor name: {self._connection.get_humidity_controller_sensor_name()}"
                    )
                    self.logger().info(
                        f"Humidity sensor serial: {self._connection.get_humidity_controller_sensor_serial()}"
                    )
                    self.logger().info(
                        f"Humidity sensor hardware version: "
                        f"{self._connection.get_humidity_controller_sensor_hardware_version()}"
                    )

                self.logger().info(f"Stage name: {self._connection.get_stage_name()}")
                self.logger().info(f"Stage serial: {self._connection.get_stage_serial()}")
                self.logger().info(f"Stage hardware version: {self._connection.get_stage_hardware_version()}")

                return
            except sdk.ControllerConnectError as exc:
                if attempt >= self._CONNECT_ATTEMPTS - 1:
                    raise CommunicationError('Unable to connect to Linkam controller. Ensure controller is turned on '
                                             'and power-cycle if necessary.') from exc

                self.sleep(0.25, 'reconnect retry')
                self.logger().warning(f"Error starting Linkam (error: {exc!s})")

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        
        super(T96Controller, self).transition_disconnect(event)
    
    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
        assert self._connection is not None

        # Force default humidity and enable
        if self._has_humidity:
            self.set_humidity(0)
            self._connection.enable_humidity(True)

        super(T96Controller, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        if self._connection is not None:
            # Stop heating and humidity regulation
            if self._has_heater:
                self._connection.enable_heater(False)

            if self._has_humidity:
                self._connection.enable_humidity(False)

        super(T96Controller, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        super(T96Controller, self).transition_error(event)
