import os.path
import typing

from transitions import EventData
from experimentlib.data.humidity import abs_to_rel, rel_to_abs, rel_to_dew, unit_abs
from experimentlib.data.unit import T_PARSE_QUANTITY, parse, registry, Quantity

from ..base.core import Hardware, ParameterError
from experimentserver.interface.linkam import LinkamSDK, StageValueType, SDK_PATH
from experimentserver.interface.linkam.license import fetch_license
from experimentserver.interface.linkam.sdk import LinkamConnectionError
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

        # Delay since Linkam only produces measurements data every ~0.1s
        self._measurement_delay = self._REFRESH_PERIOD

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

        # Humidity controller
        self._read_humidity = False
        self._humidity_setpoint: typing.Optional[Quantity] = None

        self._humidity_regulator_enable: bool = False
        self._humidity_flow_ratio: float = 1.0

    @Hardware.register_measurement(description='Temperature/humidity measurements', force=True)
    def measure(self) -> T_MEASUREMENT_SEQUENCE:
        assert self._handle is not None

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

        if self._read_humidity:
            if controller.flags.humidityReady:
                humidity = self._handle.get_humidity_details()

                rh = parse(humidity.rh, registry.pct, mag_round=2)
                rh_scaled = self._humidity_flow_ratio * rh
                rh_temp = parse(humidity.rhTemp, registry.degC, mag_round=2)
                rh_setpoint = parse(humidity.rhSetpoint, registry.pct)
                rh_setpoint_scaled = self._humidity_flow_ratio * rh_setpoint

                self.logger().trace(f"RH: {rh} ({rh.units}, id: {id(rh)}), "
                                    f"RH scaled: {rh_scaled} ({rh_scaled.units}, id: {id(rh_scaled)}), "
                                    f"RH_temp: {rh_temp}, "
                                    f"Setpoint: {rh_setpoint} ({rh_setpoint.units}, id: {id(rh_setpoint)}), "
                                    f"Setpoint scaled: {rh_setpoint_scaled} ({rh_setpoint_scaled.units}, "
                                    f"id: {id(rh_setpoint_scaled)})")

                humidity_fields = {
                    'stage_rh': rh_scaled,
                    'stage_rh_raw': rh,
                    'stage_rh_temperature': rh_temp,
                    'stage_rh_setpoint': rh_setpoint_scaled,
                    'stage_rh_setpoint_raw': rh_setpoint,
                    'generator_rh': parse(humidity.tubePercent, registry.pct, mag_round=2),
                    'generator_rh_setpoint': parse(humidity.rhSetpoint, registry.pct),
                    'water_temperature': parse(humidity.waterTemp, registry.degC, mag_round=2),
                    'water_temperature_setpoint': parse(humidity.waterSetpoint, registry.degC),
                    # 'generator_column': humidity.status.flags.colSel,
                    # 'generator_drying': humidity.status.flags.dessicantDryMode,
                    # 'generator_running': humidity.status.flags.started
                }

                # Apply updated target based upon sensor temperature
                if self._humidity_regulator_enable and self._humidity_setpoint > 0:
                    try:
                        setpoint_abs = rel_to_abs(self._humidity_room_temp, self._humidity_setpoint)
                        setpoint_rel = abs_to_rel(rh_temp, setpoint_abs).to(registry.pct)
                        self.logger().trace(f"Linkam Setpoint: {rh_setpoint}, Setpoint @ {self._humidity_room_temp}: "
                                            f"{self._humidity_setpoint.to(registry.pct)}, Abs: {setpoint_abs}, "
                                            f"Rel @ {rh_temp}: {setpoint_rel}")

                        self._set_humidity_setpoint(setpoint_rel)
                    except (ValueError, NotImplementedError):
                        self.logger().warning(f"Error during regulator humidity calculation, RH: {rh}, Temp: {rh_temp},"
                                              f" Setpoint: {self._humidity_setpoint}, Linkam Setpoint: {rh_setpoint}")

                humidity_tags = None

                # Calculate absolute humidity
                try:
                    if rh_scaled > 0:
                        humidity_fields['stage_abs'] = rel_to_abs(rh_temp, rh_scaled)
                        humidity_fields['stage_dew'] = rel_to_dew(rh_temp, rh_scaled)
                    else:
                        self.logger().debug("Zero relative humidity")
                        humidity_fields['stage_abs'] = parse(0, unit_abs)

                    # RH at room temperature
                    if self._humidity_room_temp is not None:
                        room_rh = abs_to_rel(self._humidity_room_temp, humidity_fields['stage_abs'])
                        humidity_fields['stage_room_rh'] = room_rh
                        humidity_fields['stage_room_temperature'] = self._humidity_room_temp

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

    def _set_humidity_setpoint(self, setpoint: Quantity):
        setpoint_scaled = (setpoint / self._humidity_flow_ratio).to(registry.pct)
        self.logger().trace(f"Setpoint: {setpoint}, Scaled: {setpoint_scaled}, Ratio: {self._humidity_flow_ratio}")

        if setpoint_scaled < 0 or setpoint_scaled.m_as(registry.dimensionless) > 1:
            raise ParameterError(f"Cannopt set scaled humidity setpoint to {setpoint_scaled} (original: "
                                 f"{setpoint}, ratio: {self._humidity_flow_ratio})")

        if not self._handle.set_value(StageValueType.HUMIDITY_SETPOINT, setpoint_scaled.m_as(registry.pct)):
            raise ParameterError('Cannot configure humidity set point')

    @Hardware.register_parameter(description='Stage humidity (0 = off)')
    def set_humidity(self, humidity):
        # Enable humidity reading if set at least once
        self._read_humidity = True

        if not self._has_humidity:
            raise ParameterError('Humidity not supported')

        # Update setpoint
        self._humidity_setpoint = parse(humidity, registry.pct).to(registry.pct)

        self._set_humidity_setpoint(self._humidity_setpoint)

    @Hardware.register_parameter(description='Enable/disable external humidity regulation (EXPERIMENTAL)')
    def set_humidity_regulator(self, enable: typing.Union[bool, str]):
        if type(enable) is str:
            enable = enable.strip().lower() in ['true', '1', 'on']

        self._humidity_regulator_enable = enable

    @Hardware.register_parameter(description='Humidity flow ratio')
    def set_humidity_flow_ratio(self, ratio: float):
        self._humidity_flow_ratio = float(ratio)

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

    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super(T96Controller, self).transition_connect(event)

        # Connect over USB
        for _ in range(5):
            try:
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
                # self.logger().info(f"Stage firmware version: {self._handle.get_stage_firmware_version()}")

                controller = self._handle.get_controller_config()

                if controller.flags.supportsHeater:
                    self.logger().info(f"Heating supported")
                    self._has_heater = True

                if controller.flags.lnpReady:
                    self.logger().info(f"Cooling supported")

                if controller.flags.humidityReady:
                    self.logger().info(f"Humidity supported")
                    self._has_humidity = True
                
                break
            except LinkamConnectionError as exc:
                self.logger().warning(f"Error starting Linkam (error: {exc!s})")

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        self._handle.close()
        self._handle = None

        super(T96Controller, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
        # Force default humidity and enable
        if self._has_humidity:
            self.set_humidity(0)
            self._handle.enable_humidity(True)

        super(T96Controller, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        # Stop heating and humidity regulation
        self._handle.enable_heater(False)
        self._handle.enable_humidity(False)

        super(T96Controller, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        super(T96Controller, self).transition_error(event)
