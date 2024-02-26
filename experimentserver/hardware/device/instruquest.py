import typing
from datetime import datetime
from enum import IntFlag, auto

from experimentlib.data.gas import registry as gas_registry
from experimentlib.data.unit import registry, parse

from ..base.serial import SerialStringHardware
from ..metadata import TYPE_PARAMETER_DICT
from experimentserver.measurement import MeasurementGroup, Measurement

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class HumiSysGenerator(SerialStringHardware):
    class PortA(IntFlag):
        SOLENOID_WET = auto()
        SOLENOID_DRY = auto()
        UNUSED0 = auto()
        UNUSED1 = auto()
        EXTERNAL_OUT = auto()
        PUMP_CONTROL = auto()
        EXTRA = auto()
        RESERVED = auto()

    class PortB(IntFlag):
        EXTERNAL_START_STOP = auto()
        RELAY_LEVEL_SENSOR = auto()
        WATER_LEVEL_SENSOR = auto()
        MODE_MANUAL = auto()

    _ADC_FULL_SCALE = 5.0
    _DAC_FULL_SCALE = 4095

    _MFC_DRY_INTERCEPT = -0.2
    _MFC_DRY_SLOPE = 21.1828
    _MFC_WET_INTERCEPT = -0.2
    _MFC_WET_SLOPE = 21.1912

    _RH1_RH_INTERCEPT = -0.2
    _RH1_RH_SLOPE = 21.1828
    _RH1_TEMP_INTERCEPT = -0.2
    _RH1_TEMP_SLOPE = 21.1912

    _RH2_RH_INTERCEPT = -0.2
    _RH2_RH_SLOPE = 21.1131
    _RH2_TEMP_INTERCEPT = -0.2
    _RH2_TEMP_SLOPE = 21.1558

    _RTD1_INTERCEPT = -18.9484
    _RTD1_SLOPE = 25.5698

    _RTD2_INTERCEPT = -19.6134
    _RTD2_SLOPE = 25.5933

    _FLOW_FULL_SCALE = 200.0
    _OUTPUT_FULL_SCALE = 5.0

    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(HumiSysGenerator, self).__init__(identifier, port, parameters, {
            'baudrate': 19200
        }, ','.encode())

        self._gas_dry = gas_registry.air
        self._gas_wet = gas_registry.humid_air

        self._enable_analog = True
        self._enable_digital = True

        self._port_a = HumiSysGenerator.PortA(0)
        self._port_b = HumiSysGenerator.PortB(0)

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'InstruQuest HumiSys Humidity Generation System'

    def command_request_all(self):
        with self._serial_lock.lock('request_all'):
            self._serial_port.write(b'rs,')
    
    @SerialStringHardware.register_measurement(description='Get reading', force=True)
    def get_measurement(self) -> typing.Sequence[Measurement]:
        # Tigger reporting of all metrics
        self.command_request_all()

        self.sleep(2.5, 'rate_limit')

        return []

    def _command_dac_set(self, channel: int, value: float):
        assert 0 < channel <= 4

    def _set_port_a(self, flag: PortA, state: bool):
        if state:
            self._port_a |= flag
        else:
            self._port_a &= ~flag

        with self._serial_lock.lock('set_port_a'):
            assert self._serial_port is not None
            self._serial_port.write(f"pA{self._port_a.value},".encode())

    def _handle_payload(self, payload: str, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        if payload == 'READY':
            # Ignore ready message
            return None
        else:
            try:
                if payload.startswith('da3'):
                    # FIXME
                    value = int(payload[3:]) / self._DAC_FULL_SCALE * self._OUTPUT_FULL_SCALE
                    self.logger().comm(f"Saturator TC1 (voltage): {value}")

                    return [Measurement(
                        self,
                        MeasurementGroup.HUMIDITY,
                        {
                            'saturator_temperature_setpoint_voltage': parse(value, registry.volt, mag_round=3),
                            'saturator_temperature_setpoint_voltage_raw': round(value, 3)
                        },
                        received,
                        tags={
                            'humisys_channel': 'da3'
                        }
                    )]
                elif payload.startswith('da4'):
                    value = int(payload[3:])
                    self.logger().comm(f"Unused da4: {value}")

                    return None
                elif payload.startswith('pA'):
                    value = self.PortA(int(payload[2:]))
                    self.logger().comm(f"PortA: {value!r}")

                    self._port_a = value

                    return [
                        Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'solenoid_wet': bool(self._port_a.SOLENOID_WET),
                                'solenoid_dry': bool(self._port_a.SOLENOID_DRY)
                            },
                            received,
                            tags={
                                'humisys_channel': 'pA'
                            }
                        ),
                    ]
                elif payload.startswith('pB'):
                    value = self.PortB(int(payload[2:]))
                    self.logger().comm(f"PortB: {value!r}")

                    self._port_b = value

                    return [
                        Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'manual': bool(self._port_b.MODE_MANUAL),
                                'water_level_ok': bool(self._port_b.WATER_LEVEL_SENSOR)
                            },
                            received,
                            tags={
                                'humisys_channel': 'pB'
                            }
                        ),
                    ]
                else:
                    value = float(payload[3:])
                    
                    if payload.startswith('ad0'):
                        self.logger().comm(f"WET MFC: {value}")

                        return [Measurement(
                            self,
                            MeasurementGroup.MFC,
                            {
                                'flow_actual': parse(value / self._ADC_FULL_SCALE * self._FLOW_FULL_SCALE, registry.sccm, mag_round=0),
                                'flow_actual_raw': round(value, 3)
                            },
                            received,
                            tags={
                                'gas_mixture': self._gas_wet,
                                'humisys_channel': 'ad0'
                            }
                        )]
                    elif payload.startswith('ad1'):
                        self.logger().comm(f"DRY MFC: {value}")

                        return [Measurement(
                            self,
                            MeasurementGroup.MFC,
                            {
                                'flow_actual': parse(
                                    value / self._ADC_FULL_SCALE * self._FLOW_FULL_SCALE,
                                    registry.sccm,
                                    mag_round=0
                                ),
                                'flow_actual_raw': round(value, 3)
                            },
                            received,
                            tags={
                                'gas_mixture': self._gas_dry,
                                'humisys_channel': 'ad1'
                            }
                        )]
                    elif payload.startswith('ad2'):
                        value = value * self._RTD1_SLOPE + self._RTD1_INTERCEPT
                        self.logger().comm(f"RTD1 (Internal, Saturator): {value}")

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'saturator_temperature': parse(value, registry.degC)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RTD1',
                                'humisys_channel': 'ad2'
                            }
                        )]
                    elif payload.startswith('ad3'):
                        value = value * self._RTD2_SLOPE + self._RTD2_INTERCEPT
                        self.logger().comm(f"RTD2 (External, Target): {value}")

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'outlet_temperature': parse(value, registry.degC)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RTD2',
                                'humisys_channel': 'ad3'
                            }
                        )]
                    elif payload.startswith('ad4'):
                        value = value * self._RH1_RH_SLOPE + self._RH1_RH_INTERCEPT
                        self.logger().comm(f"HygroClip 1 RH (analog): {value}")

                        if not self._enable_analog:
                            return None

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh': parse(value, registry.pct)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH1',
                                'humisys_channel': 'ad4',
                                'humisys_method': 'analog'
                            }
                        )]
                    elif payload.startswith('ad5'):
                        value = value * self._RH1_TEMP_SLOPE + self._RH1_TEMP_INTERCEPT
                        self.logger().comm(f"HygroClip 1 T (analog): {value}")

                        if not self._enable_analog:
                            return None

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh_temperature': parse(value, registry.degC)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH1',
                                'humisys_channel': 'ad5',
                                'humisys_method': 'analog'
                            }
                        )]
                    elif payload.startswith('ad6'):
                        value = value * self._RH2_RH_SLOPE + self._RH2_RH_INTERCEPT
                        self.logger().comm(f"HygroClip 2 RH (analog): {value}")

                        if not self._enable_analog:
                            return None

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh': parse(value, registry.pct)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH2',
                                'humisys_channel': 'ad6',
                                'humisys_method': 'analog'
                            }
                        )]
                    elif payload.startswith('ad7'):
                        value = value * self._RH2_TEMP_SLOPE + self._RH2_TEMP_INTERCEPT
                        self.logger().comm(f"HygroClip 2 T (analog): {value}")

                        if not self._enable_analog:
                            return None

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh_temperature': parse(value, registry.degC)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH2',
                                'humisys_channel': 'ad7',
                                'humisys_method': 'analog'
                            }
                        )]
                    elif payload.startswith('ad8'):
                        value = value
                        self.logger().comm(f"HygroClip 1 RH (digital): {value}")

                        if not self._enable_digital:
                            return None
                        
                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh': parse(value, registry.pct)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH1',
                                'humisys_channel': 'ad8',
                                'humisys_method': 'digital'
                            }
                        )]
                    elif payload.startswith('sp8'):
                        value = value
                        self.logger().comm(f"HygroClip 2 RH (digital): {value}")

                        if not self._enable_digital:
                            return None

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh': parse(value, registry.pct)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH2',
                                'humisys_channel': 'sp8',
                                'humisys_method': 'digital'
                            }
                        )]
                    elif payload.startswith('ad9'):
                        value = value
                        self.logger().comm(f"HygroClip 1 T (digital): {value}")

                        if not self._enable_digital:
                            return None

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh_temperature': parse(value, registry.degC)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH1',
                                'humisys_channel': 'ad9',
                                'humisys_method': 'digital'
                            }
                        )]
                    elif payload.startswith('sp9'):
                        value = value
                        self.logger().comm(f"HygroClip 2 T (digital): {value}")

                        if not self._enable_digital:
                            return None

                        return [Measurement(
                            self,
                            MeasurementGroup.HUMIDITY,
                            {
                                'generator_rh_temperature': parse(value, registry.degC)
                            },
                            received,
                            tags={
                                'humisys_sensor': 'RH2',
                                'humisys_channel': 'sp9',
                                'humisys_method': 'digital'
                            }
                        )]
                    elif payload.startswith('da1'):
                        value_int = int(payload[3:])
                        self.logger().comm(f"WET MFC Setpoint: {value}")

                        return [Measurement(
                            self,
                            MeasurementGroup.MFC,
                            {
                                'flow_target': parse(
                                    float(value_int) / self._DAC_FULL_SCALE * self._FLOW_FULL_SCALE,
                                    registry.sccm,
                                    mag_round=0
                                ),
                                'flow_target_raw': value_int
                            },
                            received,
                            tags={
                                'gas_mixture': self._gas_wet,
                                'humisys_channel': 'da1'
                            }
                        )]
                    elif payload.startswith('da2'):
                        value_int = int(payload[3:])
                        self.logger().comm(f"DRY MFC Setpoint: {value}")

                        return [Measurement(
                            self,
                            MeasurementGroup.MFC,
                            {
                                'flow_target': parse(
                                    float(value_int) / self._DAC_FULL_SCALE * self._FLOW_FULL_SCALE,
                                    registry.sccm,
                                    mag_round=0
                                ),
                                'flow_target_raw': value_int
                            },
                            received,
                            tags={
                                'gas_mixture': self._gas_dry,
                                'humisys_channel': 'da2'
                            }
                        )]
            except ValueError:
                self.logger().error(f"Unable to cast value in payload {payload!r}")
                return None
        
        self.logger().error(f"Received unhandled message {payload!r}")
        return None
