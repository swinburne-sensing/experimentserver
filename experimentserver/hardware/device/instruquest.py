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
        MODE = auto()

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

    def __init__(self, identifier: str, port: str, parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(HumiSysGenerator, self).__init__(identifier, port, parameters, {
            'baudrate': 19200
        }, ','.encode())

        self._port_a = HumiSysGenerator.PortA(0)
        self._port_b = HumiSysGenerator.PortB(0)

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'InstruQuest HumiSys Humidity Generation System'

    def command_request_all(self):
        with self._serial_lock.lock('request_all'):
            self._serial_port.write(b'rs,')

    def _command_dac_set(self, channel: int, value: float):
        assert 0 < channel <= 4

    def _set_port_a(self, flag: PortA, state: bool):
        if state:
            self._port_a |= flag
        else:
            self._port_a &= ~flag

        with self._serial_lock.lock():
            self._serial_port.write(f"pA{self._port_a.value},".encode())

    def _handle_payload(self, payload: str, received: datetime) -> typing.Optional[typing.List[Measurement]]:
        if not payload.endswith(','):
            self.logger().warning(f"Message missing terminator {payload!r}")
            return

        # Strip terminator
        payload = payload[:-1]

        if payload == 'READY,':
            # Ignore ready message
            pass
        elif payload.startswith('ad0'):
            value = float(payload[3:]) / self._ADC_FULL_SCALE * self._FLOW_FULL_SCALE
            self.logger().comm(f"WET MFC: {value}")

            return [Measurement(
                self,
                MeasurementGroup.MFC,
                {
                    'flow_actual': parse(value, registry.sccm),
                    'flow_actual_raw': float(payload[3:])
                },
                received,
                tags={
                    'gas_mixture': gas_registry.humid_air,
                    'humisys_channel': 'ad0'
                }
            )]
        elif payload.startswith('ad1'):
            value = float(payload[3:]) / self._ADC_FULL_SCALE * self._FLOW_FULL_SCALE
            self.logger().comm(f"DRY MFC: {value}")

            return [Measurement(
                self,
                MeasurementGroup.MFC,
                {
                    'flow_actual': parse(value, registry.sccm),
                    'flow_actual_raw': float(payload[3:])
                },
                received,
                tags={
                    'gas_mixture': gas_registry.air,
                    'humisys_channel': 'ad1'
                }
            )]
        elif payload.startswith('ad2'):
            value = float(payload[3:]) * self._RTD1_SLOPE + self._RTD1_INTERCEPT
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
            value = float(payload[3:]) * self._RTD2_SLOPE + self._RTD2_INTERCEPT
            self.logger().comm(f"RTD2 (External, Target): {value}")

            return [Measurement(
                self,
                MeasurementGroup.HUMIDITY,
                {
                    'external_temperature': parse(value, registry.degC)
                },
                received,
                tags={
                    'humisys_sensor': 'RTD2',
                    'humisys_channel': 'ad3'
                }
            )]
        elif payload.startswith('ad4'):
            value = float(payload[3:]) * self._RH1_RH_SLOPE + self._RH1_RH_INTERCEPT
            self.logger().comm(f"HygroClip 1 RH: {value}")

            return [Measurement(
                self,
                MeasurementGroup.HUMIDITY,
                {
                    'generator_rh': parse(value, registry.pct)
                },
                received,
                tags={
                    'humisys_sensor': 'RH1',
                    'humisys_channel': 'ad4'
                }
            )]
        elif payload.startswith('ad5'):
            value = float(payload[3:]) * self._RH1_TEMP_SLOPE + self._RH1_TEMP_INTERCEPT
            self.logger().comm(f"HygroClip 1 T: {value}")

            return [Measurement(
                self,
                MeasurementGroup.HUMIDITY,
                {
                    'generator_rh_temperature': parse(value, registry.degC)
                },
                received,
                tags={
                    'humisys_sensor': 'RH1',
                    'humisys_channel': 'ad5'
                }
            )]
        elif payload.startswith('ad6'):
            value = float(payload[3:]) * self._RH2_RH_SLOPE + self._RH2_RH_INTERCEPT
            self.logger().comm(f"HygroClip 2 RH: {value}")

            return [Measurement(
                self,
                MeasurementGroup.HUMIDITY,
                {
                    'generator_rh': parse(value, registry.pct)
                },
                received,
                tags={
                    'humisys_sensor': 'RH2',
                    'humisys_channel': 'ad6'
                }
            )]
        elif payload.startswith('ad7'):
            value = float(payload[3:]) * self._RH2_TEMP_SLOPE + self._RH2_TEMP_INTERCEPT
            self.logger().comm(f"HygroClip 2 T: {value}")

            return [Measurement(
                self,
                MeasurementGroup.HUMIDITY,
                {
                    'generator_rh_temperature': parse(value, registry.degC)
                },
                received,
                tags={
                    'humisys_sensor': 'RH2',
                    'humisys_channel': 'ad7'
                }
            )]
        elif payload.startswith('ad8'):
            value = float(payload[3:])
        elif payload.startswith('sp8'):
            value = float(payload[3:])
        elif payload.startswith('ad9'):
            value = float(payload[3:])
        elif payload.startswith('sp9'):
            value = float(payload[3:])
        elif payload.startswith('da1'):
            value = int(payload[3:]) / self._DAC_FULL_SCALE * self._FLOW_FULL_SCALE
            self.logger().comm(f"WET MFC Setpoint: {value}")

            return
        elif payload.startswith('da2'):
            value = int(payload[3:]) / self._DAC_FULL_SCALE * self._FLOW_FULL_SCALE
            self.logger().comm(f"DRY MFC Setpoint: {value}")

            return
        elif payload.startswith('da3'):
            # FIXME
            value = int(payload[3:]) / self._DAC_FULL_SCALE
            self.logger().comm(f"Saturator TC1: {value}")

            return
        elif payload.startswith('da4'):
            value = int(payload[3:])
            self.logger().comm(f"Unused da4: {value}")

            return
        elif payload.startswith('pA'):
            value = self.PortA(int(payload[2:]))
            self.logger().comm(f"PortA: {value}")

            self._port_a = value

            return
        elif payload.startswith('pB'):
            value = self.PortB(int(payload[2:]))
            self.logger().comm(f"PortB: {value}")

            self._port_b = value

            return

        self.logger().warning(f"Received unhandled message {payload!r}")
