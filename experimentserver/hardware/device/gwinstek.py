from __future__ import annotations

import enum
import typing

from experimentlib.data.unit import T_PARSE_QUANTITY, parse, registry, dimensionless
from experimentlib.util.time import now

from experimentserver.hardware.base.enum import HardwareEnum, TYPE_ENUM_CAST
from experimentserver.hardware.base.scpi import SCPIHardware
from experimentserver.hardware.base.visa import VISAHardware, TYPE_ERROR
from experimentserver.hardware.error import MeasurementError
from experimentserver.measurement import T_MEASUREMENT_SEQUENCE, Measurement, MeasurementGroup


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class LCRMeasurement(HardwareEnum):
    """ Enum covering different measurement_group modes for an LCR meter. """
    CAP_SERIES_RES_SERIES = enum.auto()
    CAP_SERIES_DISSIPATION = enum.auto()
    CAP_PARALLEL_RES_PARALLEL = enum.auto()
    CAP_PARALLEL_DISSIPATION = enum.auto()
    IND_PARALLEL_RES_PARALLEL = enum.auto()
    IND_PARALLEL_QUALITY = enum.auto()
    IND_SERIES_RES_SERIES = enum.auto()
    IND_SERIES_QUALITY = enum.auto()
    RES_SERIES_QUALITY = enum.auto()
    RES_PARALLEL_QUALITY = enum.auto()
    RES_REACTANCE = enum.auto()
    IMPEDANCE_PHASE_RAD = enum.auto()
    IMPEDANCE_PHASE_DEG = enum.auto()
    IMPEDANCE_DISSIPATION = enum.auto()
    IMPEDANCE_QUALITY = enum.auto()
    DCRES = enum.auto()

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.List[str]]]:
        return {
            cls.CAP_SERIES_RES_SERIES: ['csrs'],
            cls.CAP_SERIES_DISSIPATION: ['csd'],
            cls.CAP_PARALLEL_RES_PARALLEL: ['cprp'],
            cls.CAP_PARALLEL_DISSIPATION: ['cpd'],
            cls.IND_PARALLEL_RES_PARALLEL: ['iprp', 'lprp'],
            cls.IND_PARALLEL_QUALITY: ['ipq', 'lpq'],
            cls.IND_SERIES_RES_SERIES: ['isrs', 'lsrs'],
            cls.IND_SERIES_QUALITY: ['isq', 'lqd'],
            cls.RES_SERIES_QUALITY: ['rsq'],
            cls.RES_PARALLEL_QUALITY: ['rpq'],
            cls.RES_REACTANCE: ['rr'],
            cls.IMPEDANCE_PHASE_RAD: ['zpr'],
            cls.IMPEDANCE_PHASE_DEG: ['zpd'],
            cls.IMPEDANCE_DISSIPATION: ['zd'],
            cls.IMPEDANCE_QUALITY: ['zq'],
            cls.DCRES: ['r', 'ohm', 'res', 'resistance'],
        }

    @classmethod
    def _get_description_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.CAP_SERIES_RES_SERIES: 'Series Capacitance + Resistance (ESR) (Cs-Rs)',
            cls.CAP_SERIES_DISSIPATION: 'Series Capacitance + Dissipation (Cs-D)',
            cls.CAP_PARALLEL_RES_PARALLEL: 'Parallel Capacitance + Resistance (Cp-Rp)',
            cls.CAP_PARALLEL_DISSIPATION: 'Parallel Capacitance + Dissipation (Cp-D)',
            cls.IND_PARALLEL_RES_PARALLEL: 'Parallel Inductance + Resistance (Lp-Rp)',
            cls.IND_PARALLEL_QUALITY: 'Parallel Inductance + Q-Factor (Lp-Q)',
            cls.IND_SERIES_RES_SERIES: 'Series Inductance + Resistance (ESR) (Ls-Rs)',
            cls.IND_SERIES_QUALITY: 'Series Inductance + Q-Factor (Ls-Q)',
            cls.RES_SERIES_QUALITY: 'Series Resistance + Q-Factor (Rs-Q)',
            cls.RES_PARALLEL_QUALITY: 'Parallel Resistance + Q-Factor (Rp-Q)',
            cls.RES_REACTANCE: 'Resistance + Reactance (R-X)',
            cls.IMPEDANCE_PHASE_RAD: 'Impedance + Phase (radians) (Z-thr)',
            cls.IMPEDANCE_PHASE_DEG: 'Impedance + Phase (degrees) (Z-thd)',
            cls.IMPEDANCE_DISSIPATION: 'Impedance + Dissipation (Z-D)',
            cls.IMPEDANCE_QUALITY: 'Impedance + Q-Factor (Z-Q)',
            cls.DCRES: 'DC Resistance (DCR)'
        }

    @classmethod
    def _get_command_map(cls) -> typing.Dict[HardwareEnum, str]:
        return {
            cls.CAP_SERIES_RES_SERIES: 'Cs-Rs',
            cls.CAP_SERIES_DISSIPATION: 'Cs-D',
            cls.CAP_PARALLEL_RES_PARALLEL: 'Cp-Rp',
            cls.CAP_PARALLEL_DISSIPATION: 'Cp-D',
            cls.IND_PARALLEL_RES_PARALLEL: 'Lp-Rp',
            cls.IND_PARALLEL_QUALITY: 'Lp-Q',
            cls.IND_SERIES_RES_SERIES: 'Ls-Rs',
            cls.IND_SERIES_QUALITY: 'Ls-Q',
            cls.RES_SERIES_QUALITY: 'Rs-Q',
            cls.RES_PARALLEL_QUALITY: 'Rp-Q',
            cls.RES_REACTANCE: 'R-X',
            cls.IMPEDANCE_PHASE_RAD: 'Z-thr',
            cls.IMPEDANCE_PHASE_DEG: 'Z-thd',
            cls.IMPEDANCE_DISSIPATION: 'Z-D',
            cls.IMPEDANCE_QUALITY: 'Z-Q',
            cls.DCRES: 'DCR'
        }

    @classmethod
    def get_reading_map(cls, measure: typing.Optional[LCRMeasurement] = None):
        if measure is not None:
            return cls.get_reading_map()[measure]
        else:
            return {
                cls.CAP_SERIES_RES_SERIES: ((MeasurementGroup.CAPACITANCE, 'series', registry.farad),
                                            (MeasurementGroup.RESISTANCE, 'series', registry.ohm)),
                cls.CAP_SERIES_DISSIPATION: ((MeasurementGroup.CAPACITANCE, 'series', registry.farad),
                                             (MeasurementGroup.DISSIPATION, 'factor', dimensionless)),
                cls.CAP_PARALLEL_RES_PARALLEL: ((MeasurementGroup.CAPACITANCE, 'parallel', registry.farad),
                                                (MeasurementGroup.RESISTANCE, 'parallel', registry.ohm)),
                cls.CAP_PARALLEL_DISSIPATION: ((MeasurementGroup.CAPACITANCE, 'parallel', registry.farad),
                                               (MeasurementGroup.DISSIPATION, 'factor', dimensionless)),
                cls.IND_PARALLEL_RES_PARALLEL: ((MeasurementGroup.INDUCTANCE, 'parallel', registry.henry),
                                                (MeasurementGroup.RESISTANCE, 'parallel', registry.ohm)),
                cls.IND_PARALLEL_QUALITY: ((MeasurementGroup.INDUCTANCE, 'parallel', registry.henry),
                                           (MeasurementGroup.QUALITY, 'factor', dimensionless)),
                cls.IND_SERIES_RES_SERIES: ((MeasurementGroup.INDUCTANCE, 'series', registry.henry),
                                            (MeasurementGroup.RESISTANCE, 'series', registry.ohm)),
                cls.IND_SERIES_QUALITY: ((MeasurementGroup.INDUCTANCE, 'series', registry.henry),
                                         (MeasurementGroup.QUALITY, 'factor', dimensionless)),
                cls.RES_SERIES_QUALITY: ((MeasurementGroup.RESISTANCE, 'series', registry.ohm),
                                         (MeasurementGroup.QUALITY, 'factor', dimensionless)),
                cls.RES_PARALLEL_QUALITY: ((MeasurementGroup.RESISTANCE, 'parallel', registry.ohm),
                                           (MeasurementGroup.QUALITY, 'factor', dimensionless)),
                cls.RES_REACTANCE: ((MeasurementGroup.RESISTANCE, 'resistance', registry.ohm),
                                    (MeasurementGroup.RESISTANCE, 'reactance', registry.ohm)),
                cls.IMPEDANCE_PHASE_RAD: ((MeasurementGroup.IMPEDANCE, 'mag', registry.ohm),
                                          (MeasurementGroup.IMPEDANCE, 'phase_rad', registry.rad)),
                cls.IMPEDANCE_PHASE_DEG: ((MeasurementGroup.IMPEDANCE, 'mag', registry.ohm),
                                          (MeasurementGroup.IMPEDANCE, 'phase_deg', registry.deg)),
                cls.IMPEDANCE_DISSIPATION: ((MeasurementGroup.RESISTANCE, 'impedance', registry.ohm),
                                            (MeasurementGroup.DISSIPATION, 'factor', dimensionless)),
                cls.IMPEDANCE_QUALITY: ((MeasurementGroup.RESISTANCE, 'impedance', registry.ohm),
                                        (MeasurementGroup.QUALITY, 'factor', dimensionless)),
                cls.DCRES: ((MeasurementGroup.RESISTANCE, 'resistance', registry.ohm),)
            }


class GWInstekLCR6000Series(SCPIHardware):
    """  """

    def __init__(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        """
        # Configure baud rate for serial link
        super().__init__(*args, visa_open_args={
            'baud_rate': 115200
        }, visa_rate_limit=0.2, **kwargs)

    @SCPIHardware.register_parameter(description='Set measurement mode')
    def set_mode(self, mode: typing.Union[TYPE_ENUM_CAST, LCRMeasurement]):
        mode = LCRMeasurement.from_input(mode)

        with self.visa_transaction() as transaction:
            transaction.write(':FUNC {}', mode)

    @SCPIHardware.register_parameter(description='Source output DC bias')
    def set_source_bias(self, voltage: T_PARSE_QUANTITY):
        """

        :param voltage:
        :return:
        """
        if type(voltage) is str and voltage.upper() == 'MAX':
            voltage = 'MAX'
        elif type(voltage) is str and voltage.upper() == 'MIN':
            voltage = 'MIN'
        else:
            voltage = parse(voltage, registry.V).magnitude

        with self.visa_transaction() as transaction:
            transaction.write(":BISA {}", voltage)

    @SCPIHardware.register_parameter(description='Source output frequency')
    def set_source_frequency(self, frequency: T_PARSE_QUANTITY):
        if type(frequency) is str and frequency.upper() == 'MAX':
            frequency = 'MAX'
        elif type(frequency) is str and frequency.upper() == 'MIN':
            frequency = 'MIN'
        else:
            frequency = parse(frequency, 'Hz').magnitude

        with self.visa_transaction() as transaction:
            transaction.write(':FREQ {}', frequency)

    @SCPIHardware.register_parameter(description='Source output voltage')
    def set_source_voltage(self, voltage: T_PARSE_QUANTITY):
        """

        :param voltage:
        :return:
        """
        if type(voltage) is str and voltage.upper() == 'MAX':
            voltage = 'MAX'
        elif type(voltage) is str and voltage.upper() == 'MIN':
            voltage = 'MIN'
        else:
            voltage = parse(voltage, registry.V).magnitude

        if voltage == 0:
            voltage = 'OFF'

        with self.visa_transaction() as transaction:
            transaction.write(":LEV:VOLT {}", voltage)

    @SCPIHardware.register_measurement(description='Take measurement in configured mode', default=True)
    def get_reading(self) -> T_MEASUREMENT_SEQUENCE:
        with self.visa_transaction() as transaction:
            # Determine measurement mode
            measure_function_str = transaction.query(':FUNC?')

            # Measurement settings
            measure_bias = transaction.query(':BIAS?')
            measure_freq = transaction.query(':FREQ?')
            measure_voltage = transaction.query(':LEV:VOLT?')

            # Get reading
            measure_fields = transaction.query(':FETC:MAIN?')

        # Parse config
        if measure_bias.lower() == 'off':
            measure_bias = 0

        measure_bias = parse(measure_bias, registry.V)
        measure_freq = parse(measure_freq, registry.Hz)
        measure_voltage = parse(measure_voltage, registry.V)

        # Parse reading and determine measurement mode
        measure_function = LCRMeasurement.from_input(measure_function_str)
        measure_fields = measure_fields.split(',')

        # Get measurement mode metadata
        measure_meta = LCRMeasurement.get_reading_map(measure_function)

        # Validate number of fields
        if len(measure_fields) != len(measure_meta):
            raise MeasurementError(f"Expected {len(measure_meta)} field(s) but got {len(measure_fields)} in: "
                                   f"{measure_fields!r}")

        measurements = []
        measure_timestamp = now()

        for field, field_meta in zip(measure_fields, measure_meta):
            # Parse value
            field = parse(field, field_meta[2])

            measurements.append(Measurement(self, field_meta[0], {
                field_meta[1]: field
            }, measure_timestamp, tags={
                'source_bias': measure_bias,
                'source_frequency': measure_freq,
                'source_voltage': measure_voltage
            }))

        return measurements

    @classmethod
    def scpi_display(cls, transaction: VISAHardware.VISATransaction,
                     msg: typing.Optional[str] = None) -> typing.NoReturn:
        if msg is not None:
            if len(msg) > 30:
                cls.logger().warning("Truncating message to 30 characters (original: {})".format(msg))
                msg = msg[:30]
                msg = msg.replace(':', ' ')

            transaction.write(":DISP:LINE \"{}\"", msg)

    @classmethod
    def _get_visa_error(cls, transaction: VISAHardware.VISATransaction) -> typing.Optional[TYPE_ERROR]:
        msg = transaction.query(':ERR?')

        if '*E00' in msg:
            return None

        return msg

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'GW-INSTEK 6000 Series LRC Meter'
