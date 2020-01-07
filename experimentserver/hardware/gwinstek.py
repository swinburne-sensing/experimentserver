# import enum
# import typing
#
# from transitions import EventData
#
# from .scpi import SCPIHardware
# from .visa import VISAEnum
# from experimentserver.data import units
#
#
# class LCRMeasurement(VISAEnum):
#     """ Enum covering different measurement_group modes for an LCR meter. """
#     CAP_SERIES_RES_SERIES = enum.auto()
#     CAP_SERIES_DISSIPATION = enum.auto()
#     CAP_PARALLEL_RES_PARALLEL = enum.auto()
#     CAP_PARALLEL_DISSIPATION = enum.auto()
#     IND_PARALLEL_RES_PARALLEL = enum.auto()
#     IND_PARALLEL_QUALITY = enum.auto()
#     IND_SERIES_RES_SERIES = enum.auto()
#     IND_SERIES_QUALITY = enum.auto()
#     RES_SERIES_QUALITY = enum.auto()
#     RES_PARALLEL_QUALITY = enum.auto()
#     RES_REACTANCE = enum.auto()
#     IMPEDANCE_PHASE_RAD = enum.auto()
#     IMPEDANCE_PHASE_DEG = enum.auto()
#     IMPEDANCE_DISSIPATION = enum.auto()
#     IMPEDANCE_QUALITY = enum.auto()
#     DCRES = enum.auto()
#
#     @classmethod
#     def _get_description_map(cls) -> typing.Dict[VISAEnum, str]:
#         return {
#             cls.CAP_SERIES_RES_SERIES: 'Series Capacitance + Resistance (ESR) (Cs-Rs)',
#             cls.CAP_SERIES_DISSIPATION: 'Series Capacitance + Dissipation (Cs-D)',
#             cls.CAP_PARALLEL_RES_PARALLEL: 'Parallel Capacitance + Resistance (Cp-Rp)',
#             cls.CAP_PARALLEL_DISSIPATION: 'Parallel Capacitance + Dissipation (Cp-D)',
#             cls.IND_PARALLEL_RES_PARALLEL: 'Parallel Inductance + Resistance (Lp-Rp)',
#             cls.IND_PARALLEL_QUALITY: 'Parallel Inductance + Q-Factor (Lp-Q)',
#             cls.IND_SERIES_RES_SERIES: 'Series Inductance + Resistance (ESR) (Ls-Rs)',
#             cls.IND_SERIES_QUALITY: 'Series Inductance + Q-Factor (Ls-Q)',
#             cls.RES_SERIES_QUALITY: 'Series Resistance + Q-Factor (Rs-Q)',
#             cls.RES_PARALLEL_QUALITY: 'Parallel Resistance + Q-Factor (Rp-Q)',
#             cls.RES_REACTANCE: 'Resistance + Reactance (R-X)',
#             cls.IMPEDANCE_PHASE_RAD: 'Impedance + Phase (radians) (Z-thr)',
#             cls.IMPEDANCE_PHASE_DEG: 'Impedance + Phase (degrees) (Z-thd)',
#             cls.IMPEDANCE_DISSIPATION: 'Impedance + Dissipation (Z-D)',
#             cls.IMPEDANCE_QUALITY: 'Impedance + Q-Factor (Z-Q)',
#             cls.DCRES: 'DC Resistance (DCR)'
#         }
#
#     @classmethod
#     def _get_visa_map(cls) -> typing.Dict[VISAEnum, str]:
#         return {
#             cls.CAP_SERIES_RES_SERIES: 'Cs-Rs',
#             cls.CAP_SERIES_DISSIPATION: 'Cs-D',
#             cls.CAP_PARALLEL_RES_PARALLEL: 'Cp-Rp',
#             cls.CAP_PARALLEL_DISSIPATION: 'Cp-D',
#             cls.IND_PARALLEL_RES_PARALLEL: 'Lp-Rp',
#             cls.IND_PARALLEL_QUALITY: 'Lp-Q',
#             cls.IND_SERIES_RES_SERIES: 'Ls-Rs',
#             cls.IND_SERIES_QUALITY: 'Ls-Q',
#             cls.RES_SERIES_QUALITY: 'Rs-Q',
#             cls.RES_PARALLEL_QUALITY: 'Rp-Q',
#             cls.RES_REACTANCE: 'R-X',
#             cls.IMPEDANCE_PHASE_RAD: 'Z-thr',
#             cls.IMPEDANCE_PHASE_DEG: 'Z-thd',
#             cls.IMPEDANCE_DISSIPATION: 'Z-D',
#             cls.IMPEDANCE_QUALITY: 'Z-Q',
#             cls.DCRES: 'DCR'
#         }
#
#     @classmethod
#     def get_reading_map(cls):
#         return {
#             cls.CAP_SERIES_RES_SERIES: (('capacitance_series', units.farad), ('resistance_series', units.ohm)),
#             cls.CAP_SERIES_DISSIPATION: (('capacitance_series', units.farad),
#                                          ('dissipation_factor', units.dimensionless)),
#             cls.CAP_PARALLEL_RES_PARALLEL: (('capacitance_parallel', units.farad),
#                                             ('resistance_parallel', units.ohm)),
#             cls.CAP_PARALLEL_DISSIPATION: (('capacitance_parallel', units.farad),
#                                            ('dissipation_factor', units.dimensionless)),
#             cls.IND_PARALLEL_RES_PARALLEL: (('inductance_parallel', units.henry),
#                                             ('resistance_parallel', units.ohm)),
#             cls.IND_PARALLEL_QUALITY: (('inductance_parallel', units.henry),
#                                        ('quality_factor', units.dimensionless)),
#             cls.IND_SERIES_RES_SERIES: (('inductance_series', units.henry), ('resistance_series', units.ohm)),
#             cls.IND_SERIES_QUALITY: (('inductance_series', units.henry), ('quality_factor', units.dimensionless)),
#             cls.RES_SERIES_QUALITY: (('resistance_series', units.ohm), ('quality_factor', units.dimensionless)),
#             cls.RES_PARALLEL_QUALITY: (('resistance_parallel', units.ohm),
#                                        ('quality_factor', units.dimensionless)),
#             cls.RES_REACTANCE: (('resistance', units.ohm), ('reactance', units.ohm)),
#             cls.IMPEDANCE_PHASE_RAD: (('impedance_mag', units.ohm), ('impedance_phase_rad', units.rad)),
#             cls.IMPEDANCE_PHASE_DEG: (('impedance_mag', units.ohm), ('impedance_phase_deg', units.deg)),
#             cls.IMPEDANCE_DISSIPATION: (('impedance', units.ohm), ('dissipation_factor', units.dimensionless)),
#             cls.IMPEDANCE_QUALITY: (('impedance', units.ohm), ('quality_factor', units.dimensionless)),
#             cls.DCRES: (('resistance', units.ohm),)
#         }
#
#
# class GWInstekLCR6000Series(SCPIHardware):
#     """ GW INSTEK 6000-Series LCR meters. """
#
#     def __init__(self, *args, **kwargs):
#         # Configure baud rate for serial link
#         super().__init__(*args, visa_open_args={
#             'baud_rate': 115200
#         }, rate_limit_interval=0.05, **kwargs)
#
#     # Abstract methods from SCPIHardware
#     def display_msg(self, msg: str = None):
#         if msg is not None:
#             if len(msg) > 30:
#                 self._logger.warning("Truncating message to 30 characters (original: {})".format(msg))
#                 msg = msg[:30]
#
#             msg = msg.replace(':', ' ')
#
#             with self.get_hardware_lock():
#                 self.visa_write(":DISP:LINE \"{}\"".format(msg))
#
#     def _get_error(self) -> typing.Union[None, str, typing.Tuple[int, str]]:
#         msg = self.visa_query(':ERR?')
#
#         if '*E00' in msg:
#             return None
#
#         return msg
#
#     @staticmethod
#     def get_hardware_description() -> str:
#         return 'GW-INSTEK 6000 Series LRC Meter'
#
#     def handle_setup(self, event: EventData):
#         pass
#
#     def handle_start(self, event: EventData):
#         pass
#
#     def handle_pause(self, event: EventData):
#         pass
#
#     def handle_resume(self, event: EventData):
#         pass
#
#     def handle_stop(self, event: EventData):
#         pass
#
#     def handle_cleanup(self, event: EventData):
#         pass
#
#     def handle_error(self, event: EventData):
#         pass
