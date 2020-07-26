import typing
from datetime import timedelta

from . import StageConfigurationError
from .core import BaseStage
from ...config import ConfigManager
from ...data import Quantity, TYPE_TIME, to_timedelta, to_unit
from ...data.gas import GasConstant
from ...hardware.manager import HardwareManager
from ...hardware.device.mks import GE50MassFlowController


class FlowCalculationError(StageConfigurationError):
    pass


def get_available_gases(mfc_list: typing.Optional[typing.Sequence[GE50MassFlowController]] = None) \
        -> typing.Dict[GasConstant, typing.Tuple[Quantity, GE50MassFlowController]]:
    """ Get a list of available gases and the best source

    :param mfc_list:
    :return:
    """
    gases: typing.Dict[GasConstant, typing.Tuple[Quantity, GE50MassFlowController]] = {}

    if mfc_list is None:
        mfc_list = list(GE50MassFlowController.get_all_instances().values())

    # Find highest concentration of each available gas
    for mfc in mfc_list:
        for gas, concentration in mfc.get_composition().items():
            if gas not in gases or concentration > gases[gas][0]:
                gases[gas] = (concentration, mfc)

    return gases


def calculate_flow(composition: typing.Dict[GasConstant, Quantity], target_flow: Quantity, balance_gas: GasConstant,
                   mfc_list: typing.Optional[typing.Sequence[GE50MassFlowController]] = None) \
        -> typing.Sequence[typing.Tuple[GE50MassFlowController, Quantity]]:
    """

    :param composition:
    :param target_flow:
    :param balance_gas:
    :param mfc_list:
    :return:
    """
    if mfc_list is None:
        mfc_list = list(GE50MassFlowController.get_all_instances().values())

    available_gases = get_available_gases(mfc_list)

    flows: typing.List[typing.Tuple[GE50MassFlowController, float]] = [(mfc, to_unit(0, 'sccm')) for mfc in mfc_list]

    for gas, concentration in composition.items():
        # Test if gas is available
        if gas not in available_gases:
            raise FlowCalculationError(f"Composition component {gas.label} not available from configured MFCs")

        # Test if gas is concentration is suitable
        if concentration > available_gases[gas][1]:
            raise FlowCalculationError(f"Requested composition component {gas.get_concentration_label(concentration)} "
                                       f"exceeds concentration available (max: "
                                       f"{gas.get_concentration_label(available_gases[gas][1])})")

    # Check that calculated flows do not exceed 1
    total_flow = sum((flow[1] for flow in flows))

    if total_flow > 1:
        raise FlowCalculationError(f"Could not generate requested composition, mixture not possible")

    # Normalise flow rates against target_flow
    flows = [(mfc, target_flow * flow) for mfc, flow in flows]

    return flows


class Pulse(BaseStage):
    """ Exposure to specified gas concentration. """

    def __init__(self, config: ConfigManager, composition: typing.Dict[str, str], exposure: TYPE_TIME,
                 recovery: TYPE_TIME, uid: typing.Optional[str] = None, setup: typing.Optional[TYPE_TIME] = None,
                 idle: typing.Optional[typing.Dict[str, str]] = None):
        self._pulse_setup = to_timedelta(setup, True)
        self._pulse_exposure = to_timedelta(exposure)
        self._pulse_recovery = to_timedelta(recovery)

        self._idle_composition = {}
        self._pulse_composition = {}

        valid_mfc = HardwareManager.get_all_hardware_instances(GE50MassFlowController)

        for hardware, flow in idle.items():
            pass

        for hardware, flow in composition.items():
            pass

        metadata = {
            'pulse_setup': self._pulse_setup.total_seconds(),
            'pulse_exposure': self._pulse_exposure.total_seconds(),
            'pulse_recovery': self._pulse_recovery.total_seconds()
        }

        super(Pulse, self).__init__(config, uid, metadata)

    @staticmethod
    def get_config_dependencies() -> typing.Optional[typing.Sequence[str]]:
        return [
            'pulse_idle'
        ]

    def get_stage_duration(self) -> typing.Optional[timedelta]:
        if self._pulse_setup is None:
            return self._pulse_exposure + self._pulse_recovery
        else:
            return self._pulse_setup + self._pulse_exposure + self._pulse_recovery

    def stage_validate(self) -> typing.NoReturn:
        return super().stage_validate()

    def stage_enter(self) -> typing.NoReturn:
        super().stage_enter()

        # Zero unused MFCs
        HardwareManager.get_all_instances()

    def stage_run(self) -> bool:
        pass

    def stage_exit(self) -> typing.NoReturn:
        # Return to idle flow

        super().stage_exit()

    def stage_export(self) -> typing.Dict[str, typing.Any]:
        stage = super(Pulse, self).stage_export()

        return stage
