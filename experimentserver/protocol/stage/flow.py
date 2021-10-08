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
