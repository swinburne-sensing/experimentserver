import typing

import experimentserver.data.export as export
import experimentserver.hardware as hardware
import experimentserver.hardware.manager as manager
from .config import ConfigManager
from .data import TYPING_TAG
from .util.logging import get_logger
from .util.module import class_instance_from_dict
from .ui.web import WebUI


_LOGGER = get_logger(__name__)


class ServerException(Exception):
    pass


def main(config: ConfigManager, metadata: TYPING_TAG):
    # Configure metadata
    export.add_tags(metadata)

    # Hardware instances
    hardware_list: typing.List[hardware.Hardware] = []

    # Setup hardware instances
    for hardware_inst_config in config.get('hardware', default=[]):
        _LOGGER.info("Hardware {}".format(hardware_inst_config))

        try:
            hardware_inst = class_instance_from_dict(hardware_inst_config, hardware)
            hardware_list.append(hardware_inst)
        except Exception as exc:
            raise ServerException(f"Failed to initialise hardware using configuration: {hardware_inst_config!r}") \
                from exc

    # Setup hardware managers
    manager_list: typing.Dict[str, manager.HardwareStateManager] = {}

    for hardware_inst in hardware_list:
        manager_inst = manager.HardwareStateManager(hardware_inst)

        # Start management thread
        manager_inst.start()

        manager_list[hardware_inst.get_hardware_identifier()] = manager_inst

    # Start web server
    ui = WebUI(config, metadata, manager_list)
    ui.run()

    # Cleanup managers
    for manager_inst in manager_list.values():
        manager_inst.stop()
