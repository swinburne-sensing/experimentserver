import typing

import experimentserver
import experimentserver.hardware as hardware
from .config import ConfigManager
from .data import TYPE_TAG_DICT
from .experiment import Procedure
from .hardware.state.manager import StateManager
from .util.logging import get_logger
from .util.module import class_instance_from_dict
from .ui.web import WebUI


_LOGGER = get_logger(__name__)


class ServerException(experimentserver.ApplicationException):
    pass


def start_server(config: ConfigManager, metadata: TYPE_TAG_DICT):
    # Hardware and state instances
    hardware_list: typing.Dict[str, hardware.Hardware] = {}
    manager_list: typing.Dict[str, StateManager] = {}

    # Setup hardware instances and managers
    for hardware_inst_config in config.get('hardware', default=[]):
        _LOGGER.info("Hardware {}".format(hardware_inst_config))

        try:
            # Create hardware object
            hardware_inst = class_instance_from_dict(hardware_inst_config, hardware)

            hardware_inst = typing.cast(hardware.Hardware, hardware_inst)

            hardware_list[hardware_inst.get_hardware_identifier()] = hardware_inst

            # Create and start state
            manager_inst = StateManager(hardware_inst)
            manager_inst.start()

            manager_list[hardware_inst.get_hardware_identifier()] = manager_inst
        except Exception as exc:
            raise ServerException(f"Failed to initialise hardware using configuration: {hardware_inst_config!r}") \
                from exc

    # Create experiment procedure
    procedure = Procedure(manager_list)

    # Start web server
    ui = WebUI(config, metadata, manager_list, procedure)
    ui.run()

    # Cleanup managers
    for manager_inst in manager_list.values():
        manager_inst.stop()
