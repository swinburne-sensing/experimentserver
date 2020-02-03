import typing

import experimentserver
from .config import ConfigManager
from .data.measurement import TYPE_TAG_DICT
from .experiment.procedure import Procedure
from .experiment.stage import DelayStage
from .hardware import Hardware, device
from .hardware.manager import HardwareManager
from .util.logging import get_logger
from .util.module import class_instance_from_dict
from .ui.web import WebServer
from .ui.html import register_html
from .ui.json import register_json


_LOGGER = get_logger(__name__)


class ServerException(experimentserver.ApplicationException):
    pass


def start_server(config: ConfigManager, metadata: TYPE_TAG_DICT):
    # Hardware and manager instances
    hardware_list: typing.Dict[str, Hardware] = {}
    manager_list: typing.Dict[str, HardwareManager] = {}

    # Setup hardware instances and managers
    for hardware_inst_config in config.get('hardware', default=[]):
        _LOGGER.info("Hardware {}".format(hardware_inst_config))

        try:
            # Create hardware object
            hardware_inst = class_instance_from_dict(hardware_inst_config, device)

            hardware_inst = typing.cast(Hardware, hardware_inst)

            hardware_list[hardware_inst.get_hardware_identifier()] = hardware_inst

            # Create and start manager
            manager_inst = HardwareManager(hardware_inst)
            manager_inst.thread_start()

            manager_list[hardware_inst.get_hardware_identifier()] = manager_inst
        except Exception as exc:
            raise ServerException(f"Failed to initialise hardware using configuration: {hardware_inst_config!r}") \
                from exc

    # Create experiment procedure
    procedure = Procedure()
    procedure.thread_start()

    # FIXME remove in final
    procedure.add_hardware('multimeter')
    procedure.add_stage(DelayStage('30 secs', True))
    procedure.add_stage(DelayStage('0.1min'))
    procedure.add_stage(DelayStage('1min'))

    # Setup web server
    ui = WebServer(config, metadata, procedure)
    register_html(ui)
    register_json(ui)

    # Start web server
    ui.run()

    # Cleanup managers
    for manager_inst in manager_list.values():
        manager_inst.thread_stop()
