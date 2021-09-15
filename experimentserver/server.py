import experimentserver
from .config import ConfigManager
from .data.measurement import TYPE_TAG_DICT
from .util.logging import get_logger
from .ui.web import WebServer
from .ui.html import register_html
from .ui.json import register_json


_LOGGER = get_logger(__name__)


class ServerException(experimentserver.ApplicationException):
    pass


def start_server(config: ConfigManager, app_metadata: TYPE_TAG_DICT, user_metadata: TYPE_TAG_DICT):
    # Setup web server
    ui = WebServer(config, app_metadata, user_metadata)
    register_html(ui)
    register_json(ui)

    # Start web server
    ui.run()
