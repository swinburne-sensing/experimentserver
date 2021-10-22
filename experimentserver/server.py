from experimentserver import ApplicationException
from experimentserver.config import ConfigManager
from experimentserver.measurement import T_TAG_MAP
from experimentserver.ui.web import WebServer
from experimentserver.ui.html import register_html
from experimentserver.ui.json import register_json


class ServerException(ApplicationException):
    pass


def start_server(config: ConfigManager, app_metadata: T_TAG_MAP, user_metadata: T_TAG_MAP):
    # Setup web server
    ui = WebServer(config, app_metadata, user_metadata)
    register_html(ui)
    register_json(ui)

    # Start web server
    ui.run()
