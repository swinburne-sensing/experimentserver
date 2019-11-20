from .config import ConfigManager
from .data import TYPING_TAGS
from .util.logging import get_logger
from .ui.web import WebUIThread


_LOGGER = get_logger(__name__)


def main(config: ConfigManager, metadata: TYPING_TAGS):
    # Setup hardware instances

    # Start web server
    webui = WebUIThread(config)
    webui.start()
    webui.join()
