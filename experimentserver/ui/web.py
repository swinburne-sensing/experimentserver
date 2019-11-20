import threading

import flask
from flask import request

from experimentserver.config import ConfigManager
from experimentserver.util.logging import LoggerObject


class WebUIThread(LoggerObject):
    def __init__(self, config: ConfigManager):
        super().__init__()

        self._config = config

        # Create flask instance
        self._app = flask.Flask(__name__)

        # Register methods
        self._register_route()

        # Thread for hosting
        self._thread = threading.Thread(target=self._thread_serve)

    def start(self):
        self._thread.start()

    def join(self):
        self._thread.join()

    def _register_route(self):
        @self._app.route('/')
        def page_index():
            return 'Hello world'

        @self._app.route('/shutdown')
        def page_shutdown():
            self._logger.info('Shutdown requested via web interface', notify=True)

            # Fetch server shutdown method
            shutdown_func = request.environ.get('werkzeug.server.shutdown')

            if shutdown_func is None:
                raise RuntimeError('Not running with the Werkzeug Server')

            # Call shutdown method
            shutdown_func()

            return "Shutting down..."

    def _thread_serve(self):
        self._logger.info('Started WebUI')

        try:
            self._app.run(host=self._config.get('server.host'), port=self._config.get('server.port'),
                          debug=self._config.get('server.debug', default=True), use_reloader=False)
        except Exception:
            self._logger.exception('Unhandled exception in web interface thread')
            raise

        self._logger.info('Stopped WebUI')
