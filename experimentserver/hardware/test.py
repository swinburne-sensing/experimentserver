from . import Hardware


# Author tag for support purposes
__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class TestHardware(Hardware):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_hardware_description() -> str:
        return 'Test Hardware'

    def handle_setup(self):
        super().handle_setup()

    def handle_start(self):
        super().handle_start()

    def handle_pause(self):
        super().handle_pause()

    def handle_resume(self):
        super().handle_resume()

    def handle_stop(self):
        super().handle_stop()

    def handle_cleanup(self):
        super().handle_cleanup()

    def handle_error(self):
        super().handle_error()
