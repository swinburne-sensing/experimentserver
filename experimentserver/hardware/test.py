from . import Hardware


class TestHardware(Hardware):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_hardware_description() -> str:
        return 'Test Hardware'

    def handle_setup(self):
        pass

    def handle_start(self):
        pass

    def handle_stop(self):
        pass

    def handle_pause(self):
        pass

    def handle_resume(self):
        pass

    def handle_error(self):
        pass



