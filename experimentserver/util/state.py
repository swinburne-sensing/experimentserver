import threading

import transitions


class SynchronizedMachine:
    def __init__(self, states, initial):
        super().__init__()

        self._machine = transitions.Machine(states=states, initial=initial)
