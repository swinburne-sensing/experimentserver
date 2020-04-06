import logging
import time

from experimentserver.data.measurement import DummyTarget
from experimentserver.hardware import HardwareManager, HardwareTransition
from experimentserver.hardware.device.custom import AccelMonitor


logging.basicConfig(level=logging.DEBUG)


t = DummyTarget()


h = AccelMonitor('accel', 'COM6')
m = HardwareManager(h)
m.thread_start()

m.queue_transition(HardwareTransition.CONNECT)

time.sleep(1)
print(1)
