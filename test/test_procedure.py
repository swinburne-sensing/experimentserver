import yaml

from experimentserver.config import ConfigManager
from experimentserver.protocol import Procedure
from experimentserver.protocol.stage.core import Delay, Setup
from experimentserver.protocol.stage.flow import Pulse


config = ConfigManager()


proc1 = Procedure(config=config)
proc1.add_stage(Delay, interval='1min')
proc1.add_stage(Delay, interval='3min')

# Export
d = proc1.procedure_export()
s = yaml.dump(d)
print(s)

# Import
proc2 = Procedure.procedure_import(s)
print(proc2)

pass
