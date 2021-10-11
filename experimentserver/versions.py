from colorama import __version__ as __colorama_version
# noinspection PyProtectedMember
from influxdb_client import __version__ as __influxdb_version
from jinja2 import __version__ as __jinja2_version
from flask import __version__ as __flask_version
from pint import __version__ as __pint_version
from psutil import __version__ as __psutil_version
from pymodbus import __version__ as __pymodbus_version
from pyvisa import __version__ as __pyvisa_version
from serial import __version__ as __pyserial_version
from requests import __version__ as __requests_version
from transitions import __version__ as __transitions_version
# noinspection PyProtectedMember
from urllib3 import __version__ as __urllib3_version
from wrapt import __version__ as __wrapt_version
from yaml import __version__ as __yaml_version

dependencies = {
    'colorama': __colorama_version,
    'Flask': __flask_version,
    'influxdb-client': __influxdb_version,
    'jinja2': __jinja2_version,
    'pint': __pint_version,
    'psutil': __psutil_version,
    'pymodbus': __pymodbus_version,
    'pyserial': __pyserial_version,
    'python-pushover': '0.4',
    'pyvisa': __pyvisa_version,
    'pyyaml': __yaml_version,
    'requests': __requests_version,
    'transitions': __transitions_version,
    'typing_inspect': '0.5.0',
    'urllib3': __urllib3_version,
    'wrapt': __wrapt_version
}

python_version_tested = [(3, 7), (3, 8), (3, 9)]
