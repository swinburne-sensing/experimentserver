# -*- coding: utf-8 -*-

from certifi import __version__ as __certifi_version
from colorama import __version__ as __colorama_version
# noinspection PyProtectedMember
from influxdb import __version__ as __influxdb_version
from flask import __version__ as __flask_version
from pint import __version__ as __pint_version
from psutil import __version__ as __psutil_version
from pyvisa import __version__ as __pyvisa_version
from requests import __version__ as __requests_version
from transitions import __version__ as __transitions_version
# noinspection PyProtectedMember
from urllib3 import __version__ as __urllib3_version
from wrapt import __version__ as __wrapt_version
from yaml import __version__ as __yaml_version
from zmq import zmq_version

__app_name__ = 'experimentserver'
__version__ = '0.2.0'

__author__ = 'Chris Harrison'
__credits__ = ['Chris Harrison']

__maintainer__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'

__status__ = 'Development'

__dependencies__ = {
    'certfif': __certifi_version,
    'colorama': __colorama_version,
    'Flask': __flask_version,
    'python-influxdb': __influxdb_version,
    'pint': __pint_version,
    'psutil': __psutil_version,
    'python-pushover': '0.4',
    'pyvisa': __pyvisa_version,
    'requests': __requests_version,
    'transitions': __transitions_version,
    'urllib3': __urllib3_version,
    'pyyaml': __yaml_version,
    'pyzmq': zmq_version(),
    'wrapt': __wrapt_version
}

python_version_tested = [(3, 7)]
