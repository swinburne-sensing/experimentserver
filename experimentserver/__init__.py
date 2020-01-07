# -*- coding: utf-8 -*-
import os.path

__app_name__ = 'experimentserver'
__version__ = '0.2.1'

__author__ = 'Chris Harrison'
__credits__ = ['Chris Harrison']

__maintainer__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'

__status__ = 'Development'


ROOT_PATH = os.path.dirname(os.path.abspath(__file__))


class ApplicationException(Exception):
    """ Base exception for all custom application exceptions that occur during runtime. """
    pass
