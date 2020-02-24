import os
import typing


def add_path(path: str) -> typing.NoReturn:
    """ Add specified path to the environment PATH variable

    :param path: path to add to PATH
    """
    os.environ['PATH'] = path + os.pathsep + os.environ['PATH']
