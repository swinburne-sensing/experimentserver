import functools
import importlib
import pkgutil
import typing
import sys


# From http://stackoverflow.com/questions/1176136/convert-string-to-python-class-object
def class_from_str(class_name: str, parent: typing.Any):
    """
    Gets a class based upon a string
    :param class_name: Name of the class to instantiate
    :param parent: Parent module of class
    :return:
    """
    if type(parent) is str:
        parent = sys.modules[parent]

    return functools.reduce(getattr, class_name.split('.'), parent)


def class_instance_from_str(class_name: str, parent: typing.Any, *args, **kwargs):
    """
    Create an instance of a class based upon a name in a string
    :param class_name: Name of the class to instantiate
    :param parent: Parent module of class
    :param args: Positional arguments to pass to class constructor
    :param kwargs: Keyword arguments to pass to class constructor
    :return: An instance of the named class
    """
    class_type = class_from_str(class_name, parent)

    return class_type(*args, **kwargs)


def class_instance_from_dict(class_dict: typing.Dict[str, typing.Any], parent: typing.Any):
    class_name = class_dict.pop('class')

    return class_instance_from_str(class_name, parent, **class_dict)


def import_submodules(package, recursive=True):
    """
    Import submodules within a given package.
    :param package: base package to begin import from
    :param recursive: if True then
    :return:
    """
    if isinstance(package, str):
        package = importlib.import_module(package)

    results = {}

    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = package.__name__ + '.' + name
        results[full_name] = importlib.import_module(full_name)
        if recursive and is_pkg:
            results.update(import_submodules(full_name))

    return results


def __recurse_subclasses(subclass_list: typing.List[type]) -> typing.List[type]:
    return_list = []

    for subclass in subclass_list:
        if len(subclass.__subclasses__()) > 0:
            return_list.extend(__recurse_subclasses(subclass.__subclasses__()))
        else:
            return_list.append(subclass)

    return return_list


def get_all_subclasses(class_root: type):
    return __recurse_subclasses([class_root])
