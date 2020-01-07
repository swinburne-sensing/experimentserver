import functools
import inspect
import importlib
import os.path
import pkgutil
import typing
import sys

import experimentserver


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
    """

    :param class_dict:
    :param parent:
    :return:
    """
    class_name = class_dict.pop('class')

    return class_instance_from_str(class_name, parent, **class_dict)


def get_call_context(discard_calls: int = 1):
    """

    :param discard_calls:
    :return:
    """
    app_root = os.path.dirname(experimentserver.__file__)

    context = inspect.stack()
    context_list = []

    # Filter stack frames from this application
    context = [x for x in context[discard_calls:] if x[1].startswith(app_root)]

    for frame in context:
        frame_path = frame[1][len(app_root):]

        context_list.append(f"{frame_path}:{frame[3]}:{frame[2]}")

    return context_list


def import_submodules(package, recursive=True):
    """ Import all submodules within a given package.

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


def __recurse_subclasses(subclass_list):
    return_list = []

    for subclass in subclass_list:
        if len(subclass.__subclasses__()) > 0:
            return_list.extend(__recurse_subclasses(subclass.__subclasses__()))

            if not inspect.isabstract(subclass):
                return_list.append(subclass)
        else:
            return_list.append(subclass)

    return return_list


T = typing.TypeVar('T', bound=type)


def get_all_subclasses(class_root: typing.Type[T]) -> typing.List[typing.Type[T]]:
    """ Get a list of subclasses for a given parent class.

    :param class_root: parent class type
    :return: list
    """
    return __recurse_subclasses([class_root])


# From https://stackoverflow.com/questions/18078744/python-hybrid-between-regular-method-and-classmethod
class HybridMethod(object):
    """  """
    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        context = obj if obj is not None else cls

        @functools.wraps(self.func)
        def hybrid(*args, **kw):
            return self.func(context, *args, **kw)

        # optional, mimic methods some more
        hybrid.__func__ = hybrid.im_func = self.func
        hybrid.__self__ = hybrid.im_self = context

        return hybrid
