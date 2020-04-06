from __future__ import annotations

import abc
import functools
import inspect
import importlib
import os.path
import pkgutil
import threading
import typing
import sys
import weakref

from .logging import get_logger

import experimentserver


_LOGGER = get_logger(__name__)


class InitialisationError(Exception):
    pass


class TrackedIdentifierError(Exception):
    pass


class OptionalModule(Exception):
    """ Exception for modules imported via import_submodules, if an error occurs during initialisation then this module
     may be skipped. """
    pass


class __TrackedMeta(type):
    """ Metaclass for instance tracking classes. """

    def __new__(mcs, *args, **kwargs):
        class_instance = super().__new__(mcs, *args, **kwargs)

        # Create empty instance list for class
        class_instance.__instances__ = {}
        class_instance.__instances_lock__ = threading.Lock()

        return class_instance

    def __call__(cls, *args, **kwargs):
        with cls.__instances_lock__:
            instance = super().__call__(*args, **kwargs)
            identifier = instance.get_identifier()

            # Save instance to class list
            if identifier in cls.__instances__:
                raise TrackedIdentifierError(f"Identifier {identifier} already exists in tracked class {cls.__name__}")

            cls.__instances__[identifier] = weakref.ref(instance)

        return instance


__AbstractTrackedMeta = type('__AbstractTrackedMeta', (abc.ABCMeta, __TrackedMeta), {})


TTracked = typing.TypeVar('TTracked', bound='Tracked')


class Tracked(object, metaclass=__TrackedMeta):
    """ Base class for objects tracked by a unique identifier. """

    def __init__(self, identifier: str):
        self._identifier = identifier

    def get_identifier(self):
        """ Get th unique identifier string for this object.

        :return: str
        """
        return self._identifier

    @classmethod
    def get_instance(cls: typing.Type[TTracked], identifier: str) -> TTracked:
        """ Get an instance of class given that objects unique identifier.

        :param identifier: unique identifier string
        :return: object
        """
        try:
            return cls.get_all_instances()[identifier]
        except KeyError:
            raise TrackedIdentifierError(f"Invalid identifier {identifier} for tracked class {cls.__name__}")

    @classmethod
    def get_all_instances(cls: typing.Type[TTracked], include_subclass: bool = True) -> typing.Dict[str, TTracked]:
        """ Get all instances of this class. Optionally include subclasses.

        :param include_subclass: if True then subclasses of this class will be included in the search.
        :return: dict
        """
        instances = {}

        with cls.__instances_lock__:
            invalid_identifiers = []

            for identifier, reference in cls.__instances__.items():
                instance = reference()

                # Check instance still exists
                if instance is not None:
                    instances[identifier] = instance
                else:
                    invalid_identifiers.append(identifier)

            # Discard any broken references
            for identifier in invalid_identifiers:
                cls.__instances__.pop(identifier)

        if include_subclass:
            # Recurse through subclasses getting instances
            for subclass in cls.__subclasses__():
                # Subclasses are by definition a subclass of this class
                subclass = typing.cast(cls, subclass)

                instances.update(subclass.get_all_instances())

        return instances


class AbstractTracked(Tracked, metaclass=__AbstractTrackedMeta):
    pass


# From https://stackoverflow.com/questions/18078744/python-hybrid-between-regular-method-and-classmethod
class HybridMethod(object):
    """ Wrapper for method that can be called on an instance or on a class. """

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


# From http://stackoverflow.com/questions/1176136/convert-string-to-python-class-object
def class_from_str(class_name: str, parent: typing.Any):
    """ Get a class from a given module given the classes name as a string.

    :param class_name: Name of the class to instantiate
    :param parent: Parent module of class
    :return: class type
    """
    if type(parent) is str:
        try:
            parent = sys.modules[parent]
        except KeyError:
            raise KeyError(f"Module {parent} is not available")

    return functools.reduce(getattr, class_name.split('.'), parent)


def class_instance_from_str(class_name: str, parent: typing.Any, *args, **kwargs):
    """ Instantiate a class from a given module given the class name as a string.

    :param class_name: Name of the class to instantiate
    :param parent: Parent module of class
    :param args: Positional arguments to pass to class constructor
    :param kwargs: Keyword arguments to pass to class constructor
    :return: class instance
    """
    class_type = class_from_str(class_name, parent)

    # # Get init arguments and self argument
    # init_parameters = inspect.signature(class_type.__init__).parameters.copy()
    # init_parameters.pop('self')
    #
    # # Validate arguments
    # for arg in args:
    #     init_arg = init_parameters.popitem(last=False)
    #
    # for kwarg_name, kwarg_value in kwargs.items():
    #     init_kwarg = init_parameters.pop(kwarg_name)
    #
    #     if typing_inspect.is_union_type(init_kwarg.annotation):
    #         for init_kwarg_type in typing_inspect.get_args(init_kwarg.annotation):
    #             pass
    #     else:
    #         # Try to cast passed argument
    #         init_kwarg.annotation(kwarg_value)

    return class_type(*args, **kwargs)


def class_instance_from_dict(class_dict: typing.Dict[str, typing.Any], parent: typing.Any):
    """ Instantiate a class from a given module given a dict containing the class name as a value in the dict.

    :param class_dict: Object description as a dict, requires at least a value for 'class'
    :param parent: Parent module of class
    :return: class instance
    """
    class_name = class_dict.pop('class')

    return class_instance_from_str(class_name, parent, **class_dict)


def get_call_context(discard_calls: int = 1, filter_app: bool = True) -> typing.List[str]:
    """ Get a list of contextual calls in the stack.

    :param discard_calls: number of stacks calls to discard (0 would include call to this method)
    :param filter_app: if True only calls to methods in this application (no external modules or built-ins)
    :return: list
    """
    # Derive the base path for this application
    app_root = os.path.dirname(experimentserver.__file__)

    context = inspect.stack()
    context_list = []

    # Filter stack frames from this application
    if filter_app:
        context = [x for x in context[discard_calls:] if x[1].startswith(app_root)]

    for frame in context:
        frame_path = frame[1]

        if filter_app:
            frame_path = frame_path[len(app_root):]

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

        try:
            results[full_name] = importlib.import_module(full_name)
            if recursive and is_pkg:
                results.update(import_submodules(full_name))
        except OptionalModule as exc:
            _LOGGER.info(f"Skipped import of optional module {full_name}", exc_info=exc)

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


TClass = typing.TypeVar('TClass', bound=object)


def get_all_subclasses(class_root: typing.Type[TClass]) -> typing.List[typing.Type[TClass]]:
    """ Get a list of subclasses for a given parent class.

    :param class_root: parent class type
    :return: list
    """
    return __recurse_subclasses([class_root])
