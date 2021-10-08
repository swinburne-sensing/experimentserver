from __future__ import annotations

import abc
import collections
import functools
import inspect
import typing


class BoundMetadataCall(object):
    """  """

    def __init__(self, parent: Metadata, target: object, **kwargs):
        """

        :param parent:
        :param target:
        :param args:
        :param kwargs:
        """
        super(BoundMetadataCall, self).__init__()
        
        self.parent = parent
        self.kwargs = kwargs
        # noinspection PyUnresolvedReferences
        self.partial = functools.partial(self.parent.method.__get__(target, target.__class__), **kwargs)

    def __call__(self, **kwargs):
        return self.partial(**kwargs)

    def __hash__(self):
        if self.parent.primary_key is None:
            return super(BoundMetadataCall, self).__hash__()
        
        # Generate hash based off arguments
        key = []

        for attribute in self.parent.primary_key:
            if attribute in self.partial.keywords:
                key.append(self.partial.keywords[attribute])
            else:
                key.append(None)

        return hash(tuple(key))

    def __lt__(self, other):
        # Sort calls by parent
        return self.parent < other.parent

    def __str__(self) -> str:
        kwargs = [f"{arg}={value!s}" for arg, value in self.kwargs.items()]

        # FIXME Bad type hints
        return f"{self.parent.description}: {', '.join(kwargs)}"

    def __repr__(self):
        kwargs = [f"{arg}={value!s}" for arg, value in self.kwargs.items()]

        return f"{self.partial.func!s}({', '.join(kwargs)})"


class Metadata(metaclass=abc.ABCMeta):
    """  """

    def __init__(self, method: typing.Callable, primary_key: typing.Optional[typing.List[str]] = None):
        """

        :param method:
        :param primary_key
        """
        self.method = method
        self.signature = inspect.signature(method)
        self.primary_key = primary_key or tuple(self.signature.parameters)[1:]

    def bind(self, target: object, **kwargs) -> BoundMetadataCall:
        """ Bind the wrapped method to a given parent class.

        :param target: class to bind the method to
        :param kwargs: arguments to pass to bound method
        :return: BoundMetadataCall partial wrapper for the enclosed method
        """
        return BoundMetadataCall(self, target, **kwargs)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.method})"


TYPE_METADATA = typing.TypeVar('TYPE_METADATA', bound=Metadata)


class OrderedMetadata(Metadata):
    """  """

    def __init__(self, method: typing.Callable, order: int, primary_key: typing.Optional[typing.List[str]] = None):
        """

        :param method:
        :param order:
        :param primary_key:
        """
        super(OrderedMetadata, self).__init__(method, primary_key)
        
        self.order = order

    def __lt__(self, other):
        assert issubclass(other.__class__, self.__class__)

        return self.order < other.order


def get_metadata(target: typing.Any, class_filter: typing.Optional[typing.Type[TYPE_METADATA]] = None) \
        -> typing.MutableMapping[str, TYPE_METADATA]:
    """

    :param target:
    :param class_filter:
    :return:
    """
    if class_filter is None:
        class_filter = Metadata

    metadata_list: typing.Dict[str, Metadata] = {}

    for attrib_name in dir(target):
        attrib = getattr(target, attrib_name)

        if not hasattr(attrib, 'metadata'):
            continue

        # Fetch metadata
        metadata = attrib.metadata

        # Check against class filter
        if issubclass(metadata.__class__, class_filter):
            metadata = typing.cast(Metadata, metadata)
            metadata_list[attrib_name] = metadata

    # If ordered metadata then return ordered dict
    if all((issubclass(x.__class__, OrderedMetadata) for x in metadata_list.values())):
        # noinspection PyTypeChecker
        return collections.OrderedDict(sorted(metadata_list.items(), key=lambda x: x[1]))

    return metadata_list


def register_metadata(metadata_factory: typing.Callable[..., Metadata], *metadata_args,
                      **metadata_kwargs):
    def wrapper(func):
        func.metadata = metadata_factory(func, *metadata_args, **metadata_kwargs)

        return func

    return wrapper
