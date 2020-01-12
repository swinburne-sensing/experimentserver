import abc
import collections
import typing


class Metadata(metaclass=abc.ABCMeta):
    """  """

    def __init__(self, method: typing.Callable):
        """

        :param method:
        :param priority:
        """
        self.method = method

    def bind(self, target):
        return self.method.__get__(target, target.__class__)


TYPE_METADATA = typing.TypeVar('TYPE_METADATA', bound=Metadata)


class OrderedMetadata(Metadata):
    def __init__(self, method: typing.Callable, order):
        super(OrderedMetadata, self).__init__(method)
        
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
        return collections.OrderedDict(sorted(metadata_list.items(), key=lambda x: x[1]))

    return metadata_list


def register_metadata(metadata_factory: typing.Callable[..., Metadata], *metadata_args,
                      **metadata_kwargs):
    def wrapper(func):
        func.metadata = metadata_factory(func, *metadata_args, **metadata_kwargs)

        return func

    return wrapper
