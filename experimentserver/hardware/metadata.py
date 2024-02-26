from __future__ import annotations

import functools
import inspect
import typing

from .error import MeasurementError, ParameterError
from experimentserver.util.metadata import BoundMetadataCall, OrderedMetadata
from experimentserver.measurement import T_FIELD_MAP, T_MEASUREMENT_RETURN, T_MEASUREMENT_SEQUENCE, Measurement, \
    MeasurementGroup


class _MeasurementMetadata(OrderedMetadata):
    """ Registered Hardware measurement method. """

    def __init__(self, method: CALLABLE_MEASUREMENT, description: str,
                 measurement_group: typing.Optional[MeasurementGroup] = None, default: bool = False,
                 force: bool = False, order: int = 50, user: bool = True, setup: typing.Optional[str] = None,
                 setup_args: typing.Optional[typing.List] = None, setup_kwargs: typing.Optional[typing.Dict] = None):
        """ Initialise _HardwareMeasurement.

        :param method:
        :param description: user readable description of the measurement(s) produced by this method
        :param measurement_group: optional MeasurementGroup for generated measurements
        :param default: if True measurement is enabled by default
        :param force: if True measurement is always enabled
        :param order: execution priority
        :param user:
        :param setup:
        :param setup_args:
        :param setup_kwargs:
        """
        super().__init__(method, description, order)

        # return_annotation = inspect.signature(method).return_annotation
        type_hints = typing.get_type_hints(method)

        if 'return' not in type_hints:
            raise MeasurementError(f"Registered measurement method {method!r} does not provide required return "
                                   "annotation")

        return_annotation = type_hints['return']

        if return_annotation != 'Measurement' and return_annotation != Measurement and \
                return_annotation != T_MEASUREMENT_SEQUENCE and return_annotation != 'T_MEASUREMENT_SEQUENCE' and \
                return_annotation != T_FIELD_MAP and return_annotation != 'T_FIELD_MAP':
            raise MeasurementError(f"Registered measurement method {method!r} must provide compatible "
                                   f"return annotation")

        if return_annotation == T_FIELD_MAP and measurement_group is None:
            raise MeasurementError('Registered measurement methods that return a dict must provide a '
                                   'MeasurementGroup')

        self.description = description
        self.measurement_group = measurement_group
        self.default = default
        self.force = force
        self.user = user
        self.setup = setup
        self.setup_args = setup_args or []
        self.setup_kwargs = setup_kwargs or {}


class _ParameterMetadata(OrderedMetadata):
    """  """

    def __init__(self, method: CALLABLE_PARAMETER, description: str, order: int = 50,
                 validation: typing.Optional[typing.Dict[str, typing.Callable[[typing.Any], bool]]] = None,
                 primary_key: typing.Optional[typing.List[str]] = None):
        """ Initialise _HardwareParameter.

        :param method: wrapped method
        :param description: user-readable description of the function(s) controlled by this parameter
        :param order: order for application of parameters when performed as a set, lower order is performed first
        :param validation: optional set of validation methods for each argument
        :param primary_key: optional primary key set for caching of parameter calls
        """
        super().__init__(method, description, order, primary_key)

        self.validation = validation

    def bind(self, target: object, **kwargs) -> BoundMetadataCall:
        # Validate arguments if enabled
        if self.validation is not None:
            for kwarg_name, kwarg_value in kwargs.items():
                if kwarg_name in self.validation:
                    # Bind method to parent
                    # noinspection PyUnresolvedReferences
                    validation = self.validation[kwarg_name].__get__(target, target.__class__)

                    if not validation(kwarg_value):
                        raise ParameterError(f"Parameter argument {kwarg_name} failed validation (value: "
                                             f"{kwarg_value!r})")

        return super().bind(target, **kwargs)

    # def export(self) -> typing.Dict[str, typing.Union[str, typing.Dict[int, str]]]:
    #     """
    #
    #     :return:
    #     """
    #     method_desc = {}
    #
    #     # Get method arguments
    #     method_args = dict(inspect.signature(self.method).parameters)
    #
    #     for arg_name, arg_properties in method_args.items():
    #         # Ignore self
    #         if arg_name == 'self':
    #             continue
    #
    #         # Search for preferred types, default to strings
    #         method_desc[arg_name] = 'str'
    #
    #         for arg_type in typing_inspect.get_args(arg_properties.annotation):
    #             if issubclass(arg_type, HardwareEnum):
    #                 # Override with enum
    #                 method_desc[arg_name] = {x.value: x.get_description() for x in arg_type}
    #                 break
    #             elif arg_type is Quantity:
    #                 # Override with quantity
    #                 method_desc[arg_name] = 'unit'
    #                 break
    #             elif arg_type is datetime:
    #                 # Override with date/time
    #                 method_desc[arg_name] = 'time'
    #                 break
    #             elif arg_type is timedelta:
    #                 # Override with time delta
    #                 method_desc[arg_name] = 'timedelta'
    #                 break
    #             elif arg_type is bool:
    #                 # Override with boolean
    #                 method_desc[arg_name] = 'bool'
    #                 break
    #
    #     return method_desc


# Parameter callable
CALLABLE_PARAMETER = typing.Callable[..., None]

# Measurement methods may return a dict, a single measurement, or a sequence of measurements
CALLABLE_MEASUREMENT = typing.Callable[[], T_MEASUREMENT_RETURN]
TYPE_PARAMETER_DICT = typing.MutableMapping[str, typing.Dict[str, typing.Any]]
TYPE_PARAMETER_COMMAND = typing.Union[None, TYPE_PARAMETER_DICT, typing.List[TYPE_PARAMETER_DICT]]
TYPE_PARAMETER_VALID_DICT = typing.MutableMapping[_ParameterMetadata, functools.partial]
