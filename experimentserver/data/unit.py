import typing
from datetime import timedelta

import pint

# Unit registry
units = pint.UnitRegistry(autoconvert_offset_to_baseunit=True)

# Change default printing format
units.default_format = '.3g~P#'

# Define some additional units
units.define('percent = count / 100 = pct')
units.define('ppm = count / 1e6')
units.define('ppb = count / 1e9')
# Broken?
# units.define('ppt = count / 1e12')
units.define('standard_cubic_centimeter_per_minute = cm ** 3 / min = sccm')


# Shorthand
Quantity = units.Quantity


# Values are optional fields that represent data of some sort
TYPE_VALUE = typing.Union[None, str, int, float, bool, Quantity, timedelta]

# Types for units
TYPE_UNIT = typing.Union[str, float, Quantity]
TYPE_UNIT_OPTIONAL = typing.Optional[TYPE_UNIT]

# Type for timedelta or delay
TYPE_TIME = typing.Union[float, str, timedelta]


def is_unit(obj: typing.Any) -> bool:
    """ Test if a variable is a Quantity.

    :param obj: object to test
    :return: True when the object is a data class, False when it is not
    """
    return type(obj) is Quantity


def to_unit(x: TYPE_VALUE, default_unit: typing.Optional[typing.Union[str, units.Unit]] = None,
            magnitude: bool = False, allow_none: bool = True, apply_round: typing.Optional[int] = None) -> Quantity:
    """ Create a data from arbitrary input.

    If input is already a Quantity then it is returned in preferred units. If a number or a string then a new Quantity
    is created, using pint for recognition of input.

    :param x:
    :param default_unit: if provided this unit will be used if none is provided, if a unit is provided in input then \
        input will be cast to this unit
    :param magnitude: if True return the magnitude of the unit (nomalised to base unit)
    :param allow_none: if False then exception is thrown if input is None or empty
    :param apply_round:
    :return:
    """
    # Ignore empty inputs
    if x is None or (type(x) is str and len(x) == 0):
        if not allow_none:
            raise ValueError('None or empty string not allowed in this context')

        return None

    # Fetch data if provided as a string
    if type(default_unit) is str:
        if hasattr(units, default_unit):
            default_unit = getattr(units, default_unit)
        else:
            raise ValueError(f"Unknown default data {default_unit}")

    # If input is already a data handle conversion (if necessary)
    if is_unit(x):
        # FIXME Causes crash at runtime even though this should do nothing
        # x = typing.cast(x, Quantity)

        # Convert if necessary
        if default_unit is not None:
            # noinspection PyUnresolvedReferences
            x = x.to(default_unit)

        # Return magnitude if requested
        if magnitude:
            return x.magnitude
        else:
            return x

    # Fix handling of percent sign
    if type(x) is str:
        x = x.replace('%', 'percent')

    # Convert string to a data
    x = Quantity(x)

    # Apply rounding
    if apply_round is not None:
        x = Quantity(round(x.magnitude, apply_round), x.units)

    # Remove unitless units (percent, ppm, etc)
    if x.dimensionless:
        x = x.to('dimensionless')

    if default_unit is not None:
        if x.dimensionless:
            # Cast to a new data
            x = Quantity(x.magnitude, default_unit)
        else:
            # Convert to desired data
            x = x.to(default_unit)

    if magnitude:
        return x.magnitude
    else:
        return x


def to_timedelta(x: TYPE_VALUE, allow_none: bool = False) -> typing.Optional[timedelta]:
    x = to_unit(x, 'sec', magnitude=True, allow_none=allow_none)

    if x is None:
        return None

    if type(x) is timedelta:
        return x

    return timedelta(seconds=x)
