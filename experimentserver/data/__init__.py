import enum
import typing

import pint


# Create shared data registry
units = pint.UnitRegistry(autoconvert_offset_to_baseunit=True)

# Change default printing format
units.default_format = '.3g~P#'

# Define some additional units
units.define('percent = count / 100 = pct')
units.define('ppm = count / 1e6')
units.define('ppb = count / 1e9')
units.define('ppt = count / 1e12')
units.define('standard_cubic_centimeter_per_minute = cm ** 3 / min = sccm')

# Shorthand
Quantity = units.Quantity

TYPING_RECORD = typing.Dict[str, typing.Any]
TYPING_TAGS = typing.Dict[str, typing.Any]
TYPING_UNIT = typing.Union[float, str, Quantity]
TYPING_UNIT_OPTIONAL = typing.Optional[TYPING_UNIT]


class RecordType(enum.Enum):
    """ Definition for known types of hardware or measurements. """
    EVENT = 'event'
    HUMIDITY = 'humidity'
    LCR = 'lcr'
    FLOW = 'mfc'
    MULTICHANNEL = 'multichannel'
    MULTIMETER = 'multimeter'
    TEMPERATURE = 'temperature'
    PICOAMMETER = 'picoammeter'
    NANOVOLTMETER = 'nanovoltmeter'
    POWER_SUPPLY = 'psu'


def is_unit(obj: typing.Any) -> bool:
    """ Test if a variable is a Quantity.

    :param obj: object to test
    :return: True when the object is a data class, False when it is not
    """
    return type(obj) is Quantity


def to_unit(x: typing.Union[str, int, float, Quantity],
            default_unit: typing.Optional[typing.Union[str, units.Unit]] = None,
            magnitude: bool = False) -> Quantity:
    """ Create a data from arbitrary input.

    If input is already a Quantity then it is returned in prefered units. If a number or a string then a new Quantity is
    created, using pint for recognition of input.

    :param x:
    :param default_unit:
    :param magnitude:
    :return:
    """
    # Ignore empty inputs
    if x is None:
        return None

    # Fetch data if provided as a string
    if type(default_unit) is str:
        if hasattr(units, default_unit):
            default_unit = getattr(units, default_unit)
        else:
            raise ValueError("Unknown default data {}".format(default_unit))

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
