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

TYPE_ARGUMENT = typing.Union[None, str, int, float, bool, Quantity]
TYPE_FIELD_DICT = typing.Dict[str, TYPE_ARGUMENT]
TYPE_TAG_DICT = typing.Dict[str, TYPE_ARGUMENT]
TYPE_UNIT = typing.Union[str, float, Quantity]
TYPE_UNIT_OPTIONAL = typing.Optional[TYPE_UNIT]


class MeasurementGroup(enum.Enum):
    """ Definition for known types of hardware or measurements. """
    # Metadata
    EVENT = 'event'
    STATUS = 'status'

    # Electrical measurements
    VOLTAGE = 'voltage'
    CURRENT = 'current'
    RESISTANCE = 'resistance'

    # LCR
    CAPACITANCE = 'capacitance'
    INDUCTANCE = 'inductance'
    DISSIPATION = 'dissipation'
    QUALITY = 'quality'

    # Measure current, supply voltage
    CONDUCTOMETRIC_IV = 'conductometric_iv'

    # Measure voltage, supply current
    CONDUCTOMETRIC_VI = 'conductometric_vi'

    # Environmental conditions
    TEMPERATURE = 'temperature'
    HUMIDITY = 'humidity'

    # CV/CC power supply
    SUPPLY = 'supply'

    # Frequency
    FREQUENCY = 'frequency'
    PERIOD = 'period'

    # Complex signals
    TIME_DOMAIN_SAMPLE = 'timedomain'
    FREQUENY_DOMAIN_SAMPLE = 'freqdomain'
    TIME_FREQUENCY_SAMPLE = 'tfdomain'


TYPING_MEASUREMENT_SERIES = typing.Iterable[typing.Tuple[MeasurementGroup, TYPE_FIELD_DICT]]


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
