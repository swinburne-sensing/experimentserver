import math

from . import Quantity, to_unit, units, TYPE_UNIT


# Humidity calculation constants
_H20_T_CRITICAL = to_unit(647.0096, 'kelvin')
_H20_P_CRITICAL = to_unit(22.064000, 'MPa')
_PWS_C1 = -7.85951783
_PWS_C2 = 1.84408259
_PWS_C3 = -11.7866497
_PWS_C4 = 22.6807411
_PWS_C5 = -15.9618719
_PWS_C6 = 1.80122502
_HUMID_ABS_C = to_unit(2.16679, units.gram * units.kelvin / units.joule)

UNIT_ABS_HUMID = units.g / pow(units.meter, 3)


def _humidity_calc_pws_exp_constants(temperature: TYPE_UNIT) -> tuple:
    temperature = to_unit(temperature, 'celsius').magnitude

    if -70 <= temperature <= 0:
        a = 6.114742
        m = 9.778707
        tn = 273.1466
    elif 0 < temperature <= 50:
        a = 6.116441
        m = 7.591386
        tn = 240.7263
    elif 50 < temperature <= 100:
        a = 6.004918
        m = 7.337936
        tn = 229.3975
    elif 100 < temperature <= 150:
        a = 5.856548
        m = 7.27731
        tn = 225.1033
    else:
        raise NotImplementedError('Temperature outside supported range')

    return a, m, tn,


def _humidity_calc_pws_exp(temperature: TYPE_UNIT) -> Quantity:
    temperature = to_unit(temperature, 'celsius')

    (a, m, tn) = _humidity_calc_pws_exp_constants(temperature)

    return to_unit(a * pow(10, (m * temperature.magnitude) / (temperature.magnitude + tn)), 'hPa')


# Calculate dew point from absolute humidity
def abs_to_dew(temperature: TYPE_UNIT, absolute_humidity: TYPE_UNIT) -> Quantity:
    """ Convert absolute humidity concentration (g/m^3) to a dew point temperature at a specific gas temperature.

    :param temperature: temperature of gas
    :param absolute_humidity: water concentration in gas
    :return: dew point temperature as Quantity
    """
    return rel_to_dew(temperature, abs_to_rel(temperature, absolute_humidity))


def abs_to_rel(temperature: TYPE_UNIT, absolute_humidity: TYPE_UNIT) -> Quantity:
    """ Convert absolute humidity concentration (g/m^3) to a relative humidity (%) at a specific gas temperature.

    :param temperature: temperature of the gas
    :param absolute_humidity: water concentration in gas
    :return: relative humidity percentage as Quantity
    """
    temperature = to_unit(temperature, 'kelvin')
    absolute_humidity = to_unit(absolute_humidity, UNIT_ABS_HUMID)

    pw = temperature * absolute_humidity / _HUMID_ABS_C

    return to_unit((pw / _humidity_calc_pws_exp(temperature)).magnitude, 'percent')


# Calculate absolute humidity from dew point
def dew_to_abs(dew_temperature: TYPE_UNIT) -> Quantity:
    """ Convert dew point temperature to absolute humidity.

    :param dew_temperature: dew point temperature
    :return: water concentration in gas as Quantity
    """
    return rel_to_abs(dew_temperature, 1)


# Calculate relative humidity from temperature and dew point
def dew_to_rel(temperature: TYPE_UNIT, dew_temperature: TYPE_UNIT) -> Quantity:
    """ Convert dew point temperature to relative humidity.

    :param temperature: temperature of gas
    :param dew_temperature: dew point temperature
    :return:
    """
    pwd = _humidity_calc_pws_exp(dew_temperature)
    pws = _humidity_calc_pws_exp(temperature)

    return to_unit(pwd / pws, 'percent')


# Calculate dew point from temperature and relative_humidity
def rel_to_dew(temperature: TYPE_UNIT, relative_humidity: TYPE_UNIT) -> Quantity:
    """ Convert relative humidity to dew point temperature.

    :param temperature: temperature of gas
    :param relative_humidity:
    :return: dew point temperature as Quantity
    """
    temperature = to_unit(temperature, 'celsius')
    relative_humidity = to_unit(relative_humidity)

    (a, m, tn) = _humidity_calc_pws_exp_constants(temperature)
    pws = _humidity_calc_pws_exp(temperature) * relative_humidity

    return to_unit(tn / (m / (math.log10(pws.to('hPa').magnitude / a)) - 1), 'celsius')


# Calculate absolute humidity from temperature and relative humidity measurement
def rel_to_abs(temperature: TYPE_UNIT, relative_humidity: TYPE_UNIT) -> Quantity:
    """ Convert a relative humidity (%) at a specific temperature to absolute humidity concentration (g/m^3).

    :param temperature: temperature of gas
    :param relative_humidity:
    :return: water concentration in gas as Quantity
    """
    # Convert types
    temperature = to_unit(temperature, 'celsius')
    relative_humidity = to_unit(relative_humidity)

    pw = _humidity_calc_pws_exp(temperature) * relative_humidity

    return Quantity((_HUMID_ABS_C * pw.to('Pa') / temperature).magnitude, UNIT_ABS_HUMID)
