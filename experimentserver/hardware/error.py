from experimentserver import ApplicationException


class HardwareError(ApplicationException):
    """ Base exception for all errors thrown by Hardware devices. """
    pass


class HardwareInitError(HardwareError):
    """ Exception thrown during initialisation. """
    pass


class CommandError(HardwareError):
    """ Base exception for errors caused by setting parameters or taking measurements.

    May be attached to a HardwareCommunicationError or HardwareReportedError.
    """
    pass


class CommunicationError(HardwareError):
    """ Base exception for IO and other Hardware communication errors.

    This covers errors that are reported server side, and may require reconnection of the Hardware in order to
    recover.
    """
    pass


class ExternalError(HardwareError):
    """ Base exception for Hardware reported errors or for errors caused by unexpected or invalid instrument response.

    This covers errors reported by the Hardware itself, and may not require reconnection of the Hardware in order to
    recover."""
    pass


class MeasurementError(CommandError):
    """ Exception thrown during measurement capture. """
    pass


class MeasurementUnavailable(MeasurementError):
    pass


class ParameterError(CommandError):
    """ Exception thrown during parameter configuration. """
    pass


class NoResetHandler(ApplicationException):
    """ Exception to indicate that reset events should be handled as setup events. """
    pass
