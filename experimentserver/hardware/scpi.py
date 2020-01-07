import abc
import time
import typing

from transitions import EventData

import experimentserver
from . import HardwareError
from .visa import VISAHardware, VISACommunicationError, VISAHardwareError


TYPE_ERROR = typing.Union[None, str, typing.Tuple[int, str]]


class SCPIHardwareError(VISAHardwareError):
    pass


class SCPIDisplayUnavailable(experimentserver.ApplicationException):
    """ Error occurred during update of instrument display. """
    pass


class SCPIHardware(VISAHardware, metaclass=abc.ABCMeta):
    """ Base class for instrument relying upon a VISA communication layer that implement Standard Commands for
    Programmable Instruments (SCPI) commands. Note that not all SCPI instrument implement all SCPI commands. """
    _SCPI_DISPLAY_TIMEOUT = 1

    def __init__(self, *args, **kwargs):
        """ Constructs a SCPIHardware object. This is an abstract class and shouldn't be implemented directly.

        :param args: passed to parent
        :param kwargs: passed to parent
        """
        super().__init__(*args, **kwargs)

    # VISA implementations
    def _visa_transaction_error_check(self) -> typing.NoReturn:
        error_buffer = []

        # Attempt to get error
        error = self.get_scpi_error()

        # Keep fetching errors if available
        while error is not None:
            error_buffer.append(error)
            error = self.get_scpi_error()

        if len(error_buffer) == 1:
            raise SCPIHardwareError(f"SCPI error check returned {error_buffer[0]}")
        elif len(error_buffer) > 1:
            raise SCPIHardwareError(f"SCPI error check returned multiple errors: {', '.join(error_buffer)}")

    # Display handling
    @abc.abstractmethod
    def set_scpi_display(self, msg: typing.Optional[str] = None) -> typing.NoReturn:
        """

        :param msg: display message or None if display should be cleared
        :raises SCPIDisplayUnavailable: when no display is available or display is busy
        """
        pass

    # Error handling
    @abc.abstractmethod
    def get_scpi_error(self) -> TYPE_ERROR:
        """ Check for instrument generated error codes or response.

        A VISA transaction lock is acquired before calling this method.

        :return: a value or string detailing the error, None if no error has occurred
        """
        pass

    # SCPI commands
    def scpi_reset(self) -> typing.NoReturn:
        """ Issue SCPI instrument reset command. Should return instrument to power-up defaults. """
        self.visa_write('*RST')

    def scpi_clear(self) -> typing.NoReturn:
        """ Issue SCPI instrument clear status command. Clears event registers and error queue. """
        self.visa_write('*CLS')

    def scpi_get_event_status_enable(self) -> int:
        """ Query SCPI event status enable register.

        :return: integer containing current event status enable register value
        """
        return int(self.visa_query('*ESE?'))

    def scpi_get_event_status_opc(self) -> bool:
        """ Query SCPI operation complete status.

        :return: True when all pending operations are complete
        """
        return bool(self.visa_query('*OPC?'))

    def scpi_get_service_request(self) -> int:
        """ Query SCPI service request enable register.

        :return: integer containing current service request enable register value
        """
        return int(self.visa_query('*SRE?'))

    def scpi_get_status(self) -> int:
        """ Query SCPI status byte.

        :return: integer containing current status byte register value
        """
        return int(self.visa_query('*STB?'))

    def scpi_get_event_status(self) -> int:
        """

        :return:
        """
        return int(self.visa_query('*ESR?'))

    def scpi_get_identifier(self) -> str:
        """ Query SCPI instrument identification.

        Typically returns manufacturer, model number, serial number, firmware version and/or software revision.

        :return: string containing identification information
        """
        return self.visa_query('*IDN?')

    def scpi_get_options(self) -> str:
        """

        :return:
        """
        return self.visa_query('*OPT?')

    def scpi_set_event_status_enable(self, mask) -> typing.NoReturn:
        """

        :param mask:
        :return:
        """
        self.visa_write(f"*ESE {mask}")

    def scpi_set_service_request_enable(self, mask) -> typing.NoReturn:
        """

        :param mask:
        :return:
        """
        self.visa_write(f"*SRE {mask}")

    def scpi_set_event_status_opc(self):
        """ TODO

        :return:
        """
        self.visa_write('*OPC')

    def scpi_trigger(self) -> typing.NoReturn:
        """ TODO

        :return:
        """
        self.visa_write('*TRG')

    def scpi_wait(self) -> typing.NoReturn:
        """ Wait for pending commands to complete.

        Notes: from experience this command usually has mixed results. Behaviour is often defines by the status event
        register is some way, so consider making sure that is correct. Sometimes behaviour varies by protocol too, so
        USB commands may always return instantly even if their operation has not completed.
        """
        self.visa_write('*WAI')

    def handle_connect(self, event: typing.Optional[EventData] = None):
        super().handle_connect(event)

        # Clear any existing errors from queue
        self._logger.debug('Clearing existing errors from queue')

        try:
            self._visa_transaction_error_check()
        except HardwareError as exc:
            self._logger.warning(f"Existing instrument errors found: {exc}")

        # Log instrument identifier
        self._logger.info(f"SCPI ID: {self.scpi_get_identifier()}")

        # Reset instrument
        self.scpi_reset()

        try:
            with self.get_visa_transaction_lock(error_ignore=True):
                # Display identifier on display
                self.set_scpi_display(f"ID:{self.get_hardware_identifier()}")

                time.sleep(self._SCPI_DISPLAY_TIMEOUT)

                # Clear display
                self.set_scpi_display()
        except SCPIDisplayUnavailable:
            # If no display is available then skip
            self._logger.warning('Unable to display instrument identifier', event=False)

    def handle_disconnect(self, event: typing.Optional[EventData] = None):
        try:
            with self.get_visa_transaction_lock(error_ignore=True):
                # Display identifier on display
                self.set_scpi_display('Disconnected')
        except SCPIDisplayUnavailable:
            # If no display is available then skip
            self._logger.warning('Unable to display disconnect message', event=False)

        super().handle_disconnect(event)

    def handle_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        try:
            with self.get_visa_transaction_lock(error_ignore=True):
                # Display identifier on display
                self.set_scpi_display('Error')
        except (HardwareError, VISACommunicationError, SCPIDisplayUnavailable):
            # If no display is available then skip
            self._logger.warning('Unable to display error message', event=False)

        super().handle_error(event)
