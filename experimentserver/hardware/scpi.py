import abc
import contextlib
import time
import typing

import pyvisa.errors
from transitions import EventData

from experimentserver.util.module import get_call_context
from .visa import VISAHardware, VISAException


class SCPIException(VISAException):
    """ Specific subset of VISA exceptions for SCPI instruments. """
    pass


class SCPIDisplayException(SCPIException):
    """ Error occurred during update of instrument display. """
    pass


class SCPIHardware(VISAHardware, metaclass=abc.ABCMeta):
    """ Base class for instrument relying upon a VISA communication layer that implement Standard Commands for
    Programmable Instruments (SCPI) commands. Note that not all SCPI instrument implement all SCPI commands. """
    _SCPI_IDENTIFIER_DISPLAY_TIMEOUT = 1

    def __init__(self, *args, **kwargs):
        """ Constructs a SCPIHardware object. This is an abstract class and shouldn't be implemented directly.

        :param args: passed to parent
        :param kwargs: passed to parent
        """
        super().__init__(*args, **kwargs)

    # Error handling
    @abc.abstractmethod
    def _get_error(self) -> typing.Union[None, str, typing.Tuple[int, str]]:
        """ Check for instrument generated error codes or response.

        A hardware lock is acquired before calling this method.

        :return: a value or string detailing the error, None if no error has occurred
        """
        pass

    def _raise_on_error(self):
        """ Raise an SCPIException if onr or multiple errors are present in the hardware buffer.

        :raise SCPIException: when an instrument error has occurred
        """
        error_buffer = []

        # Request error message
        with self.get_hardware_lock(False):
            error = self._get_error()

            while error is not None:
                error_buffer.append(error)

                # Try and get another error
                error = self._get_error()

        if len(error_buffer) == 1:
            raise SCPIException(f"Instrument error: {error_buffer[0]!s} (last command: {self._visa_last_command})")
        elif len(error_buffer) > 1:
            raise SCPIException(f"Multiple instrument errors: {', '.join( map(str, error_buffer))} (last command: "
                                f"{self._visa_last_command})")

        self._logger.debug('No SCIP errors')

    # SCPI wrapper
    @contextlib.contextmanager
    def get_hardware_lock(self, error_check: bool = True):
        """

        :return:
        """
        context = ', '.join(get_call_context(2))

        with super().get_hardware_lock():
            try:
                yield
            finally:
                if error_check:
                    if self._visa_resource is not None:
                        self._logger.debug(f"SCIP error check (context: {context})")
                        self._raise_on_error()

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

    def scpi_set_event_status_enable(self, mask):
        """

        :param mask:
        :return:
        """
        self.visa_write(f"*ESE {mask}")

    def scpi_set_service_request_enable(self, mask):
        """

        :param mask:
        :return:
        """
        self.visa_write(f"*SRE {mask}")

    def scpi_set_event_status_opc(self):
        """

        :return:
        """
        self.visa_write('*OPC')

    def scpi_trigger(self):
        """

        :return:
        """
        self.visa_write('*TRG')

    def scpi_wait(self):
        """

        :return:
        """
        self.visa_write('*WAI')

    # Display
    @abc.abstractmethod
    def display_msg(self, msg: str = None) -> typing.NoReturn:
        """ Display a specified message on the display of this hardware.

        :param msg: string message to display, None if display should be cleared
        :raises SCPIDisplayException: when an error occurs during display or no display is available
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def get_hardware_description() -> str:
        pass

    @abc.abstractmethod
    def handle_setup(self, event: EventData):
        super().handle_setup(event)

        # Log instrument identifier
        try:
            self._logger.info(f"SCPI ID: {self.scpi_get_identifier()}")
        except pyvisa.errors.VisaIOError as exc:
            raise SCPIException('Could not retrieve identifier from instrument. Check power and data connections. '
                                'Verify that instrument VISA address is correct.') from exc

        # Clear any existing errors from queue
        try:
            self._raise_on_error()
        except SCPIException as exc:
            self._logger.warning(f"Existing instrument errors found: {exc!s}")

        # Reset instrument
        self.scpi_reset()

        try:
            with self.get_hardware_lock(False):
                # Display identifier on display
                self.display_msg(f"ID:{self.get_hardware_identifier()}")

                time.sleep(self._SCPI_IDENTIFIER_DISPLAY_TIMEOUT)

                # Clear display
                self.display_msg()
        except SCPIDisplayException:
            # If no display is available then skip
            self._logger.warning('Unable to display instrument identifier')

    @abc.abstractmethod
    def handle_start(self, event: EventData):
        super().handle_start(event)

    @abc.abstractmethod
    def handle_pause(self, event: EventData):
        super().handle_pause(event)

    @abc.abstractmethod
    def handle_resume(self, event: EventData):
        super().handle_resume(event)

    @abc.abstractmethod
    def handle_stop(self, event: EventData):
        super().handle_stop(event)

    @abc.abstractmethod
    def handle_cleanup(self, event: EventData):
        try:
            with self.get_hardware_lock(False):
                self.display_msg('Disconnect')

                time.sleep(self._SCPI_IDENTIFIER_DISPLAY_TIMEOUT)

                # Clear display
                self.display_msg()
        except SCPIDisplayException:
            # If no display is available then skip
            self._logger.warning('Unable to display disconnect message')

        super().handle_cleanup(event)

    @abc.abstractmethod
    def handle_error(self, event: EventData):
        super().handle_error(event)

        if self._visa_resource is not None:
            try:
                self.display_msg('Error')
            except VISAException:
                self._logger.warning(f"Error occurred while attempting to display error message")
