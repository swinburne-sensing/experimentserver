import abc
import time
import typing

from transitions import EventData

import experimentserver
from .visa import VISAHardware, VISACommunicationError, VISAExternalError


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

    # Display handling
    @classmethod
    @abc.abstractmethod
    def scpi_display(cls, transaction: VISAHardware._VISATransaction,
                     msg: typing.Optional[str] = None) -> typing.NoReturn:
        """

        :param transaction:
        :param msg: display message or None if display should be cleared
        :raises SCPIDisplayUnavailable: when no display is available or display is busy
        """
        pass

    # SCPI commands
    @staticmethod
    def scpi_reset(transaction: typing.Optional[VISAHardware._VISATransaction] = None) -> typing.NoReturn:
        """ Issue SCPI instrument reset command. Should return instrument to power-up defaults.

        :param transaction:
        """
        transaction.write('*RST')

    @staticmethod
    def scpi_clear(transaction: VISAHardware._VISATransaction) -> typing.NoReturn:
        """ Issue SCPI instrument clear status command. Clears event registers and error queue.

        :param transaction:
        """
        transaction.write('*CLS')

    @staticmethod
    def scpi_get_event_status_enable(transaction: VISAHardware._VISATransaction) -> int:
        """ Query SCPI event status enable register.

        :param transaction:
        :return: integer containing current event status enable register value
        """
        return int(transaction.query('*ESE?'))

    @staticmethod
    def scpi_get_event_status_opc(transaction: VISAHardware._VISATransaction) -> bool:
        """ Query SCPI operation complete status.

        :param transaction:
        :return: True when all pending operations are complete
        """
        return bool(transaction.query('*OPC?'))

    @staticmethod
    def scpi_get_service_request(transaction: VISAHardware._VISATransaction) -> int:
        """ Query SCPI service request enable register.

        :param transaction:
        :return: integer containing current service request enable register value
        """
        return int(transaction.query('*SRE?'))

    @staticmethod
    def scpi_get_status(transaction: VISAHardware._VISATransaction) -> int:
        """ Query SCPI status byte.

        :param transaction:
        :return: integer containing current status byte register value
        """
        return int(transaction.query('*STB?'))

    @staticmethod
    def scpi_get_event_status(transaction: VISAHardware._VISATransaction) -> int:
        """

        :param transaction:
        :return:
        """
        return int(transaction.query('*ESR?'))

    @staticmethod
    def scpi_get_identifier(transaction: VISAHardware._VISATransaction) -> str:
        """ Query SCPI instrument identification.

        Typically returns manufacturer, model number, serial number, firmware version and/or software revision.

        :param transaction:
        :return: string containing identification information
        """
        return transaction.query('*IDN?')

    @staticmethod
    def scpi_get_options(transaction: VISAHardware._VISATransaction) -> str:
        """

        :param transaction:
        :return:
        """
        return transaction.query('*OPT?')

    @staticmethod
    def scpi_set_event_status_enable(transaction: VISAHardware._VISATransaction, mask) -> typing.NoReturn:
        """

        :param transaction:
        :param mask:
        :return:
        """
        transaction.write(f"*ESE {mask}")

    @staticmethod
    def scpi_set_service_request_enable(transaction: VISAHardware._VISATransaction, mask) -> typing.NoReturn:
        """

        :param transaction:
        :param mask:
        :return:
        """
        transaction.write(f"*SRE {mask}")

    @staticmethod
    def scpi_set_event_status_opc(transaction: VISAHardware._VISATransaction):
        """ TODO

        :param transaction:
        :return:
        """
        transaction.write('*OPC')

    @staticmethod
    def scpi_trigger(transaction: VISAHardware._VISATransaction) -> typing.NoReturn:
        """ TODO

        :param transaction:
        :return:
        """
        transaction.write('*TRG')

    @staticmethod
    def scpi_wait(transaction: VISAHardware._VISATransaction) -> typing.NoReturn:
        """ Wait for pending commands to complete.

        Notes: from experience this command usually has mixed results. Behaviour is often defines by the status event
        register is some way, so consider making sure that is correct. Sometimes behaviour varies by protocol too, so
        USB commands may always return instantly even if their operation has not completed.

        :param transaction:
        """
        transaction.write('*WAI')

    def transition_connect(self, event: typing.Optional[EventData] = None):
        super().transition_connect(event)

        with self.visa_transaction() as transaction:
            # Reset instrument
            self.scpi_reset(transaction)

            # Log instrument identifier
            self.get_logger().info(f"SCPI ID: {self.scpi_get_identifier(transaction)}")

        with self.visa_transaction(error_raise=False) as transaction:
            try:
                # Show identifier on display
                self.scpi_display(transaction, f"ID:{self.get_hardware_identifier()}")

                time.sleep(self._SCPI_DISPLAY_TIMEOUT)

                # Clear display
                self.scpi_display(transaction)
            except SCPIDisplayUnavailable:
                # If no display is available then skip
                self.get_logger().warning('Unable to display instrument identifier', event=False)

    def transition_disconnect(self, event: typing.Optional[EventData] = None):
        with self.visa_transaction(error_raise=False) as transaction:
            try:
                # Show disconnected message on display
                self.scpi_display(transaction, 'Disconnected')
            except SCPIDisplayUnavailable:
                # If no display is available then skip
                self.get_logger().warning('Unable to display disconnect message', event=False)

        super().transition_disconnect(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        with self.visa_transaction(error_raise=False) as transaction:
            try:
                self.scpi_display(transaction, 'Error')
            except (VISAExternalError, VISACommunicationError, SCPIDisplayUnavailable):
                # If no display is available then skip
                self.get_logger().warning('Unable to display error message', event=False)

        super().transition_error(event)
