import abc
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

    def __init__(self, *args: typing.Any, **kwargs: typing.Any):
        """ Constructs a SCPIHardware object. This is an abstract class and shouldn't be implemented directly.

        :param args: passed to parent
        :param kwargs: passed to parent
        """
        super().__init__(*args, **kwargs)

    # Display handling
    @classmethod
    @abc.abstractmethod
    def scpi_display(cls, transaction: VISAHardware.VISATransaction,
                     msg: typing.Optional[str] = None) -> None:
        """

        :param transaction:
        :param msg: display message or None if display should be cleared
        :raises SCPIDisplayUnavailable: when no display is available or display is busy
        """
        pass

    # SCPI commands
    @staticmethod
    def scpi_reset(transaction: VISAHardware.VISATransaction) -> None:
        """ Issue SCPI instrument reset command. Should return instrument to power-up defaults.

        :param transaction:
        """
        transaction.write('*RST')

    def _scpi_reset_pre(self, transaction: VISAHardware.VISATransaction) -> None:
        """

        :param transaction:
        :return:
        """
        pass

    def _scpi_reset_post(self, transaction: VISAHardware.VISATransaction) -> None:
        """

        :param transaction:
        :return:
        """
        pass

    @staticmethod
    def scpi_clear(transaction: VISAHardware.VISATransaction) -> None:
        """ Issue SCPI instrument clear status command. Clears event registers and error queue.

        :param transaction:
        """
        transaction.write('*CLS')

    @staticmethod
    def scpi_get_event_status_enable(transaction: VISAHardware.VISATransaction) -> int:
        """ Query SCPI event status enable register.

        :param transaction:
        :return: integer containing current event status enable register value
        """
        return int(transaction.query('*ESE?'))

    @staticmethod
    def scpi_get_event_status_opc(transaction: VISAHardware.VISATransaction) -> bool:
        """ Query SCPI operation complete status.

        :param transaction:
        :return: True when all pending operations are complete
        """
        return bool(transaction.query('*OPC?'))

    @staticmethod
    def scpi_get_service_request(transaction: VISAHardware.VISATransaction) -> int:
        """ Query SCPI service request enable register.

        :param transaction:
        :return: integer containing current service request enable register value
        """
        return int(transaction.query('*SRE?'))

    @staticmethod
    def scpi_get_status(transaction: VISAHardware.VISATransaction) -> int:
        """ Query SCPI status byte.

        :param transaction:
        :return: integer containing current status byte register value
        """
        return int(transaction.query('*STB?'))

    @staticmethod
    def scpi_get_event_status(transaction: VISAHardware.VISATransaction) -> int:
        """

        :param transaction:
        :return:
        """
        return int(transaction.query('*ESR?'))

    @staticmethod
    def scpi_get_identifier(transaction: VISAHardware.VISATransaction) -> str:
        """ Query SCPI instrument identification.

        Typically returns manufacturer, model number, serial number, firmware version and/or software revision.

        :param transaction:
        :return: string containing identification information
        """
        return transaction.query('*IDN?')

    @staticmethod
    def scpi_get_options(transaction: VISAHardware.VISATransaction) -> str:
        """

        :param transaction:
        :return:
        """
        return transaction.query('*OPT?')

    @staticmethod
    def scpi_set_event_status_enable(transaction: VISAHardware.VISATransaction, mask: int) -> None:
        """

        :param transaction:
        :param mask:
        :return:
        """
        transaction.write(f"*ESE {mask}")

    @staticmethod
    def scpi_set_service_request_enable(transaction: VISAHardware.VISATransaction, mask: int) -> None:
        """

        :param transaction:
        :param mask:
        :return:
        """
        transaction.write(f"*SRE {mask}")

    @staticmethod
    def scpi_set_event_status_opc(transaction: VISAHardware.VISATransaction) -> None:
        """ TODO

        :param transaction:
        :return:
        """
        transaction.write('*OPC')

    @staticmethod
    def scpi_trigger(transaction: VISAHardware.VISATransaction) -> None:
        """ TODO

        :param transaction:
        :return:
        """
        transaction.write('*TRG')

    @staticmethod
    def scpi_wait(transaction: VISAHardware.VISATransaction) -> None:
        """ Wait for pending commands to complete.

        Notes: from experience this command usually has mixed results. Behaviour is often defines by the status event
        register is some way, so consider making sure that is correct. Sometimes behaviour varies by protocol too, so
        USB commands may always return instantly even if their operation has not completed.

        :param transaction:
        """
        transaction.write('*WAI')

    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super().transition_connect(event)

        with self.visa_transaction() as transaction:
            self._scpi_reset_pre(transaction)

            # Reset instrument
            self.scpi_reset(transaction)

            # Run post reset commands
            self._scpi_reset_post(transaction)

            # Log instrument identifier
            self.logger().info(f"SCPI ID: {self.scpi_get_identifier(transaction)}")

        with self.visa_transaction(error_raise=False) as transaction:
            try:
                # Show identifier on display
                self.scpi_display(transaction, f"ID:{self.get_hardware_identifier()}")

                self.sleep(self._SCPI_DISPLAY_TIMEOUT, 'display delay')

                # Clear display
                self.scpi_display(transaction)
            except SCPIDisplayUnavailable:
                # If no display is available then skip
                self.logger().warning('Unable to display instrument identifier')

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        with self.visa_transaction(error_check=False) as transaction:
            try:
                # Show disconnected message on display
                self.scpi_display(transaction, 'Disconnected')
            except SCPIDisplayUnavailable:
                # If no display is available then skip
                self.logger().warning('Unable to display disconnect message')

        super().transition_disconnect(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        with self.visa_transaction(error_raise=False) as transaction:
            try:
                self.scpi_display(transaction, 'Error')
            except (VISAExternalError, VISACommunicationError, SCPIDisplayUnavailable):
                # If no display is available then skip
                self.logger().warning('Unable to display error message')

        super().transition_error(event)
