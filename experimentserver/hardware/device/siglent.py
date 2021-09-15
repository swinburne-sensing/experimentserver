import time
import typing

from transitions import EventData

from ..base.scpi import SCPIHardware, SCPIDisplayUnavailable
from ..base.visa import VISAHardware, TYPE_ERROR
from ...data import Measurement, MeasurementGroup, to_unit
from ..error import CommunicationError


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class SDGWaveformGenerator(SCPIHardware):
    def __init__(self, *args, external_reference: bool = True, **kwargs):
        super(SDGWaveformGenerator, self).__init__(*args, **kwargs)

        self._external_reference = external_reference

    @SCPIHardware.register_measurement(description='Frequency counter measurement', force=True)
    def get_frequency_counter(self) -> Measurement:
        """

        :return:
        """
        # No way to wait for a measurement, so just delay
        self.sleep(1, 'trigger delay')

        with self.visa_transaction() as transaction:
            result_str = transaction.query('FCNT?')

        # Parse result
        result = result_str.split(' ')

        if len(result) != 2:
            raise CommunicationError(f"Response did not match expected format, expected space: {result_str}")

        result = result[1].split(',')

        if len(result) % 2 != 0:
            raise CommunicationError(f"Response did not match expected format, expected even fields: {result_str}")

        # Convert into a dict
        fields = dict(zip(result[::2], result[1::2]))

        values = {
            'frequency': to_unit(fields['FRQ'].lower().replace('hz', 'Hz'), 'Hz'),
            'duty': to_unit(fields['DUTY'], 'percent'),
            'width_positive': to_unit(fields['PW'].lower(), 's'),
            'width_negative': to_unit(fields['NW'].lower(), 's')
        }

        tags = {
            'coupling': fields['MODE'].lower()
        }

        return Measurement(self, MeasurementGroup.FREQUENCY, values, tags=tags)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_configure(event)

        with self.visa_transaction() as transaction:
            transaction.write('FCNT STATE,ON')

            if self._external_reference:
                transaction.write('ROSC EXT')

    @classmethod
    def scpi_display(cls, transaction: VISAHardware.VISATransaction,
                     msg: typing.Optional[str] = None) -> typing.NoReturn:
        # No custom display commands
        raise SCPIDisplayUnavailable()

    @classmethod
    def _get_visa_error(cls, transaction: VISAHardware.VISATransaction) -> typing.Optional[TYPE_ERROR]:
        # No error checking capability
        return None

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'Siglent SDG-series arbitrary waveform generator'
