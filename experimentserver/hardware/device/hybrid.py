import abc
import collections
import typing

from transitions import EventData

from experimentserver.data import to_timedelta, TYPE_TIME
from ..base.core import Hardware, TYPE_PARAMETER_DICT, TYPE_TAG_DICT, TYPE_HARDWARE
from ..error import ParameterError
from .hp import HP34401AMultimeter
from .keithley import MultimeterDAQ6510, Picoammeter6487
from ..metadata import TYPE_PARAMETER_COMMAND
from ...util import metadata as metadata

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class MultiChannelHardware(Hardware, metaclass=abc.ABCMeta):

    def __init__(self, identifier: str, daq_args: typing.Dict[str, typing.Any],
                 child_args: typing.Dict[str, typing.Any], parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 channel_delay: float = 1.0):
        super(MultiChannelHardware, self).__init__(identifier, parameters)

        # DAQ for channel switching
        self._daq_hardware = MultimeterDAQ6510(**daq_args)

        # Slave hardware
        self._child_hardware = self.get_child_class()(**child_args)

        # Channel configuration
        self._channel_list: typing.List[int] = []
        self._channel_metadata: typing.Dict[int, typing.Dict[str, str]] = collections.defaultdict(dict)
        self._channel_delay = to_timedelta(channel_delay)
        self._channel_repeat: int = 1

    @classmethod
    @abc.abstractmethod
    def get_child_class(cls) -> typing.Type[TYPE_HARDWARE]:
        pass

    @classmethod
    def get_hardware_class_description(cls) -> str:
        return cls.get_child_class().get_hardware_class_description() + ' [+DAQ]'

    def pre_channel_close(self, channel: int):
        pass

    def post_channel_close(self, channel: int):
        pass

    def pre_channel_open(self, channel: int):
        pass

    def post_channel_open(self, channel: int):
        pass

    # Parameters
    @Hardware.register_parameter(description='Add channel to scan list')
    def add_channel(self, channel: int):
        self._channel_list.append(int(channel))

    @Hardware.register_parameter(description='Append metadata to measurements from channel')
    def add_channel_metadata(self, channel: int, tag: str, value: str):
        self.get_logger().info(f"Add channel {channel} metadata: {tag} = {value}")
        self._channel_metadata[int(channel)][tag] = value

    @Hardware.register_parameter(description='Remove channel from scan list')
    def remove_channel(self, channel: int):
        if channel in self._channel_list:
            self._channel_list.remove(int(channel))

    @Hardware.register_parameter(description='Delay before measurement after channel switch')
    def set_channel_delay(self, t: TYPE_TIME):
        self._channel_delay = to_timedelta(t)

    @Hardware.register_parameter(description='Set channel repeat count')
    def set_channel_repeat(self, count: int):
        self._channel_repeat = int(count)

    def produce_measurement(self, extra_tags: typing.Optional[TYPE_TAG_DICT] = None) -> bool:
        extra_tags = extra_tags or {}

        measurement_flag = False

        with self._measurement_lock:
            with self._daq_hardware.visa_transaction() as multimeter_transaction:
                if len(self._channel_list) == 0:
                    # Turn off all relays
                    multimeter_transaction.write('ROUT:OPEN:ALL')

                    return False

                if len(self._channel_list) == 1:
                    # Close channel
                    multimeter_transaction.write("ROUT:CLOS (@{})", self._channel_list[0])

                    # Use original method
                    channel_tags = extra_tags.copy()

                    if self._channel_list[0] in self._channel_metadata:
                        channel_tags.update(self._channel_metadata[self._channel_list[0]])

                    channel_tags['channel'] = str(self._channel_list[0])

                    return self._child_hardware.produce_measurement(channel_tags.copy())

                # Turn off all relays
                multimeter_transaction.write('ROUT:OPEN:ALL')

                for channel in self._channel_list:
                    # Setup metadata
                    channel_tags = extra_tags.copy()

                    if channel in self._channel_metadata:
                        channel_tags.update(self._channel_metadata[channel])

                    channel_tags['channel'] = str(channel)

                    self.pre_channel_close(channel)

                    # Close channel
                    multimeter_transaction.write("ROUT:CLOS (@{})", channel)

                    self.post_channel_close(channel)

                    # Settling time
                    self.sleep(self._channel_delay.total_seconds(), 'multi-channel settling')

                    # Take measurement
                    for repeat in range(self._channel_repeat):
                        measurement_flag |= self._child_hardware.produce_measurement(channel_tags.copy())

                    self.pre_channel_open(channel)

                    # Open channel
                    multimeter_transaction.write("ROUT:OPEN (@{})", channel)

                    self.post_channel_open(channel)

        return measurement_flag

    # @HybridMethod
    # def get_hardware_measurement_metadata(self, include_force: bool = True) -> \
    #         typing.MutableMapping[str, _MeasurementMetadata]:
    #     if isinstance(self, MultiChannelHardware):
    #         measurement_metadata = self._child_hardware.get_hardware_measurement_metadata()
    #     else:
    #         measurement_metadata = self.get_child_class().get_hardware_measurement_metadata()
    #
    #     measurement_metadata.update(super().get_hardware_measurement_metadata(include_force))
    #
    #     return measurement_metadata
    #
    # @HybridMethod
    # def get_hardware_parameter_metadata(self) -> typing.MutableMapping[str, _ParameterMetadata]:
    #     if isinstance(self, MultiChannelHardware):
    #         parameter_metadata = self._child_hardware.get_hardware_parameter_metadata()
    #     else:
    #         parameter_metadata = self.get_child_class().get_hardware_parameter_metadata()
    #
    #     parameter_metadata.update(super(MultiChannelHardware, self).get_hardware_parameter_metadata())
    #
    #     return parameter_metadata

    def set_hardware_measurement(self, name: typing.Optional[str] = None) -> typing.NoReturn:
        self._child_hardware.set_hardware_measurement(name)

    def enable_hardware_measurement(self, name: str) -> typing.NoReturn:
        self._child_hardware.enable_hardware_measurement(name)

    def disable_hardware_measurement(self, name: str) -> typing.NoReturn:
        self._child_hardware.disable_hardware_measurement(name)

    def bind_parameter(self, parameter_command: TYPE_PARAMETER_COMMAND) -> typing.List[metadata.BoundMetadataCall]:
        # Attempt to bind to self, otherwise pass to child
        try:
            return super().bind_parameter(parameter_command)
        except ParameterError:
            return self._child_hardware.bind_parameter(parameter_command)

    def transition_start(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_start(event)

        self._daq_hardware.transition_start(event)
        self._child_hardware.transition_start(event)

    def transition_stop(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        try:
            self._daq_hardware.transition_stop(event)
        finally:
            self._child_hardware.transition_stop(event)

        super().transition_stop(event)

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(MultiChannelHardware, self).transition_connect(event)

        self._daq_hardware.transition_connect(event)
        self._child_hardware.transition_connect(event)

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        try:
            self._daq_hardware.transition_disconnect(event)
        finally:
            self._child_hardware.transition_disconnect(event)

        super(MultiChannelHardware, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(MultiChannelHardware, self).transition_configure(event)

        self._daq_hardware.transition_configure(event)
        self._child_hardware.transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        try:
            self._daq_hardware.transition_cleanup(event)
        finally:
            self._child_hardware.transition_cleanup(event)

        super(MultiChannelHardware, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(MultiChannelHardware, self).transition_error(event)

        try:
            self._daq_hardware.transition_error(event)
        finally:
            self._child_hardware.transition_error(event)

    def transition_reset(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super().transition_reset(event)

        try:
            self._daq_hardware.transition_reset(event)
        finally:
            self._child_hardware.transition_reset(event)


class HP34401AMultimeterMultiChannel(MultiChannelHardware):
    def __init__(self, identifier: str, daq_args: typing.Dict[str, typing.Any],
                 child_args: typing.Dict[str, typing.Any], parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 channel_delay: float = 1.0):
        super(HP34401AMultimeterMultiChannel, self).__init__(identifier, daq_args, child_args, parameters,
                                                             channel_delay)

    @classmethod
    def get_child_class(cls) -> typing.Type:
        return HP34401AMultimeter


class Picoammeter6487MultiChannel(MultiChannelHardware):
    def __init__(self, identifier: str, daq_args: typing.Dict[str, typing.Any],
                 child_args: typing.Dict[str, typing.Any], parameters: typing.Optional[TYPE_PARAMETER_DICT] = None,
                 channel_delay: float = 0.0):
        super(Picoammeter6487MultiChannel, self).__init__(identifier, daq_args, child_args, parameters, channel_delay)

    @classmethod
    def get_child_class(cls) -> typing.Type:
        return Picoammeter6487

    def post_channel_close(self, channel: int):
        super().post_channel_close(channel)

        # Enable supply
        self._child_hardware.set_source_enable(True)

    def pre_channel_open(self, channel: int):
        super().pre_channel_open(channel)

        # Disable supply
        self._child_hardware.set_source_enable(False)
