import abc
import collections
import typing
from datetime import timedelta

from experimentlib.data.unit import T_PARSE_TIMEDELTA, parse_timedelta
from experimentlib.util.time import now
from transitions import EventData

from .hp import HP34401AMultimeter
from .keithley import MultimeterDAQ6510, Picoammeter6487
from ..base.core import Hardware, TYPE_PARAMETER_DICT, TYPE_HARDWARE
from ..error import ParameterError
from ..metadata import TYPE_PARAMETER_COMMAND
from ...util import metadata as metadata
from experimentserver.measurement import T_DYNAMIC_FIELD_MAP, T_TAG_MAP

__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class MultiChannelHardware(Hardware, metaclass=abc.ABCMeta):
    SWITCH_DELAY = 0.01

    def __init__(self, identifier: str, daq_args: typing.Dict[str, typing.Any],
                 child_args: typing.Dict[str, typing.Any], parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(MultiChannelHardware, self).__init__(identifier, parameters)

        # DAQ for channel switching
        self._daq_hardware = MultimeterDAQ6510(**daq_args)

        # Slave hardware
        self._child_hardware = self.get_child_class()(**child_args)

        # Channel configuration
        self._channel_list: typing.List[int] = []
        self._channel_metadata: typing.MutableMapping[int, typing.MutableMapping[str, typing.Any]] \
            = collections.defaultdict(dict)
        self._channel_duration: typing.Optional[timedelta] = None
        self._channel_repeat: int = 1

        # Post connect delay
        self._post_channel_delay: typing.Optional[timedelta] = None

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
        # Switch open time
        self.sleep(self.SWITCH_DELAY, 'contact close time')

        self.sleep(self._post_channel_delay, 'channel delay')

    def pre_channel_open(self, channel: int):
        pass

    # noinspection PyUnusedLocal
    def post_channel_open(self, channel: int):
        # Switch open time
        self.sleep(self.SWITCH_DELAY, 'contact open time')

    def get_channel_metadata(self, channel: int) -> typing.Dict[str, typing.Any]:
        channel_tags = {
            'switch_channel': channel
        }

        if self._channel_repeat > 1:
            channel_tags['switch_repeat_total'] = self._channel_repeat

        if self._channel_duration is not None:
            channel_tags['switch_duration'] = self._channel_duration.total_seconds()

        if channel in self._channel_metadata:
            channel_tags.update(self._channel_metadata[channel])

        return channel_tags

    def _daq_open_all(self):
        with self._daq_hardware.visa_transaction() as daq_transaction:
            # Turn off all relays
            daq_transaction.write('ROUT:OPEN:ALL')

        # Switch open time
        self.sleep(self.SWITCH_DELAY, 'contact open time')

    # Parameters
    @Hardware.register_parameter(description='Add channel to scan list')
    def add_channel(self, channel: typing.Any):
        channel = int(channel)

        if channel not in self._channel_list:
            with self._measurement_lock:
                self._channel_list.append(channel)

        self.logger().info(f"Added channel {channel}")

    @Hardware.register_parameter(description='Append metadata to measurements from channel')
    def add_channel_metadata(self, channel: typing.Any, tag: str, value: typing.Any):
        channel = int(channel)
        tag = str(tag)
        value = str(value)

        self._channel_metadata[channel][tag] = value
        self.logger().info(f"Added channel {channel} metadata: {tag} = {value}")

    @Hardware.register_parameter(description='Remove channel from scan list')
    def remove_channel(self, channel: typing.Any):
        channel = int(channel)

        if channel in self._channel_list:
            with self._measurement_lock:
                self._channel_list.remove(channel)

                # Force all relays open
                self._daq_open_all()

            self.logger().info(f"Removed channel {channel}")
        else:
            self.logger().warning(f"Channel {channel} not in scan list")

    @Hardware.register_parameter(description='Set scan list to single channel')
    def set_channel(self, channel: typing.Any):
        channel = int(channel)

        with self._measurement_lock:
            if channel == 0:
                self._channel_list = []
            else:
                self._channel_list = [channel]

            # Force all relays open
            self._daq_open_all()

        self.logger().info(f"Selecting channel {channel}")

    @Hardware.register_parameter(description='Time to make measurements from selected channel')
    def set_channel_duration(self, duration: T_PARSE_TIMEDELTA):
        duration = parse_timedelta(duration)

        with self._measurement_lock:
            if duration.total_seconds() > 0:
                self._channel_duration = duration
            else:
                self._channel_duration = None

        self.logger().info(f"Set channel duration to {self.set_channel_duration}")

    @Hardware.register_parameter(description='Set channel repeat reading count')
    def set_channel_repeat(self, count: typing.Any):
        count = int(count)

        with self._measurement_lock:
            self._channel_repeat = count

        self.logger().info(f"Set channel repeat to {self._channel_repeat}")

    @Hardware.register_parameter(description='Time to make measurements from selected channel')
    def set_channel_duration(self, duration: T_PARSE_TIMEDELTA):
        duration = parse_timedelta(duration)

        with self._measurement_lock:
            if duration.total_seconds() > 0:
                self._channel_duration = duration
            else:
                self._channel_duration = None

        self.logger().info(f"Set channel duration to {self.set_channel_duration}")

    @Hardware.register_parameter(description='Delay before commencing measurements after channel switch')
    def set_post_channel_delay(self, delay: T_PARSE_TIMEDELTA):
        delay = parse_timedelta(delay)

        with self._measurement_lock:
            if delay.total_seconds() > 0:
                self._post_channel_delay = delay
            else:
                self._post_channel_delay = None

        self.logger().info(f"Set post-channel switch delay to {self._post_channel_delay}")

    def produce_measurement(self, extra_dynamic_fields: typing.Optional[T_DYNAMIC_FIELD_MAP] = None,
                            extra_tags: typing.Optional[T_TAG_MAP] = None) -> bool:
        extra_tags = extra_tags or {}

        measurement_flag = False

        with self._measurement_lock:
            # No channels selected, just return
            if len(self._channel_list) == 0:
                self.logger().debug('No channels')
                return False

            with self._daq_hardware.visa_transaction() as daq_transaction:
                if len(self._channel_list) == 1:
                    self.logger().debug('Single channel')

                    # Close channel
                    daq_transaction.write("ROUT:CLOS (@{})", self._channel_list[0])

                    # Use child method
                    return self._child_hardware.produce_measurement(extra_dynamic_fields,
                                                                    self.get_channel_metadata(self._channel_list[0]))

                self.logger().debug(f"Channels: {', '.join(map(str, self._channel_list))}")

                for channel in self._channel_list:
                    channel_repeat = 0

                    # Setup metadata
                    channel_tags = extra_tags.copy()
                    channel_tags.update(self.get_channel_metadata(channel))

                    self.pre_channel_close(channel)

                    # Close channel
                    daq_transaction.write("ROUT:CLOS (@{})", channel)
                    self.logger().debug(f"Channel {channel} closed")

                    # Start timing
                    channel_time = now()

                    self.post_channel_close(channel)

                    # Take measurement
                    channel_active = True

                    while channel_active:
                        channel_repeat += 1

                        measurement_metadata = channel_tags.copy()
                        measurement_metadata['channel_repeat'] = str()

                        self.logger().debug(f"Channel {channel} metadata {measurement_metadata!r}")

                        measurement_flag |= self._child_hardware.produce_measurement(extra_dynamic_fields,
                                                                                     measurement_metadata)

                        # Stop when enough measurements made
                        if self._channel_duration is not None:
                            if (now() - channel_time) > self._channel_duration:
                                self.logger().trace('Enough duration')
                                channel_active = False
                        elif self._channel_repeat > 1:
                            if channel_repeat > self._channel_repeat:
                                self.logger().trace('Enough repeats')
                                channel_active = False
                        else:
                            # No stopping condition, single measurement
                            channel_active = False

                    self.pre_channel_open(channel)

                    # Open channel
                    daq_transaction.write("ROUT:OPEN (@{})", channel)
                    self.logger().debug(f"Channel {channel} opened")

                    self.post_channel_open(channel)

        return measurement_flag

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
                 child_args: typing.Dict[str, typing.Any], parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(HP34401AMultimeterMultiChannel, self).__init__(identifier, daq_args, child_args, parameters)

    @classmethod
    def get_child_class(cls) -> typing.Type:
        return HP34401AMultimeter


class Picoammeter6487MultiChannel(MultiChannelHardware):
    def __init__(self, identifier: str, daq_args: typing.Dict[str, typing.Any],
                 child_args: typing.Dict[str, typing.Any], parameters: typing.Optional[TYPE_PARAMETER_DICT] = None):
        super(Picoammeter6487MultiChannel, self).__init__(identifier, daq_args, child_args, parameters)

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
