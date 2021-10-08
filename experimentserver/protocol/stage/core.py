import typing
from datetime import datetime, timedelta

from . import BaseStage
from ...config import ConfigManager
from ...data import TYPE_TAG_DICT, TYPE_TIME, Measurement, to_timedelta
from ...util.constant import FORMAT_TIMESTAMP


class Delay(BaseStage):
    def __init__(self, config: ConfigManager, interval: TYPE_TIME, uid: typing.Optional[str] = None,
                 metadata: typing.Optional[TYPE_TAG_DICT] = None, sync_minute: bool = False):
        # Convert interval
        self._delay_interval = to_timedelta(interval)
        self._delay_sync_minute = sync_minute

        metadata = metadata or {}

        metadata.update({
            'delay_interval': str(self._delay_interval.total_seconds())
        })

        if sync_minute:
            metadata['delay_sync'] = 'true'

        super(Delay, self).__init__(config, uid, metadata=metadata)

        # Additional timestamp
        self._delay_exit_timestamp: typing.Optional[datetime] = None

    def get_stage_duration(self) -> typing.Optional[timedelta]:
        return self._delay_interval

    def get_stage_remaining(self) -> typing.Optional[timedelta]:
        if self._delay_exit_timestamp is None:
            return self.get_stage_duration()

        duration = self._delay_exit_timestamp - datetime.now()

        if duration.total_seconds() > 0:
            return duration
        else:
            return timedelta()

    def get_stage_summary(self) -> typing.MutableSequence[typing.Union[str, typing.MutableSequence[str]]]:
        summary = super(Delay, self).get_stage_summary()

        summary.append(f"Delay for {self._delay_interval} ({self._delay_interval.total_seconds()} seconds)")

        return summary

    def stage_enter(self) -> typing.NoReturn:
        super(Delay, self).stage_enter()

        # Calculate exit timestamp
        self._delay_exit_timestamp = self._stage_enter_timestamp + self._delay_interval

        self._sync_delay_exit_timestamp()

        self.get_logger().info(f"Delay for {self._delay_interval} until "
                               f"{self._delay_exit_timestamp.strftime(FORMAT_TIMESTAMP)}")

        # Add tag
        Measurement.add_global_tag('delay_interval', self._delay_interval.total_seconds())

    def stage_run(self) -> bool:
        # Test for stage completion
        return datetime.now() < self._delay_exit_timestamp

    def stage_resume(self) -> typing.NoReturn:
        # Update resume timestamp
        self._sync_delay_exit_timestamp(datetime.now() - self._stage_pause_timestamp)

        super(Delay, self).stage_resume()

    def stage_exit(self) -> typing.NoReturn:
        # Clear timestamp
        self._delay_exit_timestamp = None

        super(Delay, self).stage_exit()

    def stage_export(self) -> typing.Dict[str, typing.Any]:
        stage = super(Delay, self).stage_export()

        stage.update({
            'interval': self._delay_interval.total_seconds(),
        })

        if self._delay_sync_minute:
            stage['sync_minute'] = self._delay_sync_minute

        return stage

    def _sync_delay_exit_timestamp(self, offset: typing.Optional[timedelta] = None):
        if offset is not None:
            self._delay_exit_timestamp += offset

        if self._delay_sync_minute:
            # Delay to next full minute
            self._delay_exit_timestamp += timedelta(minutes=1)
            self._delay_exit_timestamp -= timedelta(seconds=self._delay_exit_timestamp.second,
                                                    microseconds=self._delay_exit_timestamp.microsecond)


class Pause(BaseStage):
    def __init__(self, config: ConfigManager, uid: typing.Optional[str] = None,
                 metadata: typing.Optional[TYPE_TAG_DICT] = None):
        super(Pause, self).__init__(config, uid, metadata=metadata)

    def stage_enter(self) -> typing.NoReturn:
        super().stage_enter()

        self.get_logger().info('Reached pause in procedure', notify=True)

    def get_stage_duration(self) -> typing.Optional[timedelta]:
        return None

    def stage_run(self) -> bool:
        # Always waiting
        return True

    def stage_export(self) -> typing.Dict[str, typing.Any]:
        return super(Pause, self).stage_export()


class Setup(BaseStage):
    """ Stage that only sets up hardware. Has no duration. """

    def __init__(self, config: ConfigManager, uid: typing.Optional[str] = None,
                 parameters: typing.Optional[typing.Dict[str, typing.Any]] = None):
        super(Setup, self).__init__(config, uid, parameters, has_duration=False)

    def stage_run(self) -> bool:
        # Always finished
        return False

    def stage_export(self) -> typing.Dict[str, typing.Any]:
        return super(Setup, self).stage_export()


class Notify(Setup):
    def __init__(self, config: ConfigManager, message: str = 'Ping', uid: typing.Optional[str] = None):
        super(Notify, self).__init__(config, uid)

        self._message = message

    def stage_enter(self) -> typing.NoReturn:
        super().stage_enter()

        self.get_logger().info(self._message, notify=True)


