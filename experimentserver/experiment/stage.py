import typing
from datetime import datetime, timedelta

from . import BaseStage
from ..data import TYPE_TIME, to_timedelta


class DelayStage(BaseStage):
    def __init__(self, delay: TYPE_TIME, sync_minute: bool = False):
        super(DelayStage, self).__init__()

        # Convert input to timedelta
        self._delay = to_timedelta(delay)
        self._sync_minute = sync_minute

        # Pause must be stored to offset delay on resume
        self._stage_pause_time: typing.Optional[datetime] = None

        # Calculated stage exit time
        self._stage_exit_time: typing.Optional[datetime] = None

    def _calc_stage_exit_time(self, offset: typing.Optional[timedelta] = None):
        if offset is not None:
            self._stage_exit_time += offset

        if self._sync_minute:
            # Delay to next full minute
            self._stage_exit_time += timedelta(minutes=1)
            self._stage_exit_time -= timedelta(seconds=self._stage_exit_time.second,
                                               microseconds=self._stage_exit_time.microsecond)

    def start(self) -> typing.NoReturn:
        super(DelayStage, self).start()

        # Calculate stage exit time
        self._stage_exit_time = datetime.now() + self._delay
        self._calc_stage_exit_time()

    def pause(self) -> typing.NoReturn:
        super(DelayStage, self).pause()

        # Store pause time
        self._stage_pause_time = datetime.now()

    def resume(self) -> typing.NoReturn:
        # Calculate paused time
        self._calc_stage_exit_time(datetime.now() - self._stage_pause_time)

        self.get_logger().info(f"Estimated completion at {self._stage_exit_time.strftime('%Y-%m-%d %H:%M:%S')}")

        super(DelayStage, self).resume()

    def tick(self) -> bool:
        return datetime.now() >= self._stage_exit_time

    def stop(self) -> typing.NoReturn:
        # Clear exit time
        self._stage_exit_time = None

        super(DelayStage, self).stop()

    def validate(self) -> typing.NoReturn:
        super(DelayStage, self).validate()

    def get_duration(self) -> timedelta:
        if self._sync_minute:
            # Possible delay of up to one extra minute
            return self._delay + timedelta(minutes=1)
        else:
            return self._delay

    def get_status(self) -> str:
        return f"Completion in {self._stage_exit_time - datetime.now()!s} at " \
               f"{self._stage_exit_time.strftime('%Y-%m-%d %H:%M:%S')}"

    @staticmethod
    def get_stage_type() -> str:
        return 'Delay'

    def _export_stage(self) -> typing.Dict[str, typing.Any]:
        return {
            'delay': self._delay.total_seconds(),
            'sync_minute': self._sync_minute
        }


class PulseStage(BaseStage):
    def __init__(self, expose_time: TYPE_TIME, recover_time: TYPE_TIME, setup_time: typing.Optional[TYPE_TIME] = None):
        super(PulseStage, self).__init__()

        if setup_time is not None:
            self._setup_time = to_timedelta(setup_time)
        else:
            self._setup_time = None

        self._expose_time = to_timedelta(expose_time)
        self._recover_time = to_timedelta(recover_time)

    def start(self) -> typing.NoReturn:
        pass

    def pause(self) -> typing.NoReturn:
        pass

    def resume(self) -> typing.NoReturn:
        pass

    def tick(self) -> bool:
        pass

    def stop(self) -> typing.NoReturn:
        pass

    def validate(self) -> typing.NoReturn:
        super(PulseStage, self).validate()

    def get_duration(self) -> timedelta:
        if self._setup_time is not None:
            return self._setup_time + self._expose_time + self._recover_time
        else:
            return self._expose_time + self._recover_time

    def get_status(self) -> str:
        pass

    @staticmethod
    def get_stage_type() -> str:
        return 'Pulse'

    def _export_stage(self) -> typing.Dict[str, typing.Any]:
        return {
            'expose_time': self._expose_time.total_seconds(),
            'recover_time': self._recover_time.total_seconds(),
            'setup_time': self._setup_time.total_seconds() if self._setup_time is not None else None
        }


