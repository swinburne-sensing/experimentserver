import abc
import time
import typing
import uuid
from datetime import datetime, timedelta

import experimentserver
from experimentserver.data.measurement import Measurement, dynamic_field_time_delta
from experimentserver.hardware import Hardware, HardwareTransition
from experimentserver.hardware.manager import HardwareManager
from experimentserver.hardware.metadata import TYPE_PARAMETER_DICT, BoundMetadataCall
from experimentserver.util.logging import LoggerObject


class ExperimentError(experimentserver.ApplicationException):
    """ Base exception for all errors raised by experiment Procedures, Stages, or Conditions. """
    pass


class ProcedureError(ExperimentError):
    """ Exception for errors generated during procedure setup or runtime. """
    pass


class StageError(ExperimentError):
    """ Exception for errors generated during stage setup or runtime. """
    pass


class BaseStage(LoggerObject, metaclass=abc.ABCMeta):
    """  """

    def __init__(self):
        super(BaseStage, self).__init__()

        # Stage entry time
        self._enter_time: typing.Optional[datetime] = None

        # Hardware parameters to apply on entry
        self._stage_hardware_param: typing.Dict[str, typing.Dict[int, BoundMetadataCall]] = {}

        # Additional metadata for stage
        self._stage_metadata = {}

        self._uuid = None

    def add_hardware_parameter(self, hardware: typing.Union[str, HardwareManager], parameters: TYPE_PARAMETER_DICT) \
            -> typing.NoReturn:
        """ Add hardware parameter change to this stage.

        :param hardware: target Hardware object or identifier string
        :param parameters: parameter(s) to apply
        :raises ParameterError:
        """
        if type(hardware) is str:
            hardware = Hardware.get_instance(hardware)

        parameters_bound = hardware.bind_parameter(parameters)
        parameter_map = {hash(x): x for x in parameters_bound}

        if hardware.get_identifier() in self._stage_hardware_param:
            self._stage_hardware_param[hardware.get_identifier()].update(parameter_map)
        else:
            self._stage_hardware_param[hardware.get_identifier()] = parameter_map

    def remove_hardware_parameter(self, hardware: typing.Union[str, HardwareManager]) -> typing.NoReturn:
        """

        :param hardware:
        :return:
        """
        pass

    def clear_hardware_parameter(self):
        self._stage_hardware_param.clear()

    def get_hardware_parameters(self):
        """

        :return:
        """
        return list(self._stage_hardware_param.values())

    def add_metadata(self):
        pass

    def remove_metadata(self):
        pass

    def get_metadata(self):
        pass

    @abc.abstractmethod
    def start(self) -> typing.NoReturn:
        self._uuid = uuid.uuid4()

        # Apply hardware configuration
        for identifier, parameter_map in self._stage_hardware_param.values():
            manager = HardwareManager.get_instance(identifier)

            for parameter in sorted(parameter_map.values()):
                manager.queue_transition(HardwareTransition.PARAMETER, parameter)

        # Push metadata
        Measurement.push_metadata()

        # Field for time since stage started
        Measurement.add_dynamic_field('time_delta_stage', dynamic_field_time_delta(datetime.now()))

        # Stage metadata
        Measurement.add_tag('stage_type', self.__class__.__name__)
        Measurement.add_tag('stage_uuid', self._uuid)
        Measurement.add_tag('stage_time', time.strftime('%Y-%m-%d %H:%M:%S'))
        Measurement.add_tag('stage_timestamp', time.time())

        self.get_logger().info(f"Entering stage, estimated completion in {self.get_duration()!s} at "
                               f"{(datetime.now() + self.get_duration()).strftime('%Y-%m-%d %H:%M:%S')}", event=True)
        self._enter_time = datetime.now()

    @abc.abstractmethod
    def pause(self) -> typing.NoReturn:
        """
        """
        pass

    @abc.abstractmethod
    def resume(self) -> typing.NoReturn:
        """
        """
        pass

    @abc.abstractmethod
    def tick(self) -> bool:
        """
        """
        pass

    @abc.abstractmethod
    def stop(self) -> typing.NoReturn:
        """
        """
        assert self._enter_time is not None

        # Print runtime
        exit_time = datetime.now()

        self.get_logger().info(f"Stage completed in {(exit_time - self._enter_time)!s}", event=True)

        self._enter_time = None

        # Restore metadata
        Measurement.pop_metadata()

    @abc.abstractmethod
    def validate(self) -> typing.NoReturn:
        """

        :raises StageError:
        """
        pass

    @abc.abstractmethod
    def get_duration(self) -> timedelta:
        """ Get estimated runtime for his stage.

        :return: timedelta of expected duration
        """
        pass

    @abc.abstractmethod
    def get_status(self) -> str:
        """ Get the current status of this stage.

        :return: str
        """
        pass

    def export_stage(self) -> typing.Dict[str, typing.Any]:
        # Save class
        obj = {
            'class': self.__class__.__name__
        }

        # Append custom properties
        obj.update(self._export_stage())

        return obj

    @staticmethod
    @abc.abstractmethod
    def get_stage_type() -> str:
        pass

    @abc.abstractmethod
    def _export_stage(self) -> typing.Dict[str, typing.Any]:
        """

        :return:
        """
        pass
