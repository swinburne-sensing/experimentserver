from __future__ import annotations

import os
import threading

import experimentserver
from .license import generate_license
from .interface import *
from experimentserver.data.unit import to_unit, is_unit
from experimentserver.util.path import add_path
from experimentserver.util.logging import LoggerObject


# Locate SDK files and add to system path
_SDK_PATH = os.path.dirname(os.path.abspath(__file__))
add_path(_SDK_PATH)


class LinkamSDKError(experimentserver.ApplicationException):
    pass


class LinkamConnectionError(LinkamSDKError):
    pass


class LinkamSDK(LoggerObject):
    """ Wrapper for Linkam SDK. """

    class LinkamConnection(LoggerObject):
        """ Wrapper for connection to Linkam controller. """

        def __init__(self, parent: LinkamSDK, handle: CommsHandle):
            super().__init__()

            self._parent = parent
            self._handle = handle

        def enable_heater(self, enabled: bool) -> bool:
            """ Enable/disable the temperature controller.

            :param enabled: if True start heating, otherwise stop heating
            :return:
            """
            return self._parent._sdk_process_message(Message.START_HEATING, ('vBoolean', enabled),
                                                     comm_handle=self._handle)

        def enable_humidity(self, enabled: bool) -> typing.NoReturn:
            """ Enable/disable the humidity generator.

            :param enabled: if True start humidity, otherwise stop humidity
            """
            self._parent._sdk_process_message(Message.START_HUMIDITY, ('vBoolean', enabled), comm_handle=self._handle)

        def get_controller_config(self) -> ControllerConfig:
            return self._parent._sdk_process_message(Message.GET_CONTROLLER_CONFIG, comm_handle=self._handle)

        def get_controller_firmware_version(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_CONTROLLER_FIRMWARE_VERSION, 64, self._handle)

        def get_controller_hardware_version(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_CONTROLLER_HARDWARE_VERSION, 64, self._handle)

        def get_controller_name(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_CONTROLLER_NAME, 26, self._handle)

        def get_controller_serial(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_CONTROLLER_SERIAL, 18, self._handle)

        def get_heater_details(self, channel: int = 0) -> HeaterDetails:
            detail = HeaterDetails()

            self._parent._sdk_process_message(Message.GET_CONTROLLER_HEATER_DETAILS,
                                              ('vPtr', detail),
                                              ('vUint32', channel + 1),
                                              comm_handle=self._handle)

            return detail

        def get_humidity_controller_sensor_name(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_HUMIDITY_CONTROLLER_SENSOR_NAME, 26, self._handle)

        def get_humidity_controller_sensor_serial(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_HUMIDITY_CONTROLLER_SENSOR_SERIAL, 18, self._handle)

        def get_humidity_controller_sensor_hardware_version(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_HUMIDITY_CONTROLLER_SENSOR_HARDWARE_VERSION, 7,
                                                         self._handle)

        def get_humidity_details(self) -> RHUnit:
            detail = RHUnit()

            self._parent._sdk_process_message(Message.GET_VALUE,
                                              ('vStageValueType', StageValueType.STAGE_HUMIDITY_UNIT_DATA),
                                              ('vPtr', detail),
                                              comm_handle=self._handle)

            return detail

        def get_program_state(self) -> Running:
            state = Running()

            self._parent._sdk_process_message(Message.GET_PROGRAM_STATE,
                                              ('vUint32', 1),
                                              ('vPtr', state),
                                              comm_handle=self._handle)

            return state

        def get_stage_firmware_version(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_STAGE_FIRMWARE_VERSION, 64, self._handle)

        def get_stage_hardware_version(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_STAGE_HARDWARE_VERSION, 64, self._handle)

        def get_stage_name(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_STAGE_NAME, 26, self._handle)

        def get_stage_serial(self) -> str:
            return self._parent._sdk_process_message_str(Message.GET_STAGE_SERIAL, 18, self._handle)
            
        def get_status(self) -> ControllerStatus:
            return self._parent._sdk_process_message(Message.GET_STATUS, comm_handle=self._handle)

        def get_stage_config(self) -> StageConfig:
            return self._parent._sdk_process_message(Message.GET_STAGE_CONFIG, comm_handle=self._handle)

        def _get_value_msg(self, message: Message, value_type: StageValueType) -> typing.Any:
            value = self._parent._sdk_process_message(message, ('vStageValueType', value_type.value),
                                                      comm_handle=self._handle)

            # Get variant field
            value = getattr(value, value_type.variant_field)

            # If unit is available then encapsulate it
            if value_type.unit is not None:
                value = to_unit(value, value_type.unit)

            return value

        def get_value(self, value_type: StageValueType) -> typing.Any:
            """ Read parameter from Linkam controller/stage.

            :param value_type: parameter to read
            :return: various
            """
            return self._get_value_msg(Message.GET_VALUE, value_type)

        def get_value_range(self, value_type: StageValueType) -> typing.Tuple[typing.Any, typing.Any]:
            return self._get_value_msg(Message.GET_MIN_VALUE, value_type), self._get_value_msg(Message.GET_MAX_VALUE,
                                                                                               value_type)

        def set_value(self, value_type: StageValueType, n: typing.Any) -> bool:
            """

            :param value_type:
            :param n:
            :return:
            """
            if value_type.unit is not None:
                n = to_unit(n, value_type.unit, magnitude=True)
            elif is_unit(n):
                n = n.magnitude

            return self._parent._sdk_process_message(Message.SET_VALUE, ('vStageValueType', value_type.value),
                                                     (value_type.variant_field, n),
                                                     comm_handle=self._handle)

    def __init__(self, log_path: typing.Optional[str] = None, license_path: typing.Optional[str] = None):
        self._sdk = None
        self._sdk_lock = threading.RLock()

        super(LinkamSDK, self).__init__()

        if log_path is None:
            log_path = os.path.join(_SDK_PATH, 'Linkam.log')

        self._log_path = log_path
        log_path = log_path.encode()

        # Generate temporary license file if none is provided
        if license_path is None:
            license_path = os.path.join(_SDK_PATH, 'Linkam.lsk')

            with open(license_path, 'wb') as license_file:
                generate_license(license_file)

        self._license_path = license_path
        license_path = license_path.encode()

        # Load SDK DLL
        self._sdk = ctypes.WinDLL('LinkamSDK.dll')

        # Initialise SDK
        if not self._sdk.linkamInitialiseSDK(log_path, license_path, True):
            raise LinkamSDKError('Failed to initialize Linkam SDK')

        self.get_logger().info(f"Initialized Linkam SDK {self.get_version()}")

        # Configure default logging
        self.set_logging_level(LoggingLevel.MINIMAL)

    def __del__(self):
        # Release SDK
        if self._sdk is not None:
            self._sdk.linkamExitSDK()

        self._sdk = None

    def _sdk_process_message(self, message: Message, *args: typing.Tuple[str, typing.Any],
                             comm_handle: typing.Optional[CommsHandle] = None) -> typing.Any:
        """ Process Linkam SDK message.

        :param message: message type to process
        :param args: arguments to pass to library (max 3), should be tuples that describe type
        :param comm_handle:
        :param buffer_size:
        :return:
        """
        # Cast arguments to variants
        variant_args = []

        if comm_handle is None:
            comm_handle = CommsHandle(0)

        for arg_type, arg_value in args:
            var_arg = Variant()

            # Cast pointers
            if arg_type == 'vPtr':
                arg_value = ctypes.cast(ctypes.pointer(arg_value), ctypes.c_void_p)

            setattr(var_arg, arg_type, arg_value)

            variant_args.append(var_arg)

        result = Variant()

        with self._sdk_lock:
            try:
                self._sdk.linkamProcessMessage(message.value, comm_handle, ctypes.pointer(result), *variant_args)
            except OSError as exc:
                raise LinkamSDKError('Error occurred while accessing Linkam SDK library') from exc

        if message.variant_field is not None:
            return getattr(result, message.variant_field)

        return result

    def _sdk_process_message_str(self, message: Message, buffer_length: int,
                                 comm_handle: typing.Optional[CommsHandle] = None) -> str:
        # Allocate buffer for return
        buffer = ctypes.create_string_buffer(buffer_length + 1)

        result = self._sdk_process_message(message, ('vPtr', buffer), ('vUint32', buffer_length),
                                           comm_handle=comm_handle)

        if type(result) is bool and not result:
            raise LinkamSDKError(f"Unable to read response to message {message!s}")

        return buffer.value.decode().strip()

    def set_logging_level(self, level: LoggingLevel) -> typing.NoReturn:
        """ Set SDK logging level.

        :param level: logging level to use
        """
        if not self._sdk_process_message(Message.ENABLE_LOGGING, ('vUint32', level.value)):
            raise LinkamSDKError('Cannot configure logging')

    def get_version(self) -> str:
        """ Get SDK version.

        :return: str
        """
        version_buffer = ctypes.create_string_buffer(256)

        with self._sdk_lock:
            if not self._sdk.linkamGetVersion(version_buffer, len(version_buffer)):
                raise LinkamSDKError('Failed to retrieve Linkam SDK version')

        return version_buffer.value.decode()

    def connect_serial(self) -> CommsHandle:
        raise NotImplementedError()

    def connect_usb(self) -> LinkamConnection:
        # Connect via USB
        comm_handle = CommsHandle(0)
        comm_info = CommsInfo()
        self._sdk.linkamInitialiseUSBCommsInfo(ctypes.pointer(comm_info), None)

        connection_result = self._sdk_process_message(Message.OPEN_COMMS, ('vPtr', comm_info), ('vPtr', comm_handle))

        if not connection_result.flags.connected:
            if connection_result.flags.errorNoDeviceFound:
                raise LinkamConnectionError('Device not found')
            elif connection_result.flags.errorMultipleDevicesFound:
                raise LinkamConnectionError('Multiple devices found')
            elif connection_result.flags.errorTimeout:
                raise LinkamConnectionError('USB timeout')
            elif connection_result.flags.errorHandleRegistrationFailed:
                raise LinkamConnectionError('Handle registration failed')
            elif connection_result.flags.errorAllocationFailed:
                raise LinkamConnectionError('Allocation failed')
            elif connection_result.flags.errorSerialNumberRequired:
                raise LinkamConnectionError('Serial number required')
            elif connection_result.flags.errorAlreadyOpen:
                raise LinkamConnectionError('Already open')
            elif connection_result.flags.errorPropertiesIncorrect:
                raise LinkamConnectionError('Properties incorrect')
            elif connection_result.flags.errorPortConfig:
                raise LinkamConnectionError('Invalid port configuration')
            elif connection_result.flags.errorCommsStreams:
                raise LinkamConnectionError('Communication error')
            elif connection_result.flags.errorUnhandled:
                raise LinkamConnectionError('Unhandled error')

        return self.LinkamConnection(self, comm_handle)