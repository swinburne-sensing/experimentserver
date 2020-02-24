from __future__ import annotations

import contextlib
import re
import queue
import struct
import threading
import typing
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta

import requests
from transitions import EventData

from .. import Hardware, CommunicationError, ParameterError, MeasurementUnavailable
from ...data import to_unit, get_gas, calc_gcf, TYPE_UNIT, TYPE_UNIT_OPTIONAL, \
    Measurement, MeasurementGroup
from experimentserver.util.java import remap_javascript_dict, JavaScriptParseException
from experimentserver.util.thread import CallbackThread


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class MFCHTTPError(CommunicationError):
    pass


class MFCResponseParserError(CommunicationError):
    """ Error occurred while parsing response from MFC. """
    pass


class _MFCConstant(object):
    # URLs for resources
    MFC_ADDRESS = 'http://{address}:{port}'
    MFC_ADDRESS_FLOW = MFC_ADDRESS + '/flow_setpoint_html'
    MFC_ADDRESS_PRESSURE = MFC_ADDRESS + '/device_html_operating_pres'
    MFC_ADDRESS_SELECT_GAS = MFC_ADDRESS + '/device_html_selected_gas'
    MFC_ADDRESS_MODE = MFC_ADDRESS + '/digital_analog_mode'
    MFC_ADDRESS_RUN_MODE_SETUP = MFC_ADDRESS + '/configure_html_check'
    MFC_ADDRESS_RUN_MODE_MONITOR = MFC_ADDRESS + '/signout.html'
    MFC_ADDRESS_JS_CONFIG_HTML = MFC_ADDRESS + '/configure_html.js'
    MFC_ADDRESS_JS_DEVICE_HTML = MFC_ADDRESS + '/device_html.js'
    MFC_ADDRESS_JS_DEVICE_ID = MFC_ADDRESS + '/deviceid.js'
    MFC_ADDRESS_JS_IOBUF = MFC_ADDRESS + '/iobuf.js'
    MFC_ADDRESS_JS_MFC = MFC_ADDRESS + '/mfc.js'
    MFC_ADDRESS_JS_GAS_LIST = MFC_ADDRESS + '/gaslist.js'
    MFC_ADDRESS_JS_GAS_TABLE = MFC_ADDRESS + '/gastable.js'
    MFC_ADDRESS_XML_TRACE_SETUP = MFC_ADDRESS + '/ToolWeb/Con'
    MFC_ADDRESS_XML_TRACE_READ = MFC_ADDRESS + '/ToolWeb/Trace'


class GE50MassFlowController(Hardware):
    """
    Class for communication with an MKS G-Series IP-based Mass Flow Controller (MFC). This class is based on significant
    reverse engineering of the undocumented HTTP interface that these MFCs provide for configuration and limited
    monitoring. As such there is little guarantee (beyond manufacture lazyness) that this interface will work with any
    future implementations of this hardware. That said the interface seems to be relatively dated with minimal signs of
    being updated.
    """

    # Epoch time for calibration calculation
    _CALIBRATION_EPOCH = datetime(1970, 1, 1)

    # Password for configuration
    _CONFIG_PASSWORD = 'config'

    # Map of EVIDs to field names
    _EVID_MAP = {
        'EVID_0': (True, 'flow_actual', 'Actual flow rate', 'sccm'),
        'EVID_1': (True, 'flow_target', 'Target flow rate', 'sccm'),
        'EVID_2': (True, 'current_valve', 'Valve current', 'mA'),
        'EVID_3': (True, 'temperature_valve', 'Valve temperature', 'degC'),
        'EVID_4': (False, 'current_valve_minimum', 'Minimum valve current (?)', 'mA'),  # Always zero?
        'EVID_5': (False, 'evid5', 'Unknown 5', None),  # Something proportional to flow rate
        'EVID_6': (False, 'evid6', 'Unknown 6', None),  # Constant setting for each MFC?
        'EVID_7': (False, 'evid7', 'Unknown 7', None),  # Always zero
        'EVID_8': (False, 'evid8', 'Counter (?)', None)  # Counts to 100 then resets back to zero
    }

    # MFCs are not accurate below 2% of rated flow
    _FLOW_LOWER_BOUND = 0.02

    # Headers used during trace setup
    _HTTP_HEADER_SETUP = {
        'Accept': 'text/xml',
        'Content-Type': 'text/xml'
    }

    # Headers used during trace read
    _HTTP_HEADER_READ = {
        'Accept': 'text/xml'
    }

    _HTTP_TIMEOUT = 3

    # Recommended 30 minute warm up before use
    _RECOMMENDED_WARM_UP = 30 * 60

    # Regular expression for extraction of gas table data
    _REGEX_GAS = re.compile(r'([0-9]+):[ ]*([a-zA-Z0-9()]+)')

    # Trace name (seems to be ignored)
    _TRACE_NAME = 'XXX'

    def __init__(self, identifier: str, host: str, port: int = 80, balance: str = 'nitrogen',
                 composition: typing.Optional[typing.Dict[str, str]] = None, pressure: TYPE_UNIT_OPTIONAL = None,
                 sample_period: float = 0.1, **kwargs):
        """

        :param identifier: passed to parent
        :param host: HTTP host
        :param port: HTTP port
        :param balance: balance gas
        :param composition: gas composition dict, gas names as keys, concentrations as values
        :param pressure: default working pressure
        """
        super(GE50MassFlowController, self).__init__(identifier, **kwargs)

        self._host = host
        self._port = port

        # Gas properties
        self._balance = get_gas(balance)
        self._pressure = to_unit(pressure, 'psi')
        self._sample_period = sample_period
        self._max_flow = None
        self._min_flow = None

        # Gas composition
        composition = composition or {}

        self._composition = {
            get_gas(gas): to_unit(concentration, 'dimensionless', allow_none=False) for gas, concentration
            in composition.items()
        }

        self._gcf = calc_gcf(list(self._composition.keys()), list(self._composition.values()), self._balance)

        # Lock to prevent multiple access
        self._http_lock = threading.RLock()

        # Consumer thread for trace data
        self._trace_buffer = queue.Queue(maxsize=1)
        self._thread_trace_consumer = CallbackThread(self.get_hardware_identifier(True),
                                                     self._thread_trace_consumer_callback, run_final=False)

    def _get_url(self, path: str):
        return f"http://{self._host}:{self._port}/{path}"

    def _fetch_url(self, path: str, data: typing.Union[None, str, typing.Dict[str, typing.Any]] = None,
                   headers: typing.Optional[typing.Dict[str, typing.Any]] = None) -> requests.Response:
        """ Communicate with MFC over HTTP. Can optionally deliver a data payload as a POST request.

        :param path: path to read/write to
        :param data: optional dict of HTTP POST data
        :param headers: optional dict of HTTP headers to attach to the request
        :return: response retrieved from the specified URL
        :raises MFCHTTPError: when HTTP communication error occurs or device response is not 200 OK
        """
        url = self._get_url(path)

        self.get_logger().debug_transaction(f"Request: {url}")

        with self._http_lock:
            post_req = requests.post(url, data=data, headers=headers, timeout=self._HTTP_TIMEOUT)

            try:
                post_req.raise_for_status()
            except requests.exceptions.ConnectionError as exc:
                raise MFCHTTPError('Error during communication with MFC, check power and network connections') from exc
            except IOError as exc:
                raise MFCHTTPError('IO error occurred while communicating with MFC') from exc

            self.get_logger().debug_transaction(f"Response: {post_req}")

            return post_req

    def _fetch_js(self, url: str, key_map: typing.Dict[str, typing.Tuple[str, str]]) -> typing.Dict[str, typing.Any]:
        # Fetch JavaScript code
        js_code = self._fetch_url(url)

        try:
            # Parse into remapped dict
            return remap_javascript_dict(js_code.text, key_map)
        except JavaScriptParseException as exc:
            raise MFCResponseParserError(f"Could not parse MFC response: {js_code.text!r}") from exc
        except KeyError as exc:
            raise MFCResponseParserError(f"Key not found in response from MFC: {js_code.text!r}") from exc

    def get_config_control(self) -> typing.Dict[str, typing.Any]:
        """ Get MFC control status.

        :return: dict
        """
        return self._fetch_js('mfc.js', {
            'analog_enable': ('mfc', 'sp_adc_enable')
        })

    def get_metadata_device(self) -> typing.Dict[str, typing.Any]:
        """
        Get MFC device information such as model and serial number.
        :return: dict containing MFC data
        """
        return self._fetch_js('deviceid.js', {
            'id_serial': ('deviceid', 'serial'),
            'mode_run': ('deviceid', 'mode'),
            'status': ('deviceid', 'status_code'),
            'id_product': ('deviceid', 'product'),
            'id_model': ('deviceid', 'model_number'),
            'id_type': ('deviceid', 'v_type'),
            'runtime': ('deviceid', 'time_sec')
        })

    def get_metadata_gas(self) -> typing.Dict[str, typing.Any]:
        """ Get MFC gas table configuration and currently selected gas settings.

        :return: dict
        """
        output_dict = self._fetch_js('iobuf.js', {
            'gas_correction_factor': ('iobuf', 'gcf')
        })

        # Request gas configuration
        output_dict.update(self._fetch_js('device_html.js', {
            'flow_scale_unit': ('device_html', 'full_scale_unit_name'),
            'flow_scale_maximum': ('device_html', 'full_scale_amount'),
            'calibration_date': ('device_html', 'calibration_date')
        }))

        # Correct calibration date
        output_dict['calibration_date'] = self._CALIBRATION_EPOCH + timedelta(
            days=output_dict['calibration_date'])

        return output_dict

    def get_metadata_network(self) -> typing.Dict[str, typing.Any]:
        """
        Get MFC network status and configuration.
        :return: dict containing MFC data
        """
        return self._fetch_js('configure_html.js', {
            'network_mac_address': ('configure_html', 'MAC_ADDR'),
            'network_ip_address': ('configure_html', 'TARGET_IP'),
            'network_ip_netmask': ('configure_html', 'NET_MASK'),
            'network_ip_gateway': ('configure_html', 'GATEWAY_IP'),
            'firmware_version': ('configure_html', 'version')
        })

    @Hardware.register_measurement(description='', force=True)
    def get_trace_data(self) -> Measurement:
        try:
            return self._trace_buffer.get(timeout=1)
        except queue.Empty:
            raise MeasurementUnavailable()

    def set_config_control(self, digital: bool) -> typing.NoReturn:
        """ Enable or disable digital operation.

        :param digital: if True digital override is enable, otherwise analogue control is used
        """
        self._fetch_url('digital_analog_mode', data={'mfc.sp_adc_enable': 0 if digital else 1})

    @contextlib.contextmanager
    def _setup_lock(self):
        with self._http_lock:
            # Switch to SETUP mode
            self._fetch_url('configure_html_check', {'CONFIG_PASSWORD': self._CONFIG_PASSWORD, 'SUBMIT': ''})

            # Do whatever needs to be done
            yield

            # Return to MONITOR mode
            self._fetch_url('signout.html')

    @Hardware.register_parameter(description='Target gas flow rate')
    def set_flow_rate(self, flow_rate: TYPE_UNIT):
        flow_rate = to_unit(flow_rate, self._max_flow.units)

        if flow_rate <= 0:
            raise ParameterError(self, f"Requested flow rate {flow_rate} is outside valid range")

        # Force really low flow rates to be zero
        if flow_rate < to_unit('0.1 sccm'):
            flow_rate = to_unit(0, self._max_flow.units)

        # Throw warnings if flow is outside recommended limits
        if flow_rate > self._max_flow:
            self.get_logger().warning(f"Requested flow rate {flow_rate} exceeds maximum flow ({self._max_flow})")
        elif flow_rate < self._min_flow:
            self.get_logger().warning(f"Requested flow rate {flow_rate} below recommended minimum flow "
                                      f"({self._min_flow})", notify=True)

        self._fetch_url('flow_setpoint_html', data={'iobuf.gcf': flow_rate.magnitude})

    @Hardware.register_parameter(description='Working pressure')
    def set_pressure(self, pressure: TYPE_UNIT):
        pressure = to_unit(pressure, 'psi', magnitude=True)

        if pressure <= 0:
            raise ParameterError(self, f"Requested operating pressure {pressure} is outside valid range")

        with self._setup_lock():
            self._fetch_url('device_html_operating_pres', data={'iobuf.pres_up': pressure})

    @Hardware.register_parameter(description='Gas correction factor')
    def set_gas_correction_factor(self, gcf: float) -> typing.NoReturn:
        if gcf <= 0:
            raise ParameterError(self, f"Requested operating pressure {gcf} is outside valid range")

        with self._setup_lock():
            self._fetch_url('flow_setpoint_html', {'iobuf.gcf': gcf})

    @staticmethod
    def get_hardware_class_description() -> str:
        return 'MKS GE50A Mass Flow Controller'

    def get_hardware_instance_description(self) -> str:
        mix = []
        units = ('pct', 'ppm', 'ppb')

        for gas, concentration in self._composition.items():
            for unit in units:
                # Find largest concentration range that makes sense
                if concentration.to(unit).magnitude > 1:
                    concentration = concentration.to(unit)
                    break

            mix.append(f"{concentration} {gas.label}")

        if len(mix) > 0:
            return f"{', '.join(mix)} in {self._balance.label} via {self.get_hardware_class_description()} " \
                   f"({self.get_hardware_identifier()} at {self._host}:{self._port})"
        else:
            return f"{self._balance.label} via {self.get_hardware_class_description()} " \
                   f"({self.get_hardware_identifier()} at {self._host}:{self._port})"

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GE50MassFlowController, self).transition_connect(event)

        # Read metadata
        metadata_device = self.get_metadata_device()
        metadata_network = self.get_metadata_network()
        metadata_gas = self.get_metadata_gas()

        self._max_flow = to_unit(metadata_gas['flow_scale_maximum'], metadata_gas['flow_scale_unit'])
        self._min_flow = self._max_flow * self._FLOW_LOWER_BOUND

        self.get_logger().info(f"Model: {metadata_device['id_model']}")
        self.get_logger().info(f"Serial: {metadata_device['id_serial']}")
        self.get_logger().info(f"Firmware: {metadata_network['firmware_version']}")
        self.get_logger().info(f"MAC address: {metadata_network['network_mac_address']}")
        self.get_logger().info(f"Calibrated: {metadata_gas['calibration_date'].strftime('%Y-%m-%d')}")
        self.get_logger().info(f"Maximum flow: {self._max_flow}")
        self.get_logger().info(f"Minimum flow: {self._min_flow}")

        # Configure trace read, the MKS web interface sends more fields but this seems to be the only one that matters
        config_trace = {
            'Period': str(int(self._sample_period * 1000))
        }

        # Create configuration XML DOM
        xml_trace_define = ElementTree.Element('TraceDefine')
        xml_trace = ElementTree.SubElement(xml_trace_define, 'Trace', config_trace)

        # Define EVIDs
        xml_trace_evids = ElementTree.SubElement(xml_trace, 'EVIDS')

        for evid in [evid for evid, prop in self._EVID_MAP.items() if prop[0]]:
            ElementTree.SubElement(xml_trace_evids, 'V', {'Name': evid})

        # Export XML as a string to include as a payload
        xml_trace_define_str = ElementTree.tostring(xml_trace_define)

        # Send configuration
        self._fetch_url('ToolWeb/Con', xml_trace_define_str, self._HTTP_HEADER_SETUP)

        # Setup data consumer
        self._thread_trace_consumer.thread_start()

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._max_flow = None

        # Stop data consumer
        self._thread_trace_consumer.thread_stop()

        super(GE50MassFlowController, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        # Configure operating pressure and GCF (might be overwritten by parameters
        if self._pressure is not None:
            self.set_pressure(self._pressure)

        self.set_gas_correction_factor(self._gcf)

        super(GE50MassFlowController, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GE50MassFlowController, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(GE50MassFlowController, self).transition_error(event)

    def _thread_trace_consumer_callback(self):
        # Open read connection
        request_read = requests.post(self._get_url('ToolWeb/Trace'), headers=self._HTTP_HEADER_READ, stream=True,
                                     timeout=self._HTTP_TIMEOUT)

        try:
            request_read.raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise MFCHTTPError('Error during communication with MFC, check power and network connections') from exc
        except IOError as exc:
            raise MFCHTTPError('IO error occurred while communicating with MFC') from exc

        for payload in request_read.raw.read_chunked():
            payload_timestamp = datetime.now()

            # Check exit flag
            if self._thread_trace_consumer.thread_stop_requested():
                return

            # Decode XML response
            xml_payload = ElementTree.fromstring(payload.decode())

            # Check for valid root node
            if xml_payload is None or xml_payload.tag != 'Data':
                raise MFCResponseParserError(f"Invalid Data node in payload {payload!r}")

            # Fetch root node
            xml_payload = xml_payload[0]

            if xml_payload.tag != 'BulkTrace' or len(xml_payload) != 2:
                raise MFCResponseParserError(f"Invalid BulkTrace node in payload {payload!r}")

            # Fetch BulkTrace node
            xml_header = xml_payload[0]
            xml_data = xml_payload[1]

            xml_field = [v_node.attrib['name'] for v_node in xml_header]
            xml_field_names = [self._EVID_MAP[v_node][1] for v_node in xml_field]
            xml_field_unit = [self._EVID_MAP[v_node][3] for v_node in xml_field]

            xml_field_value = [struct.unpack('!f', bytes.fromhex(v_node.text[2:]))[0] for v_node in xml_data]

            # Construct payload dict
            data_payload = {}

            for n in range(len(xml_field)):
                data_payload[xml_field_names[n]] = to_unit(xml_field_value[n], xml_field_unit[n])

            # Make space in the buffer
            while self._trace_buffer.full():
                self._trace_buffer.get_nowait()

            # Save data to buffer
            self._trace_buffer.put(Measurement(self, MeasurementGroup.MFC, data_payload, payload_timestamp))
