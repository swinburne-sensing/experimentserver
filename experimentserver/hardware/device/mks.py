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
from experimentlib.data.gas import Mixture
from experimentlib.data.unit import T_PARSE_QUANTITY, Quantity, registry, parse
from experimentlib.util.time import now
from transitions import EventData
import urllib3.exceptions

from experimentserver.hardware.base.core import Hardware
from experimentserver.hardware.error import CommunicationError, ParameterError, MeasurementUnavailable
from experimentserver.measurement import T_MEASUREMENT_SEQUENCE, Measurement, MeasurementGroup
from experimentserver.util.java import remap_javascript_dict, JavaScriptParseException
from experimentserver.util.thread import CallbackThread


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class MFCHTTPError(CommunicationError):
    pass


class MFCResponseParserError(CommunicationError):
    """ Error occurred while parsing response from MFC. """
    pass


class GE50MassFlowController(Hardware):
    """
    Class for communication with an MKS G-Series IP-based Mass Flow Controller (MFC). This class is based on significant
    reverse engineering of the undocumented HTTP interface that these MFCs provide for configuration and limited
    monitoring. As such there is little guarantee (beyond manufacture lazyness) that this interface will work with any
    future implementations of this hardware. That said the interface seems to be relatively dated with minimal signs of
    being updated.
    """

    class _EVIDValue(object):
        def __init__(self, field: str, description: str, measurement_group: MeasurementGroup,
                     unit_kwargs: typing.Optional[typing.Dict[str, typing.Any]], enabled: bool = True,
                     apply_gcf: bool = False):
            self.field = field
            self.description = description
            self.measurement_group = measurement_group
            self.unit_kwargs = unit_kwargs
            self.enabled = enabled
            self.apply_gcf = apply_gcf

    # Epoch time for calibration calculation
    _CALIBRATION_EPOCH = datetime(1970, 1, 1)

    # Password for configuration
    _CONFIG_PASSWORD = 'config'

    # Map of EVIDs to field names
    _EVID_MAP = {
        # Measured flow
        'EVID_0': _EVIDValue('flow_actual', 'Actual flow rate', MeasurementGroup.MFC,
                             {'to_unit': 'sccm', 'mag_round': 2}, apply_gcf=True),

        # Flow setpoint
        'EVID_1': _EVIDValue('flow_target', 'Target flow rate', MeasurementGroup.MFC,
                             {'to_unit': 'sccm', 'mag_round': 1}, apply_gcf=True),

        # Valve current
        'EVID_2': _EVIDValue('current_valve', 'Valve current', MeasurementGroup.DEBUG, {'default_unit': 'mA'}, False),

        # Valve temperature
        'EVID_3': _EVIDValue('temperature_valve', 'Valve temperature', MeasurementGroup.TEMPERATURE,
                             {'to_unit': 'degC', 'mag_round': 2}),

        # Always zero?
        'EVID_4': _EVIDValue('current_valve_minimum', 'Minimum valve current (?)', MeasurementGroup.DEBUG,
                             {'to_unit': 'mA'}, False),

        # Something proportional to flow rate
        'EVID_5': _EVIDValue('evid5', 'Unknown 5', MeasurementGroup.DEBUG, None, False),

        # Constant setting for each MFC?
        'EVID_6': _EVIDValue('evid6', 'Unknown 6', MeasurementGroup.DEBUG, None, False),

        # Always zero
        'EVID_7': _EVIDValue('evid7', 'Unknown 7', MeasurementGroup.DEBUG, None, False),

        # Counts to 100 then resets back to zero
        'EVID_8': _EVIDValue('evid8', 'Counter (?)', MeasurementGroup.DEBUG, None, False)
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

    def __init__(self, identifier: str, host: str, composition: typing.Dict[str, str], port: int = 80,
                 pressure: typing.Optional[T_PARSE_QUANTITY] = None, sample_period: float = 0.25, **kwargs):
        """

        :param identifier: passed to parent
        :param host: HTTP host
        :param port: HTTP port
        :param composition: gas composition dict, gas names as keys, concentrations as values
        :param pressure: default working pressure
        """
        Hardware.__init__(self, identifier, **kwargs)

        # HTTP properties
        self._host = host
        self._port = port
        self._sample_period = sample_period

        # Gas properties
        self._pressure: typing.Optional[Quantity] = parse(pressure, registry.psi) if pressure is not None else None
        self._max_flow: typing.Optional[Quantity] = None
        self._min_flow: typing.Optional[Quantity] = None

        # Gas composition
        self._composition: Mixture = Mixture.from_dict(composition)
        self._gcf = self._composition.gcf

        # Lock to prevent multiple access
        self._http_lock = threading.RLock()

        # Consumer thread for trace data
        self._trace_buffer: queue.Queue[typing.List[Measurement]] = queue.Queue(maxsize=1)
        self._thread_trace_consumer: typing.Optional[CallbackThread] = None

    # Utility methods
    def _get_url(self, path: str):
        # noinspection HttpUrlsUsage
        return f"http://{self._host}:{self._port}/{path}"

    def _fetch_url(self, path: str, data: typing.Union[None, bytes, str, typing.Dict[str, typing.Any]] = None,
                   headers: typing.Optional[typing.Dict[str, typing.Any]] = None) -> requests.Response:
        """ Communicate with MFC over HTTP. Can optionally deliver a data payload as a POST request.

        :param path: path to read/write to
        :param data: optional dict of HTTP POST data
        :param headers: optional dict of HTTP headers to attach to the request
        :return: response retrieved from the specified URL
        :raises MFCHTTPError: when HTTP communication error occurs or device response is not 200 OK
        """
        url = self._get_url(path)

        self.logger().trace(f"Request: {url}")

        with self._http_lock:
            post_req = requests.post(url, data=data, headers=headers, timeout=self._HTTP_TIMEOUT)

            try:
                post_req.raise_for_status()
            except requests.exceptions.ConnectionError as exc:
                raise MFCHTTPError('Error during communication with MFC, check power and network connections') from exc
            except IOError as exc:
                raise MFCHTTPError('IO error occurred while communicating with MFC') from exc

            self.logger().trace(f"Response: {post_req}")

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

    def get_composition(self) -> Mixture:
        return self._composition

    # Fetching methods
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

    @Hardware.register_measurement(description='Flow measurements', force=True)
    def get_trace_data(self) -> T_MEASUREMENT_SEQUENCE:
        try:
            return self._trace_buffer.get(timeout=1)
        except queue.Empty:
            raise MeasurementUnavailable()

    # Configuration methods
    def set_config_control(self, digital: bool) -> None:
        """ Enable or disable digital operation.

        :param digital: if True digital override is enable, otherwise analogue control is used
        """
        self._fetch_url('digital_analog_mode', data={'mfc.sp_adc_enable': 0 if digital else 1})

    # Helper for parameters
    @contextlib.contextmanager
    def _setup_lock(self):
        with self._http_lock:
            # Switch to SETUP mode
            self._fetch_url('configure_html_check', {'CONFIG_PASSWORD': self._CONFIG_PASSWORD, 'SUBMIT': ''})

            # Do whatever needs to be done
            yield

            # Return to MONITOR mode
            self._fetch_url('signout.html')

    def auto_zero(self):
        with self._setup_lock():
            self._fetch_url('configure_html_zero', {'SUBMIT_FLOW': 'Zero Flow'})

        self.logger().info('Auto zero complete')

    # Parameters
    @Hardware.register_parameter(description='Target gas flow rate')
    def set_flow_rate(self, flow_rate_raw: T_PARSE_QUANTITY):
        # Enable digital control
        self.set_config_control(True)

        flow_rate_raw = parse(flow_rate_raw, registry.sccm)

        # Apply gas correction factor
        flow_rate = flow_rate_raw / self._gcf

        if flow_rate < 0:
            raise ParameterError(self, f"Requested flow rate {flow_rate_raw} (adjusted: {flow_rate}) is outside valid "
                                       f"range")

        # Force really low flow rates to be zero
        if flow_rate < parse(0.1, registry.sccm):
            flow_rate = parse(0, registry.sccm)

        # Throw warnings if flow is outside recommended limits
        if self._max_flow is None:
            self.logger().error('Unable to check maximum flow rate, not defined')
        elif flow_rate > self._max_flow:
            self.logger().warning(f"Requested flow rate {flow_rate_raw} (adjusted: {flow_rate}) exceeds maximum "
                                  f"flow ({self._max_flow})")

        if self._min_flow is None:
            self.logger().error('Unable to check minimum flow rate, not defined')
        elif 0 < flow_rate < self._min_flow:
            self.logger().warning(f"Requested flow rate {flow_rate_raw} (adjusted: {flow_rate}) below recommended "
                                  f"minimum flow of {self._min_flow}", notify=True)

        self.logger().info(f"Setting flow rate: {flow_rate_raw} (adjusted: {flow_rate})")

        self._fetch_url('flow_setpoint_html', data={'iobuf.setpoint_unit': flow_rate.magnitude})

    @Hardware.register_parameter(description='Working pressure')
    def set_pressure(self, pressure: T_PARSE_QUANTITY):
        pressure = parse(pressure, registry.psi).magnitude

        if pressure <= 0:
            raise ParameterError(self, f"Requested operating pressure {pressure} is outside valid range")

        with self._setup_lock():
            self._fetch_url('device_html_operating_pres', data={'iobuf.pres_up': pressure})

    # Not sure if CGF is implemented in controller correctly, so for now GCF is applied when setting and reading flow
    # @Hardware.register_parameter(description='Gas correction factor')
    # def set_gas_correction_factor(self, gcf: float) -> None:
    #     if gcf <= 0:
    #         raise ParameterError(self, f"Requested operating pressure {gcf} is outside valid range")
    #
    #     with self._setup_lock():
    #         self._fetch_url('flow_setpoint_html', {'iobuf.gcf': gcf})

    # User interface methods
    @staticmethod
    def get_hardware_class_description() -> str:
        return 'MKS GE50A Mass Flow Controller'

    def get_hardware_instance_description(self) -> str:
        return f"{self._composition!s} via {self.get_hardware_class_description()} " \
               f"({self.get_hardware_identifier()} at {self._host}:{self._port})"

    # Event handlers
    def transition_connect(self, event: typing.Optional[EventData] = None) -> None:
        super(GE50MassFlowController, self).transition_connect(event)

        # Read metadata
        metadata_device = self.get_metadata_device()
        metadata_network = self.get_metadata_network()
        metadata_gas = self.get_metadata_gas()

        self._max_flow = parse(metadata_gas['flow_scale_maximum'], metadata_gas['flow_scale_unit'])
        self._min_flow = self._max_flow * self._FLOW_LOWER_BOUND

        self.logger().info(f"Model: {metadata_device['id_model']}")
        self.logger().info(f"Serial: {metadata_device['id_serial']}")
        self.logger().info(f"Firmware: {metadata_network['firmware_version']}")
        self.logger().info(f"MAC address: {metadata_network['network_mac_address']}")
        self.logger().info(f"Calibrated: {metadata_gas['calibration_date'].strftime('%Y-%m-%d')}")
        self.logger().info(f"Maximum flow: {self._max_flow}")
        self.logger().info(f"Minimum flow: {self._min_flow}")

        # Configure trace read, the MKS web interface sends more fields but this seems to be the only one that matters
        config_trace = {
            'Period': str(int(self._sample_period * 1000))
        }

        # Create configuration XML DOM
        xml_trace_define = ElementTree.Element('TraceDefine')
        xml_trace = ElementTree.SubElement(xml_trace_define, 'Trace', config_trace)

        # Define EVIDs
        xml_trace_evids = ElementTree.SubElement(xml_trace, 'EVIDS')

        for evid in [evid for evid, prop in self._EVID_MAP.items() if prop.enabled]:
            ElementTree.SubElement(xml_trace_evids, 'V', {'Name': evid})

        # Export XML as a string to include as a payload
        xml_trace_define_str = ElementTree.tostring(xml_trace_define)

        # Send configuration
        self._fetch_url('ToolWeb/Con', xml_trace_define_str, self._HTTP_HEADER_SETUP)

        # Setup data consumer
        self._thread_trace_consumer = CallbackThread(f"{self.get_hardware_identifier(True)}Trace",
                                                     self._thread_trace_consumer_callback)

        self._thread_trace_consumer.thread_start()

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> None:
        self._max_flow = None

        # Stop data consumer
        if self._thread_trace_consumer:
            self._thread_trace_consumer.thread_stop()
            self._thread_trace_consumer.thread_join()
            self._thread_trace_consumer = None

        super(GE50MassFlowController, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> None:
        # Configure operating pressure and GCF (might be overwritten by parameters
        if self._pressure is not None:
            self.set_pressure(self._pressure)

        # Zero flow rate
        self.set_flow_rate(0)

        # Auto zero
        self.auto_zero()

        super(GE50MassFlowController, self).transition_configure(event)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> None:
        # Zero flow rate
        self.set_flow_rate(0)

        super(GE50MassFlowController, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> None:
        super(GE50MassFlowController, self).transition_error(event)

    def _thread_trace_consumer_callback(self) -> None:
        # Generate tags for payloads
        data_tags = {
            'gas_mixture': str(self._composition),
            'source_ip': f"{self._host}:{self._port}"
        }

        # Open read connection
        request_read = requests.post(self._get_url('ToolWeb/Trace'), headers=self._HTTP_HEADER_READ, stream=True,
                                     timeout=self._HTTP_TIMEOUT)

        try:
            request_read.raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise MFCHTTPError('Error during communication with MFC, check power and network connections') from exc
        except IOError as exc:
            raise MFCHTTPError('IO error occurred while communicating with MFC') from exc

        try:
            for payload in request_read.raw.read_chunked():
                payload_timestamp = now()

                # Check exit flag
                assert self._thread_trace_consumer is not None
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

                xml_field_prop = [(self._EVID_MAP[v_node].field, self._EVID_MAP[v_node].measurement_group,
                                   self._EVID_MAP[v_node].unit_kwargs,
                                   self._gcf if self._EVID_MAP[v_node].apply_gcf else 1)
                                  for v_node in xml_field]

                xml_field_value = [
                    struct.unpack('!f', bytes.fromhex(typing.cast(str, v_node.text)[2:]))[0] for v_node in xml_data
                ]

                data_payload: typing.List[Measurement] = []

                for n in range(len(xml_field)):
                    fields = {
                        xml_field_prop[n][0]: parse(xml_field_value[n], **xml_field_prop[n][2]) * xml_field_prop[n][3]
                    }

                    if xml_field_prop[n][3] != 1:
                        fields[xml_field_prop[n][0] + '_raw'] = parse(xml_field_value[n], **xml_field_prop[n][2])

                    data_payload.append(Measurement(self, xml_field_prop[n][1], fields, payload_timestamp,
                                                    tags=data_tags))

                # Make space in the buffer
                while self._trace_buffer.full():
                    self._trace_buffer.get_nowait()

                # Save data to buffer
                self._trace_buffer.put(data_payload)
        except (ConnectionError, OSError, urllib3.exceptions.HTTPError) as exc:
            # Break out and restart
            raise CommunicationError('Error communicating with MFC') from exc
