from __future__ import annotations

import typing

from experimentlib.data.gas import Mixture
from experimentlib.data.unit import registry, Quantity, parse, T_PARSE_QUANTITY
from transitions import EventData
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder

from experimentserver.hardware.base.core import Hardware
from experimentserver.hardware.error import CommunicationError, ParameterError
from experimentserver.measurement import Measurement, MeasurementGroup


__author__ = 'Chris Harrison'
__email__ = 'cjharrison@swin.edu.au'


class MCMassFlowController(Hardware):
    REGISTER_GET_STATUS = 1201
    REGISTER_GET_PRESSURE = 1203
    REGISTER_GET_TEMPERATURE_FLOW = 1205
    REGISTER_GET_VOLUME_FLOW = 1207
    REGISTER_GET_MASS_FLOW = 1209
    REGISTER_GET_FLOW_SETPOINT = 1211

    REGISTER_SET_FLOW_SETPOINT = 1010

    def __init__(self, identifier: str, host: str, composition: typing.Mapping[str, str], **kwargs):
        Hardware.__init__(self, identifier, **kwargs)

        self._measurement_delay = 0.25

        # Address
        self._host = host

        # Gas composition
        self._composition = Mixture.from_dict(composition)
        self._gcf = self._composition.gcf

        # Start with sane defaults (from factory)
        self._alicat_mass_flow_unit = registry.sccm
        self._alicat_pressure_unit = registry.psi
        self._alicat_temperature_unit = registry.degC

        # Flow rate setpoint
        self._mass_flow_rate = Quantity(0, registry.sccm)

        self._modbus_client = ModbusTcpClient(host)

    @staticmethod
    def get_hardware_class_description() -> str:
        return "Alicat MC Mass Flow Controller"

    def transition_connect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(MCMassFlowController, self).transition_connect(event)

        self._modbus_client.connect()

    def transition_disconnect(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self._modbus_client.close()
        
        super(MCMassFlowController, self).transition_disconnect(event)

    def transition_configure(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        super(MCMassFlowController, self).transition_configure(event)
        
        # Zero flow rate
        self.set_flow_rate(0)

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self.set_flow_rate(0)
        
        super(MCMassFlowController, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        pass

    @Hardware.register_parameter(description='Target gas flow rate')
    def set_flow_rate(self, flow_rate_raw: T_PARSE_QUANTITY):
        flow = parse(flow_rate_raw, registry.sccm)

        # Apply gas correction factor
        flow = flow / self._gcf

        if flow.magnitude < 0:
            raise ParameterError(f"Mass flow rate setpoint must be positive (got: {flow})")

        # Save target
        self._mass_flow_rate = flow

        self.set_mass_flow_setpoint(flow)

    @Hardware.register_measurement(description='Get reading', force=True)
    def get_measurement(self) -> Measurement:
        # Force set target flow rate
        self.set_mass_flow_setpoint(self._mass_flow_rate)

        m = Measurement(self, MeasurementGroup.MFC, {
            'flow_actual': self.get_mass_flow(),
            'flow_target': self.get_mass_flow_setpoint()
        }, tags={
            'gas_mixture': str(self._composition)
        })

        return m

    def _modbus_read_input_float(self, address: int) -> float:
        """

        :param address: register address
        :return:
        """
        response = self._modbus_client.read_input_registers(address - 1, 2)

        if response.isError():
            raise CommunicationError(f"Error during ModbusTCP input register read from {address}: \"{response!s}\"")

        self.logger().debug(f"Input register read {address} response: {response.registers!s}")

        # noinspection PyArgumentEqualDefault
        decoder = BinaryPayloadDecoder.fromRegisters(response.registers, byteorder=Endian.Big, wordorder=Endian.Big)
        value = decoder.decode_32bit_float()

        return value

    def get_mass_flow_setpoint(self) -> Quantity:
        return Quantity(self._modbus_read_input_float(self.REGISTER_GET_FLOW_SETPOINT), self._alicat_mass_flow_unit)

    def set_mass_flow_setpoint(self, flow: Quantity):
        flow = flow.m_as(registry.sccm)

        # noinspection PyArgumentEqualDefault
        builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
        builder.add_32bit_float(flow)
        payload = builder.to_registers()

        address = self.REGISTER_SET_FLOW_SETPOINT
        response = self._modbus_client.write_registers(address - 1, payload)

        if response.isError():
            raise CommunicationError(f"Error during ModbusTCP register write to {address}: \"{response!s}\"")

        self.logger().debug(f"Set mass flow rate: {flow!s}")

    def get_mass_flow(self) -> Quantity:
        return parse(self._modbus_read_input_float(self.REGISTER_GET_MASS_FLOW), self._alicat_mass_flow_unit,
                     mag_round=2)
