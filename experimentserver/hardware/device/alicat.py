from __future__ import annotations

import threading
import typing

from transitions import EventData
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder

from .. import Hardware, HardwareInitError, CommunicationError, ParameterError
from ...data import Measurement, MeasurementGroup, TYPE_UNIT, to_unit, get_gas, calc_gcf, units, Quantity


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

    def __init__(self, identifier: str, host: str, composition: typing.Dict[str, str], sample_period: float = 0.5,
                 **kwargs):
        super(MCMassFlowController, self).__init__(identifier, **kwargs)

        # Address
        self._host = host

        # Gas composition
        self._composition = {}

        balance_gas = None
        overall_concentration = 1

        for gas, concentration in composition.items():
            # Parse gas
            gas = get_gas(gas)

            # Parse concentration
            if concentration.lower() == 'balance' or concentration is None:
                if balance_gas is not None:
                    raise HardwareInitError('Multiple balance gases specified')

                balance_gas = gas
            else:
                concentration = to_unit(concentration, 'dimensionless', allow_none=False)

                # Check if gas is possible
                overall_concentration -= concentration

                if overall_concentration < 0:
                    raise HardwareInitError('Gas composition concentrations exceed 100%')

                self._composition[gas] = concentration

        # Fill remaining concentration with balance gas
        if balance_gas is not None and overall_concentration > 0:
            self._composition[balance_gas] = to_unit(overall_concentration, 'dimensionless')

        self._gcf = calc_gcf(list(self._composition.keys()), list(self._composition.values()))

        # Lock to prevent multiple access
        self._http_lock = threading.RLock()

        self._sample_period = sample_period

        # Start with sane defaults (from factory)
        self._alicat_mass_flow_unit = units.sccm
        self._alicat_pressure_unit = units.psi
        self._alicat_temperature_unit = units.degC

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
        self.mass_flow_setpoint = 0

    def transition_cleanup(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        self.mass_flow_setpoint = 0
        
        super(MCMassFlowController, self).transition_cleanup(event)

    def transition_error(self, event: typing.Optional[EventData] = None) -> typing.NoReturn:
        pass

    def get_gas_mix_label(self) -> str:
        return ', '.join([gas.get_concentration_label(concentration) for gas, concentration in
                          self._composition.items()])

    @Hardware.register_parameter(description='Target gas flow rate')
    def set_flow_rate(self, flow_rate_raw: TYPE_UNIT):
        self.mass_flow_setpoint = flow_rate_raw

    @Hardware.register_measurement(description='Get reading', force=True)
    def get_measurement(self) -> Measurement:
        m = Measurement(self, MeasurementGroup.MFC, {
            'flow_actual': self.mass_flow,
            'flow_target': self.mass_flow_setpoint
        }, tags={
            'gas_mixture': self.get_gas_mix_label()
        })

        self.sleep(0.1, 'rate limit')

        return m

    def _modbus_read_input_float(self, address: int) -> float:
        """

        :param address: register address
        :return:
        """
        response = self._modbus_client.read_input_registers(address - 1, 2)

        if response.isError():
            raise CommunicationError(f"Error during ModbusTCP input register read from {address}: \"{response!s}\"")

        self.get_logger().debug(f"Input register read {address} response: {response.registers!s}")

        decoder = BinaryPayloadDecoder.fromRegisters(response.registers, Endian.Big, Endian.Big)
        value = decoder.decode_32bit_float()

        return value
    
    @property
    def mass_flow_setpoint(self) -> Quantity:
        return Quantity(self._modbus_read_input_float(self.REGISTER_GET_FLOW_SETPOINT), self._alicat_mass_flow_unit)

    @mass_flow_setpoint.setter
    def mass_flow_setpoint(self, flow: units.TYPE_VALUE):
        flow = to_unit(flow, units.sccm)

        if flow.magnitude < 0:
            raise ParameterError(f"Mass flow rate setpoint must be positive (got: {flow})")

        flow = flow.m_as(units.sccm)

        builder = BinaryPayloadBuilder(Endian.Big, Endian.Big)
        builder.add_32bit_float(flow)
        payload = builder.to_registers()

        address = self.REGISTER_SET_FLOW_SETPOINT
        response = self._modbus_client.write_registers(address - 1, payload)

        if response.isError():
            raise CommunicationError(f"Error during ModbusTCP register write to {address}: \"{response!s}\"")

        self.get_logger().info(f"Set mass flow rate: {flow!s}")

    @property
    def mass_flow(self) -> Quantity:
        return Quantity(self._modbus_read_input_float(self.REGISTER_GET_MASS_FLOW), self._alicat_mass_flow_unit)
