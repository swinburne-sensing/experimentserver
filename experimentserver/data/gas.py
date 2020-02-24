import enum
import typing

from .unit import Quantity, units


# Gas constants
class MolecularStructure(enum.Enum):
    """ Molecular structure gas constants. """
    MONOTOMIC = 1.03
    DIATOMIC = 1
    TRIATOMIC = 0.941
    POLYATOMIC = 0.88


class GasConstant(object):
    """ Structure for gas constant definition. """
    def __init__(self, label: str, molecular_structure: MolecularStructure, specific_heat: float, density: float,
                 correction_factor: float):
        self.label = label
        self.molecular_structure = molecular_structure
        self.specific_heat = specific_heat
        self.density = density
        self.correction_factor = correction_factor


# Sources
# acetone: https://www.engineeringtoolbox.com/acetone-2-propanone-dimethyl-ketone-properties-d_2036.html
# others: MKS
_GAS_CONSTANTS = {
    'air': GasConstant('Air', MolecularStructure.DIATOMIC, 0.24, 1.293, 1),
    'acetone': GasConstant('Acetone', MolecularStructure.POLYATOMIC, 0.51, 0.21, 2.9),
    'ammonia': GasConstant('Ammonia (NH_3)', MolecularStructure.POLYATOMIC, 0.492, 0.76, 0.73),
    'argon': GasConstant('Argon (Ar)', MolecularStructure.MONOTOMIC, 0.1244, 1.782, 1.39),
    'arsine': GasConstant('Arsine', MolecularStructure.POLYATOMIC, 0.1167, 3.478, 0.67),
    'boron-trichloride': GasConstant('Boron-trichloride', MolecularStructure.POLYATOMIC, 0.1279, 5.227, 0.41),
    'bromine': GasConstant('Bromine', MolecularStructure.DIATOMIC, 0.0539, 7.13, 0.81),
    'carbon-dioxide': GasConstant('Carbon-dioxide (CO_2)', MolecularStructure.TRIATOMIC, 0.2016, 1.964, 0.7),
    'carbon-monoxide': GasConstant('Carbon-monoxide (CO)', MolecularStructure.DIATOMIC, 0.2488, 1.25, 1),
    'carbon-tetrachloride': GasConstant('Carbon-tetrachloride', MolecularStructure.POLYATOMIC, 0.1655, 6.86, 0.31),
    'carbon-tetraflouride': GasConstant('Carbon-tetraflouride', MolecularStructure.POLYATOMIC, 0.1654, 3.926, 0.42),
    'chlorine': GasConstant('Chlorine (Cl_2)', MolecularStructure.DIATOMIC, 0.1144, 3.163, 0.86),
    # 'chlorodifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1544, 3.858, 0.46),
    # 'chloropentafluoroethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.164, 6.892, 0.24),
    # 'chlorotrifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.153, 4.66, 0.38),
    'cyanogen': GasConstant('Cyanogen', MolecularStructure.POLYATOMIC, 0.2613, 2.322, 0.61),
    'deuterium': GasConstant('Deuterium (H_2/D_2)', MolecularStructure.DIATOMIC, 1.722, 0.1799, 1),
    # 'diborane': GasConstant('', MolecularStructure.POLYATOMIC, 0.508, 1.235, 0.44),
    # 'dibromodifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.15, 9.362, 0.19),
    # 'dichlorodifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1432, 5.395, 0.35),
    # 'dichlorofluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.14, 4.592, 0.42),
    # 'dichloromethysilane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1882, 5.758, 0.25),
    # 'dichlorosilane': GasConstant('', MolecularStructure.POLYATOMIC, 0.15, 4.506, 0.4),
    'ethane': GasConstant('Ethane (C_2H_6)', MolecularStructure.POLYATOMIC, 0.4097, 1.342, 0.5),
    'fluorine': GasConstant('Fluorine (F_2)', MolecularStructure.DIATOMIC, 0.1873, 1.695, 0.98),
    # 'fluoroform': GasConstant('', MolecularStructure.POLYATOMIC, 0.176, 3.127, 0.5),
    'helium': GasConstant('Helium (He)', MolecularStructure.MONOTOMIC, 1.241, 0.1786, 1.45),
    # 'hexafluoroethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1843, 6.157, 0.24),
    'hydrogen': GasConstant('Hydrogen (H_2)', MolecularStructure.DIATOMIC, 3.419, 0.0899, 1.01),
    # 'hydrogen-bromide': GasConstant('', MolecularStructure.DIATOMIC, 0.0861, 3.61, 1),
    'hydrogen-chloride': GasConstant('Hydrogen-chloride (HCl)', MolecularStructure.DIATOMIC, 0.1912, 1.627, 1),
    'hydrogen-fluoride': GasConstant('Hydrogen-fluoride (HF)', MolecularStructure.DIATOMIC, 0.3479, 0.893, 1),
    # 'isobutylene': GasConstant('', MolecularStructure.POLYATOMIC, 0.3701, 2.503, 0.29),
    # 'krypton': GasConstant('', MolecularStructure.MONOTOMIC, 0.0593, 3.739, 1.543),
    'methane': GasConstant('Methane (CH_4)', MolecularStructure.POLYATOMIC, 0.5328, 0.715, 0.72),
    # 'methyl-fluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.3221, 1.518, 0.56),
    # 'molybdenum-hexafluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.1373, 9.366, 0.21),
    'neon': GasConstant('Neon (Ne)', MolecularStructure.MONOTOMIC, 0.246, 0.9, 1.46),
    'nitric-oxide': GasConstant('Nitric-oxide (NO)', MolecularStructure.DIATOMIC, 0.2328, 1.339, 0.99),
    'nitrogen': GasConstant('Nitrogen (N_2)', MolecularStructure.DIATOMIC, 0.2485, 1.25, 1),
    'nitrogen-dioxide': GasConstant('Nitrogen-dioxide (NO_2)', MolecularStructure.TRIATOMIC, 0.1933, 2.052, 0.74),
    # 'nitrogen-trifluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.1797, 3.168, 0.48),
    'nitrous-oxide': GasConstant('Nitrous-oxide (N_2O)', MolecularStructure.TRIATOMIC, 0.2088, 1.964, 0.71),
    # 'octafluorocyclobutane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1866, 8.93, 0.164),
    'oxygen': GasConstant('Oxygen (O_2)', MolecularStructure.DIATOMIC, 0.2193, 1.427, 0.993),
    # 'pentane': GasConstant('', MolecularStructure.POLYATOMIC, 0.398, 3.219, 0.21),
    # 'perfluoropropane': GasConstant('', MolecularStructure.POLYATOMIC, 0.194, 8.388, 0.17),
    # 'phosgene': GasConstant('', MolecularStructure.POLYATOMIC, 0.1394, 4.418, 0.44),
    'phosphine': GasConstant('Phosphine (PH_3)', MolecularStructure.POLYATOMIC, 0.2374, 1.517, 0.76),
    'propane': GasConstant('Propane (C_3H_8)', MolecularStructure.POLYATOMIC, 0.3885, 1.967, 0.36),
    'propylene': GasConstant('Propylene (C_3H_6)', MolecularStructure.POLYATOMIC, 0.3541, 1.877, 0.41),
    # 'silane': GasConstant('', MolecularStructure.POLYATOMIC, 0.3189, 1.433, 0.6),
    # 'silicon-tetrachloride': GasConstant('', MolecularStructure.POLYATOMIC, 0.127, 7.58, 0.28),
    # 'silicon-tetrafluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.1691, 4.643, 0.35),
    # 'sulfur-dioxide': GasConstant('', MolecularStructure.TRIATOMIC, 0.1488, 2.858, 0.69),
    'sulfur-hexafluoride': GasConstant('Sulfur Hexaflouride (SF_6)', MolecularStructure.POLYATOMIC, 0.1592, 6.516, 0.26),
    # 'trichlorofluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1357, 6.129, 0.33),
    # 'trichlorosilane': GasConstant('', MolecularStructure.POLYATOMIC, 0.138, 6.043, 0.33),
    # 'tungsten-hexafluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.081, 13.28, 0.25),
    'xenon': GasConstant('Xenon (Xe)', MolecularStructure.MONOTOMIC, 0.0378, 5.858, 1.32)
}


def get_gas(name: str) -> GasConstant:
    """

    :param name: gas name
    :return: GasConstant
    """
    try:
        return _GAS_CONSTANTS[name.lower()]
    except KeyError:
        raise KeyError(f"Unknown gas {name}")


def calc_gcf(component: typing.Sequence[GasConstant], concentration: typing.Sequence[Quantity],
             balance: GasConstant = _GAS_CONSTANTS['nitrogen']) -> float:
    """ Calculates the gas correction factor for a mixture of gases.

    :param component:
    :param concentration:
    :param balance:
    :return:
    """
    assert len(component) == len(concentration)

    # Convert concentrations to floats
    concentration = [x.to(units.dimensionless).magnitude for x in concentration]

    # Construct gas list
    gas_list: typing.List[typing.Tuple[Quantity, GasConstant]] = [
        (concentration[index], component[index]) for index in range(len(component))
    ]

    # Append balance gas
    gas_list.append((1.0 - sum(concentration), balance))

    # Calculate using generators
    return 0.3106 * sum((gas[0] * gas[1].molecular_structure.value for gas in gas_list)) / \
        sum((gas[0] * gas[1].density * gas[1].specific_heat for gas in gas_list))

