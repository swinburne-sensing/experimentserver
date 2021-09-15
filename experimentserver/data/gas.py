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

    def __init__(self, label: str, formula: typing.Optional[str], molecular_structure: MolecularStructure,
                 specific_heat: float, density: float):
        """

        :param label:
        :param formula:
        :param molecular_structure:
        :param specific_heat: specific heat in cal/g
        :param density: density in g/l
        """
        self.label = label
        self.formula = formula
        self.molecular_structure = molecular_structure
        self.specific_heat = specific_heat
        self.density = density

    @staticmethod
    def get_concentration_str(concentration: Quantity) -> str:
        for unit in ('pct', 'ppm', 'ppb'):
            concentration_str = str(concentration.to(unit))

            # Break if unit makes sense
            if concentration.to(unit).magnitude >= 1:
                break

        if'pct' in concentration_str:
            concentration_str = concentration_str.replace(' pct', '%')

        return concentration_str

    def get_concentration_label(self, concentration: Quantity) -> str:
        concentration_str = self.get_concentration_str(concentration)

        return f"{concentration_str} {self!s}"

    def __str__(self):
        if self.formula is not None:
            return f"{self.label} ({self.formula})"
        else:
            return self.label

    def __repr__(self):
        if self.formula is not None:
            return f"{self.__class__.__name__}('{self.label}', '{self.formula}', {self.molecular_structure}, " \
                   f"{self.specific_heat}, {self.density})"
        else:
            return f"{self.__class__.__name__}('{self.label}', None, {self.molecular_structure}, " \
                   f"{self.specific_heat}, {self.density})"


# Sources
# acetone: https://www.engineeringtoolbox.com/acetone-2-propanone-dimethyl-ketone-properties-d_2036.html
# others: MKS
_GAS_CONSTANTS = {
    'unknown': GasConstant('Unknown', None, MolecularStructure.MONOTOMIC, 1, 1),
    'air': GasConstant('Air', None, MolecularStructure.DIATOMIC, 0.24, 1.293),
    'acetone': GasConstant('Acetone', None, MolecularStructure.POLYATOMIC, 0.51, 0.21),
    'ammonia': GasConstant('Ammonia', 'NH_3', MolecularStructure.POLYATOMIC, 0.492, 0.76),
    'argon': GasConstant('Argon', 'Ar', MolecularStructure.MONOTOMIC, 0.1244, 1.782),
    'arsine': GasConstant('Arsine', None, MolecularStructure.POLYATOMIC, 0.1167, 3.478),
    'boron-trichloride': GasConstant('Boron-trichloride', None, MolecularStructure.POLYATOMIC, 0.1279, 5.227),
    'bromine': GasConstant('Bromine', None, MolecularStructure.DIATOMIC, 0.0539, 7.13),
    'carbon-dioxide': GasConstant('Carbon-dioxide', 'CO_2', MolecularStructure.TRIATOMIC, 0.2016, 1.964),
    'carbon-monoxide': GasConstant('Carbon-monoxide', 'CO', MolecularStructure.DIATOMIC, 0.2488, 1.25),
    'carbon-tetrachloride': GasConstant('Carbon-tetrachloride', None, MolecularStructure.POLYATOMIC, 0.1655, 6.86),
    'carbon-tetraflouride': GasConstant('Carbon-tetraflouride', None, MolecularStructure.POLYATOMIC, 0.1654, 3.926),
    'chlorine': GasConstant('Chlorine', 'Cl_2', MolecularStructure.DIATOMIC, 0.1144, 3.163),
    # 'chlorodifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1544, 3.858),
    # 'chloropentafluoroethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.164, 6.892),
    # 'chlorotrifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.153, 4.66),
    'cyanogen': GasConstant('Cyanogen', None, MolecularStructure.POLYATOMIC, 0.2613, 2.322),
    'deuterium': GasConstant('Deuterium', 'H_2/D_2', MolecularStructure.DIATOMIC, 1.722, 0.1799),
    # 'diborane': GasConstant('', MolecularStructure.POLYATOMIC, 0.508, 1.235),
    # 'dibromodifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.15, 9.362),
    # 'dichlorodifluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1432, 5.395),
    # 'dichlorofluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.14, 4.592),
    # 'dichloromethysilane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1882, 5.758),
    # 'dichlorosilane': GasConstant('', MolecularStructure.POLYATOMIC, 0.15, 4.506),
    'ethane': GasConstant('Ethane', 'C_2H_6', MolecularStructure.POLYATOMIC, 0.4097, 1.342),
    'fluorine': GasConstant('Fluorine', 'F_2', MolecularStructure.DIATOMIC, 0.1873, 1.695),
    # 'fluoroform': GasConstant('', MolecularStructure.POLYATOMIC, 0.176, 3.127),
    'helium': GasConstant('Helium', 'He', MolecularStructure.MONOTOMIC, 1.241, 0.1786),
    # 'hexafluoroethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1843, 6.157),
    'hexane': GasConstant('Hexane', 'C_6H14', MolecularStructure.POLYATOMIC, 0.54, 0.672),
    'hydrocarbons': GasConstant('Hydrocarbons', 'C_xH_y', MolecularStructure.POLYATOMIC, 0.54, 0.672),
    'hydrogen': GasConstant('Hydrogen', 'H_2', MolecularStructure.DIATOMIC, 3.419, 0.0899),
    # 'hydrogen-bromide': GasConstant('', MolecularStructure.DIATOMIC, 0.0861, 3.61),
    'hydrogen-chloride': GasConstant('Hydrogen-chloride', 'HCl', MolecularStructure.DIATOMIC, 0.1912, 1.627),
    'hydrogen-fluoride': GasConstant('Hydrogen-fluoride', 'HF', MolecularStructure.DIATOMIC, 0.3479, 0.893),
    # 'isobutylene': GasConstant('', MolecularStructure.POLYATOMIC, 0.3701, 2.503),
    # 'krypton': GasConstant('', MolecularStructure.MONOTOMIC, 0.0593, 3.739),
    'methane': GasConstant('Methane', 'CH_4', MolecularStructure.POLYATOMIC, 0.5328, 0.715),
    # 'methyl-fluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.3221, 1.518),
    # 'molybdenum-hexafluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.1373, 9.366),
    'neon': GasConstant('Neon', 'Ne', MolecularStructure.MONOTOMIC, 0.246, 0.9),
    'nitric-oxide': GasConstant('Nitric-oxide', 'NO', MolecularStructure.DIATOMIC, 0.2328, 1.339),
    'nitric-oxides': GasConstant('Nitric-oxide', 'NOx', MolecularStructure.DIATOMIC, 0.2328, 1.339),
    'nitrogen': GasConstant('Nitrogen', 'N_2', MolecularStructure.DIATOMIC, 0.2485, 1.25),
    'nitrogen-dioxide': GasConstant('Nitrogen-dioxide', 'NO_2', MolecularStructure.TRIATOMIC, 0.1933, 2.052),
    # 'nitrogen-trifluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.1797, 3.168),
    'nitrous-oxide': GasConstant('Nitrous-oxide', 'N_2O', MolecularStructure.TRIATOMIC, 0.2088, 1.964),
    # 'octafluorocyclobutane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1866, 8.93),
    'oxygen': GasConstant('Oxygen', 'O_2', MolecularStructure.DIATOMIC, 0.2193, 1.427),
    # 'pentane': GasConstant('', MolecularStructure.POLYATOMIC, 0.398, 3.219),
    # 'perfluoropropane': GasConstant('', MolecularStructure.POLYATOMIC, 0.194, 8.388),
    # 'phosgene': GasConstant('', MolecularStructure.POLYATOMIC, 0.1394, 4.418),
    'phosphine': GasConstant('Phosphine', 'PH_3', MolecularStructure.POLYATOMIC, 0.2374, 1.517),
    'propane': GasConstant('Propane', 'C_3H_8', MolecularStructure.POLYATOMIC, 0.3885, 1.967),
    'propylene': GasConstant('Propylene', 'C_3H_6', MolecularStructure.POLYATOMIC, 0.3541, 1.877),
    # 'silane': GasConstant('', MolecularStructure.POLYATOMIC, 0.3189, 1.433),
    # 'silicon-tetrachloride': GasConstant('', MolecularStructure.POLYATOMIC, 0.127, 7.58),
    # 'silicon-tetrafluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.1691, 4.643),
    # 'sulfur-dioxide': GasConstant('', MolecularStructure.TRIATOMIC, 0.1488, 2.858),
    'sulfur-hexafluoride': GasConstant('Sulfur Hexaflouride', 'SF_6', MolecularStructure.POLYATOMIC, 0.1592, 6.516),
    # 'trichlorofluoromethane': GasConstant('', MolecularStructure.POLYATOMIC, 0.1357, 6.129),
    # 'trichlorosilane': GasConstant('', MolecularStructure.POLYATOMIC, 0.138, 6.043),
    # 'tungsten-hexafluoride': GasConstant('', MolecularStructure.POLYATOMIC, 0.081, 13.28),
    'xenon': GasConstant('Xenon', 'Xe', MolecularStructure.MONOTOMIC, 0.0378, 5.858),

    # special case or humidity
    'humid_air': GasConstant('Humid Air', None, MolecularStructure.DIATOMIC, 0.24, 1.293),
    'humid_nitrogen': GasConstant('Humid Nitrogen', 'N_2', MolecularStructure.DIATOMIC, 0.2485, 1.25)
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

