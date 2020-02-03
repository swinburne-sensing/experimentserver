from __future__ import annotations

import enum
import typing


class HardwareEnum(enum.Enum):
    """ An enum base class useful for VISA fields with a fixes number of valid values. """

    @classmethod
    def _get_alias_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.List[typing.Any]]]:
        """ Get a mapping of VISAEnums to string aliases that might be used for recognition.

        Implementation of this method is optional.

        :return: dict
        """
        return None

    @classmethod
    def _get_description_map(cls) -> typing.Dict[HardwareEnum, str]:
        """ Get a mapping of VISAEnums to description strings.

        :return: dict
        """
        raise NotImplementedError()

    def get_description(self) -> str:
        """ Get a user readable description of this VISAEnum.

        :return: str
        """
        description_map = self._get_description_map()

        return description_map[self]

    @staticmethod
    def get_tag_name() -> typing.Optional[str]:
        """ Provide an optional tag name so this VISAEnum can be used to supply export _tags.

        Implementation of this method is optional.

        :return:
        """
        return None

    @classmethod
    def _get_tag_map(cls) -> typing.Optional[typing.Dict[HardwareEnum, typing.Any]]:
        """ Get a mapping of VISAEnums to tag values.

        Implementation of this method is optional.

        :return: dict
        """
        return None

    def get_tag_value(self) -> str:
        """ Get the tag value for this VISAEnum.

        :return: str, int, float
        """
        tag_map = self._get_tag_map()

        return tag_map[self]

    @classmethod
    def _get_command_map(cls) -> typing.Dict[HardwareEnum, str]:
        """ Get a mapping of VISAEnums to tag VISA strings. This is used for conversion to str in VISA calls and vice
        versa.

        :return: dict
        """
        raise NotImplementedError()

    def get_command(self) -> str:
        """ Get the command string for this enum.

        :return: str representing the enum value in hardware communications
        """
        visa_map = self._get_command_map()

        return visa_map[self]

    @classmethod
    def from_input(cls, x: typing.Any, allow_direct: bool = False, allow_alias: bool = True,
                   allow_visa: bool = True) -> HardwareEnum:
        """ Cast from string to VISAEnum.

        :param x: str input
        :param allow_direct: if True then direct conversion will be attempted (defaults to False)
        :param allow_alias: if True then conversion from alias will be attempted (defaults to True)
        :param allow_visa: if True then conversion from VISA string will be attempted (defaults to True)
        :return: VISAEnum
        :raises ValueError: when no match is found
        """
        # Ignore existing enums
        if issubclass(type(x), HardwareEnum):
            return x

        if allow_direct:
            try:
                return cls[x]
            except KeyError:
                pass

        if type(x) is str:
            x = x.strip()

            try:
                x = int(x)
            except ValueError:
                pass

        if allow_alias:
            # Test alias map if it exists
            alias_map = cls._get_alias_map()

            if type(x) is str:
                x = x.lower()

            if alias_map is not None:
                for key, value in alias_map.items():
                    if x in value:
                        return key

        if allow_visa:
            # Test against VISA strings
            visa_map = cls._get_command_map()

            for key, value in visa_map.items():
                if x == value.lower():
                    return key

        raise ValueError(f"Unknown VISAEnum string {x}")

    def __str__(self):
        return self.get_description()


TYPE_ENUM = typing.TypeVar('TYPE_ENUM', bound=HardwareEnum)
TYPE_ENUM_CAST = typing.Union[str, int]
