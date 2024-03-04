from __future__ import annotations

import typing
from enum import Enum


class HardwareEnum(Enum):
    """ An enum base class useful for VISA fields with a fixes number of valid values. """

    @classmethod
    def _get_alias_map(cls: typing.Type[TYPE_ENUM]) -> typing.Dict[TYPE_ENUM, typing.List[typing.Any]]:
        """ Get a mapping of VISAEnums to string aliases that might be used for recognition.

        Implementation of this method is optional.

        :return: dict
        """
        raise NotImplementedError()

    @classmethod
    def _get_description_map(cls: typing.Type[TYPE_ENUM]) -> typing.Dict[TYPE_ENUM, str]:
        """ Get a mapping of VISAEnums to description strings.

        :return: dict
        """
        raise NotImplementedError()

    @property
    def description(self) -> str:
        """ Get a user readable description of this VISAEnum.

        :return: str
        """
        description_map = self._get_description_map()

        return description_map[self]

    @property
    def tag_name(self) -> typing.Optional[str]:
        """ Provide an optional tag name so this VISAEnum can be used to supply export _tags.

        Implementation of this method is optional.

        :return:
        """
        return None

    @classmethod
    def _get_tag_map(cls: typing.Type[TYPE_ENUM]) -> typing.Dict[TYPE_ENUM, typing.Any]:
        """ Get a mapping of VISAEnums to tag values.

        Implementation of this method is optional.

        :return: dict
        """
        return {}

    @property
    def tag_value(self) -> typing.Any:
        """ Get the tag value for this VISAEnum.

        :return: str, int, float
        """
        tag_map = self._get_tag_map()

        return tag_map[self]

    @classmethod
    def _get_command_map(cls: typing.Type[TYPE_ENUM]) -> typing.Dict[TYPE_ENUM, str]:
        """ Get a mapping of VISAEnums to tag VISA strings. This is used for conversion to str in VISA calls and vice
        versa.

        :return: dict
        """
        raise NotImplementedError()

    @property
    def command_value(self) -> str:
        """ Get the command string for this enum.

        :return: str representing the enum value in hardware communications
        """
        visa_map = self._get_command_map()

        return visa_map[self]

    @classmethod
    def from_input(cls: typing.Type[TYPE_ENUM], x: typing.Any, allow_direct: bool = False,
                   allow_alias: bool = True, allow_visa: bool = True) -> TYPE_ENUM:
        """ Cast from string to HardwareEnum.

        :param x: str input
        :param allow_direct: if True then direct conversion will be attempted (defaults to False)
        :param allow_alias: if True then conversion from alias will be attempted (defaults to True)
        :param allow_visa: if True then conversion from VISA string will be attempted (defaults to True)
        :return: HardwareEnum
        :raises ValueError: when no match is found
        """
        # Ignore existing enums
        if isinstance(x, cls):
            return x

        if isinstance(x, bytes):
            x = x.decode()

        if allow_direct:
            try:
                return cls[x]
            except KeyError:
                pass

        if isinstance(x, str):
            x = x.strip().lower()

            try:
                x = int(x)
            except ValueError:
                pass

        if allow_alias:
            try:
                for alias_key, alias_value in cls._get_alias_map().items():
                    if x in alias_value:
                        return alias_key
            except NotImplementedError:
                pass

        if allow_visa:
            try:
                # Test against VISA strings
                for cmd_key, cmd_value in cls._get_command_map().items():
                    if x == cmd_value.lower():
                        return cmd_key
            except NotImplementedError:
                pass

        raise ValueError(f"Unknown {cls.__name__} string {x}")

    def __str__(self) -> str:
        return self.description


TYPE_ENUM = typing.TypeVar('TYPE_ENUM', bound=HardwareEnum)
TYPE_ENUM_CAST = typing.Union[str, int]
