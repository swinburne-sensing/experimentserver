import collections
import os
import time
import typing

import yaml

import experimentserver
from experimentserver.util.logging import LoggerObject


_TYPING_NODE = typing.Dict[str, typing.Any]


class ConfigException(experimentserver.ApplicationException):
    pass


class ConfigNode(collections.defaultdict):
    def __str__(self) -> str:
        return self.__class__.__name__ + '({' + ', '.join((f"{k!r}: {v!r}" for k, v in self.items())) + '})'


class ConfigManager(object):
    def __init__(self):
        self._config = self._create_node()

    def dump(self):
        """

        :return:
        """
        pass

    def load(self, filename) -> typing.NoReturn:
        """ Load a YAML file as configuration.

        :param filename:
        :return:
        """
        with open(filename) as file:
            content = yaml.load(file, _YAMLShortcutLoader)

        self.update(content)

    def update(self, content) -> typing.NoReturn:
        """

        :param content:
        :return:
        """
        content = self._update_helper(content)

        self._config.update({k.lower(): v for k, v in content.items()})

    def get(self, key, required=False, default=None):
        node = self._config
        key_set = key.split('.')

        for node_key in key_set[:-1]:
            if not issubclass(type(node), dict) or node_key not in node:
                if required:
                    raise KeyError(f"Key node {key} not present in configuration tree ({node_key} not found)")
                else:
                    return default

            node = node[node_key]

        if key_set[-1] not in node:
            if required:
                raise KeyError(f"Key {key} not present in configuration ({key_set[-1]} not found)")
            else:
                return default

        return node[key_set[-1]]

    def __contains__(self, value):
        try:
            self.get(value, required=True)
            return True
        except KeyError:
            return False

    def __getitem__(self, key):
        return self.get(key, required=True)

    @classmethod
    def _create_node(cls, content: typing.Optional[_TYPING_NODE] = None) -> ConfigNode:
        """ Generate a ConfigNode with a default factory that creates child ConfigNode nodes as necessary.

        :param content: initial content for this ConfigNode
        :return: empty ConfigNode or ConfigNode loaded with initial content
        """
        d = ConfigNode(cls._create_node)

        if content is not None:
            d.update(content)

        return d

    @classmethod
    def _flatten_helper(cls, node) -> typing.List[typing.Any]:
        """ Removes lists of lists from child nodes in configuration.

        :param node:
        :return:
        """
        flat_node = []

        for child in node:
            if type(child) is list or type(child) is tuple:
                flat_node.extend(cls._flatten_helper(child))
            else:
                flat_node.append(child)

        return flat_node

    @classmethod
    def _update_helper(cls, node):
        if type(node) is dict:
            parent = cls._create_node()

            for label, node in node.items():
                parent[label] = cls._update_helper(node)

            return parent
        elif type(node) is list or type(node) is tuple:
            node = cls._flatten_helper(node)
            node = [cls._create_node(x) if type(x) is dict else x for x in node]

        return node


class _YAMLShortcutLoader(yaml.SafeLoader, LoggerObject):
    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]

        yaml.SafeLoader.__init__(self, stream=stream)
        LoggerObject.__init__(self)

    def loader_include(self, node):
        # From http://stackoverflow.com/questions/528281/how-can-i-include-an-yaml-file-inside-another
        filename = self.construct_scalar(node)

        filename_abs = os.path.join(self._root, filename)
        filename_abs = os.path.abspath(filename_abs)

        self._logger.debug(f"Include {filename_abs}")

        with open(filename_abs) as f:
            include_data = yaml.load(f, _YAMLShortcutLoader)

            return include_data

    @staticmethod
    def loader_env(_, node):
        if node.value not in os.environ:
            raise ConfigException(f"Environment variable {node.value} not defined")

        return os.environ[node.value]

    @staticmethod
    def loader_env_optional(_, node):
        if node.value not in os.environ:
            return None

        return os.environ[node.value]

    @staticmethod
    def loader_format(_, node):
        module_root = experimentserver.__name__
        path_root = os.path.abspath(os.path.join(os.path.dirname(experimentserver.__file__), '..'))

        return node.value.format(date=time.strftime('%Y%m%d'), time=time.strftime('%H%M%S'), module_root=module_root,
                                 path_root=path_root)


_YAMLShortcutLoader.add_constructor('!include', _YAMLShortcutLoader.loader_include)
_YAMLShortcutLoader.add_constructor('!inc', _YAMLShortcutLoader.loader_include)
_YAMLShortcutLoader.add_constructor('!env', _YAMLShortcutLoader.loader_env)
_YAMLShortcutLoader.add_constructor('!envopt', _YAMLShortcutLoader.loader_env_optional)
_YAMLShortcutLoader.add_constructor('!env_optional', _YAMLShortcutLoader.loader_env_optional)
_YAMLShortcutLoader.add_constructor('!format', _YAMLShortcutLoader.loader_format)
