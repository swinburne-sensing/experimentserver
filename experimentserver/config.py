import collections
import getpass
import os
import socket
import tempfile
import threading
import time
import typing

import yaml

import experimentserver
from experimentserver.util.logging import LoggerObject


_TYPING_NODE = typing.Dict[str, typing.Any]


def get_system_metadata() -> typing.Dict[str, str]:
    """ Get metadata related to operating environment.

    :return: dict
    """
    server_version = list(map(int, experimentserver.__version__.split('.')))

    return {
        'app_name': experimentserver.__app_name__,
        'app_version': experimentserver.__version__,
        'app_version_major': server_version[0],
        'app_version_minor': server_version[1],
        'app_version_revision': server_version[2],
        'system_hostname': socket.getfqdn(),
        'system_username': getpass.getuser()
    }


class ConfigurationError(experimentserver.ApplicationException):
    """ Exception thrown during configuration update and read. """
    pass


class ConfigNode(collections.defaultdict):
    def __str__(self) -> str:
        return self.__class__.__name__ + '({' + ', '.join((f"{k!s}: {v!s}" for k, v in self.items())) + '})'


class ConfigManager(object):
    def __init__(self):
        self._config = self._create_node()
        self._config_lock = threading.RLock()

    @classmethod
    def _dump_helper(cls, data) -> typing.Dict[str, typing.Any]:
        data_dict = {}

        for key, value in data.items():
            if type(value) is ConfigNode:
                data_dict[key] = cls._dump_helper(value)
            elif type(value) is list:
                data_dict[key] = []

                for element in value:
                    if type(element) is ConfigNode:
                        element = cls._dump_helper(element)

                    data_dict[key].append(element)
            else:
                data_dict[key] = value

        return data_dict

    def dump(self) -> typing.Dict[str, typing.Any]:
        """ Dumps configuration to a dictionary. """
        with self._config_lock:
            return self._dump_helper(self._config)

    def load(self, filename: str) -> typing.NoReturn:
        """ Load a YAML file as configuration.

        :param filename:
        :return:
        """
        with open(filename) as file:
            content = yaml.load(file, YAMLShortcutLoader)

        self.update(content)

    def update(self, content) -> typing.NoReturn:
        """

        :param content:
        :return:
        """
        content = self._update_helper(content)

        with self._config_lock:
            self._config.update({k.lower(): v for k, v in content.items()})

    def get(self, key: str, required: bool = False, default: typing.Any = None) -> typing.Any:
        """ Get configuration from provided key. Config is arranged as a tree with a period (.) denoting tree levels.

        :param key: config key
        :param required: if True exception will be raised when value is not found, otherwise default will be returned
        :param default: default value to return when key is not found
        :return:
        """
        with self._config_lock:
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

    def set(self, key: str, value: typing.Any):
        with self._config_lock:
            node = self._config
            key_set = key.split('.')

            for node_key in key_set[:-1]:
                if not issubclass(type(node), dict) or node_key not in node:
                    node[node_key] = self._create_node()

                node = node[node_key]

            node[key_set[-1]] = value

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


class YAMLShortcutLoader(yaml.SafeLoader, LoggerObject):
    def __init__(self, stream):
        if type(stream) is not str:
            self._root = os.path.split(stream.name)[0]
        else:
            self._root = None

        yaml.SafeLoader.__init__(self, stream=stream)
        LoggerObject.__init__(self)

    @classmethod
    def _format_node(cls, node, parse_str=None):
        if parse_str is None:
            parse_str = node.value

        fields = get_system_metadata()

        # Find home directory
        user_path = os.path.expanduser('~')

        # Add custom fields
        fields.update({
            'app_path': experimentserver.APP_PATH,
            'config_path': experimentserver.CONFIG_PATH,
            'user_path': user_path,
            'root_path': os.path.abspath(os.path.join(experimentserver.APP_PATH, '..')),
            'temp_path': tempfile.gettempdir(),
            'date': time.strftime('%Y%m%d'),
            'time': time.strftime('%H%M%S')
        })

        # Add environment variables
        for env_var, env_val in os.environ.items():
            fields['env_' + env_var.lower()] = env_val

        try:
            return parse_str.format(**fields)
        except IndexError:
            raise ConfigurationError(f"Configuration error in node \"{node.tag} {node.value}\" from "
                                     f"{node.start_mark.name} line {node.start_mark.line}: invalid field format")
        except KeyError as exc:
            raise ConfigurationError(f"Configuration error in node \"{node.tag} {node.value}\" from "
                                     f"{node.start_mark.name} line {node.start_mark.line}: field {exc!s} not found")
        except Exception as exc:
            raise ConfigurationError(f"Configuration error in node \"{node.tag} {node.value}\" from "
                                     f"{node.start_mark.name} line {node.start_mark.line}: unhandled exception") \
                from exc

    def loader_include(self, node):
        if self._root is None:
            raise ConfigurationError('Cannot include files when loading from string')

        filename_abs = None

        for include_path in node.value.split(';'):
            # From http://stackoverflow.com/questions/528281/how-can-i-include-an-yaml-file-inside-another
            filename = self._format_node(node, include_path)

            filename_abs = os.path.join(self._root, filename)
            filename_abs = os.path.abspath(filename_abs)

            if os.path.isfile(filename_abs):
                self.get_logger().info(f"Include {filename_abs}")
                break
            else:
                self.get_logger().info(f"Include not found {filename_abs}")

        assert filename_abs is not None

        with open(filename_abs) as f:
            include_data = yaml.load(f, YAMLShortcutLoader)

            return include_data

    @staticmethod
    def loader_env(_, node):
        if node.value not in os.environ:
            raise ConfigurationError(f"Environment variable {node.value} not defined")

        return os.environ[node.value]

    @staticmethod
    def loader_env_optional(_, node):
        node = node.value.split(' ', 1)

        if node[0] not in os.environ:
            if len(node) == 1:
                return None
            else:
                return node[1]

        return os.environ[node[0]]

    def loader_format(self, node):
        return self._format_node(node)


YAMLShortcutLoader.add_constructor('!include', YAMLShortcutLoader.loader_include)
YAMLShortcutLoader.add_constructor('!inc', YAMLShortcutLoader.loader_include)
YAMLShortcutLoader.add_constructor('!env', YAMLShortcutLoader.loader_env)
YAMLShortcutLoader.add_constructor('!envopt', YAMLShortcutLoader.loader_env_optional)
YAMLShortcutLoader.add_constructor('!env_optional', YAMLShortcutLoader.loader_env_optional)
YAMLShortcutLoader.add_constructor('!format', YAMLShortcutLoader.loader_format)
