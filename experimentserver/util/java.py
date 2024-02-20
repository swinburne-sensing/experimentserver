import re
import typing


class JavaScriptParseException(Exception):
    pass


_RE_JS_ASSIGNMENT = re.compile(r'^([\w.]+)([\w\d\[\]]*) ?= ?([^;]+);$')


def javascript_parse(js_code: str) -> typing.Dict[str, typing.Any]:
    """ Parse JavaScript code for assignments of properties to objects.

    This is a standalone and fairly trivial implementation of a parser, so it would normally get a lot wrong in
    real-world applications, but for IoT style applications it's fine.

    :param js_code: str
    :return:
    """
    js_namespace: typing.Dict[str, typing.Any] = {}

    for js_line in js_code.split('\n'):
        # Fetch only assignment lines
        js_assigmnet = _RE_JS_ASSIGNMENT.match(js_line)

        if js_assigmnet is None or js_assigmnet[3].startswith('new '):
            continue

        js_parent = js_assigmnet[1].split('.')
        js_index = int(js_assigmnet[2][1:-1]) if len(js_assigmnet[2]) > 0 else None
        js_field_value = eval(js_assigmnet[3])

        # Allocate space for name
        js_namespace_node = js_namespace

        for name in js_parent[:-1]:
            if name not in js_namespace_node:
                js_namespace_node[name] = {}

            js_namespace_node = js_namespace_node[name]

        # Allocate space for value
        if js_index is None:
            js_namespace_node[js_parent[-1]] = js_field_value
        else:
            if js_parent[-1] not in js_namespace_node:
                js_namespace_node[js_parent[-1]] = []

            js_namespace_node[js_parent[-1]].append(js_field_value)

    return js_namespace


def remap_javascript_dict(js_code: str, key_map: typing.Dict[str, typing.Tuple[str, str]]) -> typing.Dict[str,
                                                                                                          typing.Any]:
    """

    :param js_code:
    :param key_map:
    :return:
    """
    output_dict = {}

    # Parse object
    response_dict = javascript_parse(js_code)

    # Map to specified keys
    for output_key, source in key_map.items():
        output_dict[output_key] = response_dict[source[0]][source[1]]

    return output_dict
