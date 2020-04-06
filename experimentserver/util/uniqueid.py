import random
import string


def hex_str(length: int = 8) -> str:
    """ Generate a random hexadecimal string of a specified length.

    :param length:
    :return: str
    """
    return ''.join(random.choice(string.hexdigits[:16]) for _ in range(length))
