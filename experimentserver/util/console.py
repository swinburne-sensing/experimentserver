import logging
import re

# Attempt to import colorama
try:
    import colorama
except ImportError:
    colorama = None


# Colourised logging stream
if colorama:
    # From https://gist.github.com/ravngr/26b84b73a1457d69185e
    # noinspection PyUnresolvedReferences
    class ColoramaStreamHandler(logging.StreamHandler):
        color_map = {
            logging.INFO: colorama.Style.BRIGHT + colorama.Fore.BLACK,
            logging.WARNING: colorama.Style.BRIGHT + colorama.Fore.YELLOW,
            logging.ERROR: colorama.Style.BRIGHT + colorama.Fore.RED,
            logging.CRITICAL: colorama.Back.RED + colorama.Fore.BLACK
        }

        __RE_TRACEBACK = re.compile(r'^[a-z._]+: ', re.IGNORECASE)

        def __init__(self, stream, color_map=None):
            super().__init__(colorama.AnsiToWin32(stream).stream)

            if color_map is not None:
                self.color_map = color_map

        @property
        def is_tty(self):
            # Check if stream is outputting to interactive session
            isatty = getattr(self.stream, 'isatty', None)

            return isatty and isatty()

        def format(self, record):
            message = super().format(record)

            if self.is_tty:
                # Don't colorise tracebacks
                parts = message.split('\n', 1)
                parts[0] = self.colorize(parts[0], record)

                # Do colorise exception lines
                if len(parts) > 1:
                    traceback_split = parts[1].split('\n')

                    for traceback_line in range(len(traceback_split)):
                        if self.__RE_TRACEBACK.match(traceback_split[traceback_line]) is not None:
                            traceback_split[traceback_line] = self.colorize(traceback_split[traceback_line], record)

                    parts[1] = '\n'.join(traceback_split)

                message = '\n'.join(parts)

            return message

        def colorize(self, message, record):
            try:
                return self.color_map[record.levelno] + message + colorama.Style.RESET_ALL

            except KeyError:
                return message
else:
    # Define dummy handler if colorama is not installed
    # noinspection PyUnresolvedReferences
    class ColoramaStreamHandler(logging.StreamHandler):
        def __init__(self, stream, _):
            logging.StreamHandler.__init__(self, stream)
