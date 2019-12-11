import logging
import queue
import sys
import time
import typing

import pushover

from experimentserver.util.thread import QueueThread


def filter_notify(record: logging.LogRecord):
    # Ignore records without the notify flag or if flag is not set
    # noinspection PyUnresolvedReferences
    if not hasattr(record, 'notify') or not record.notify:
        return False

    return True


class PushoverNotificationHandler(logging.Handler):
    """
    A logging handler that emits messages via Pushover notifications.
    """

    PUSHOVER_PRIORITY_NO_NOTIFICATION = -2
    PUSHOVER_PRIORITY_QUIET = -1
    PUSHOVER_PRIORITY_DEFAULT = 0
    PUSHOVER_PRIORITY_HIGH = 1
    PUSHOVER_PRIORITY_REQUIRE_CONFIRM = 2

    # Map logging levels to Pushover priority levels
    __DEFAULT_PRIORITY_MAP = {
        logging.DEBUG: PUSHOVER_PRIORITY_QUIET,
        logging.INFO: PUSHOVER_PRIORITY_DEFAULT,
        logging.WARNING: PUSHOVER_PRIORITY_DEFAULT,
        logging.ERROR: PUSHOVER_PRIORITY_HIGH,
        logging.CRITICAL: PUSHOVER_PRIORITY_HIGH
    }

    def __init__(self, title, api_token: typing.Optional[str], user_key: typing.Optional[str],
                 user_device: typing.Optional[str] = None, priority_map: typing.Dict[int, int] = None,
                 cache_enabled: bool = True, cache_timeout: int = 60):
        """ Create a Pushover client using the specified API authentication elements.

        :param title: notification title
        :param api_token: token for access to Pushover API
        :param user_key: user key or list of user keys for access to Pushover API
        :param user_device: optional device key to direct notifications to, if not specified all devices are notified
        :param priority_map: dict mapping logging levels to Pushover priority levels
        """
        super().__init__(level=logging.DEBUG)

        # If priority map is provided then override the default mapping
        if priority_map:
            self._priority_map = priority_map
        else:
            self._priority_map = self.__DEFAULT_PRIORITY_MAP

        self._title = title

        # Create Pushover client
        if api_token is not None and user_key is not None:
            self._pushover = pushover.Client(api_token=api_token, user_key=user_key, device=user_device)
        else:
            self._pushover = None

        # Message cache (prevent repeat messages)
        self._message_cache: typing.Optional[typing.Dict[str, typing.List[float]]] = {} if cache_enabled else None
        self._message_cache_timeout = cache_timeout

        # Message queue
        self._message_queue = queue.Queue()

        # Add notify filter
        self.addFilter(filter_notify)

        # Thread for sending messages, prevents blocking for logging
        self._client_thread = QueueThread('Pushover', self._pushover_thread)
        self._client_thread.start()

    def emit(self, record):
        msg = self.format(record)
        priority = self._priority_map[record.levelno] if record.levelno in self._priority_map else 0

        # Append to queue
        self._client_thread.append((msg, priority))

    def _pushover_thread(self, obj):
        if self._pushover is None:
            return

        (msg, priority) = obj

        msg_hash = hash(msg)

        # Check if cache is enabled
        if self._message_cache is not None:
            # Discard old cache timestamps
            for cache_hash, cache_timestamps in self._message_cache.items():
                current_timestamp = time.time()
                cache_timestamps = [x for x in cache_timestamps if current_timestamp <= x]
                self._message_cache[cache_hash] = cache_timestamps

            # Discard empty timestamp lists
            self._message_cache = {k: timestamps for k, timestamps
                                   in self._message_cache.items()
                                   if len(timestamps) > 0}

            if msg_hash in self._message_cache:
                # Append to existing list
                self._message_cache[msg_hash].append(time.time() + self._message_cache_timeout)

                # If length of the list is now 2 send a notification indicating messages were withheld
                if len(self._message_cache[msg_hash]) == 2:
                    self._pushover.send_message('Duplicate message, future duplicates will be withheld\n\n' + msg,
                                                priority=self.PUSHOVER_PRIORITY_QUIET, title=self._title)

                return
            else:
                self._message_cache[msg_hash] = [time.time() + self._message_cache_timeout]

        try:
            # Send message to Pushover client
            self._pushover.send_message(msg, priority=priority, title=self._title)
        except IOError as exc:
            print('Exception occurred while connecting to Pushover', file=sys.stderr)
            print(repr(exc), file=sys.stderr)
