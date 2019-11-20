import logging.handlers
import unittest

import experimentserver.util.logging as experimentlogging


class TestHandler(logging.handlers.BufferingHandler):
    pass


# noinspection DuplicatedCode
class MyTestCase(unittest.TestCase):
    def test_basic(self):
        # Create buffer for log events
        handler = TestHandler(100)
        handler.setLevel(logging.DEBUG)

        # Create test logging object
        logger = experimentlogging.get_logger('test')
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        # Log some normal messages
        logger.debug('debug')
        logger.info('info')
        logger.warning('warning')
        logger.error('error')
        logger.critical('critical')

        # noinspection PyBroadException
        try:
            raise Exception('test')
        except Exception:
            logger.exception('exception')

        # Read logging buffer
        # DEBUG message should have been discarded
        record = handler.buffer.pop(0)
        self.assertTrue(record.msg == 'info', 'Unexpected record message')
        self.assertTrue(record.levelno == logging.INFO, 'Unexpected record level')
        self.assertFalse(record.notify, 'Unexpected notify flag')
        self.assertFalse(record.event, 'Unexpected event flag')

        record = handler.buffer.pop(0)
        self.assertTrue(record.msg == 'warning', 'Unexpected record message')
        self.assertTrue(record.levelno == logging.WARNING, 'Unexpected record level')
        self.assertFalse(record.notify, 'Unexpected notify flag')
        self.assertTrue(record.event, 'Unexpected event flag')

        record = handler.buffer.pop(0)
        self.assertTrue(record.msg == 'error', 'Unexpected record message')
        self.assertTrue(record.levelno == logging.ERROR, 'Unexpected record level')
        self.assertTrue(record.notify, 'Unexpected notify flag')
        self.assertTrue(record.event, 'Unexpected event flag')

        record = handler.buffer.pop(0)
        self.assertTrue(record.msg == 'critical', 'Unexpected record message')
        self.assertTrue(record.levelno == logging.CRITICAL, 'Unexpected record level')
        self.assertTrue(record.notify, 'Unexpected notify flag')
        self.assertTrue(record.event, 'Unexpected event flag')

        record = handler.buffer.pop(0)
        self.assertTrue(record.msg == 'exception', 'Unexpected record message')
        self.assertTrue(record.levelno == logging.ERROR, 'Unexpected record level')
        self.assertTrue(record.notify, 'Unexpected notify flag')
        self.assertTrue(record.event, 'Unexpected event flag')

        # Clear buffer
        handler.flush()


if __name__ == '__main__':
    unittest.main()
