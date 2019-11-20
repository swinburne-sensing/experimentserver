import logging
import multiprocessing
import os
import threading
import time

import psutil

from .util.logging import get_logger, LoggerObject


class Observer(LoggerObject):
    def __init__(self):
        super().__init__()

        self._process_stop_flag = multiprocessing.Event()
        self._process = multiprocessing.Process(target=self._process_observer,
                                                args=(os.getpid(), threading.get_ident(), self._process_stop_flag))

    def start(self):
        self._process.start()
        self._logger.info('Observer process started')

    def stop(self):
        if not self._process.is_alive():
            self._logger.error('Observer process not running')

        self._process_stop_flag.set()
        self._process.join()

    def get_pid(self):
        return self._process.pid

    @staticmethod
    def _process_observer(parent_pid: int, parent_tid: int, stop_flag: multiprocessing.Event):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', handlers=[
            logging.FileHandler('observer.log')
        ])

        process_logger = get_logger('observer')

        process_logger.info('Observer started')

        while not stop_flag.is_set():
            # Get parent process status
            try:
                parent_process = psutil.Process(parent_pid)
            except psutil.NoSuchProcess:
                process_logger.error('Parent process has stopped')
                break

            if 'python' not in parent_process.name().lower():
                process_logger.error('Parent process no longer valid')
                break

            tids = [x.id for x in parent_process.threads()]

            if parent_tid not in tids:
                process_logger.error('Process alive but main thread has crashed')

                # Wait (hopefully enough time for threads to finish)
                time.sleep(10)

                # Kill parent process
                process_logger.info('Killing parent process')
                parent_process.kill()
                parent_process.wait()
                process_logger.info('Parent process killed')

                break

            process_logger.debug('Parent OK')

            time.sleep(1)

        process_logger.info('Observer stopped')
