import threading
import configparser
import os

from lsm.utils.log_util import logger


class SequenceManager(object):
    """trivial solution..."""

    def __init__(self):
        conf = configparser.ConfigParser()
        conf.read(
            os.path.join(os.path.dirname(__file__), os.pardir, "conf", "lsm_conf.ini")
        )
        sequence_number_filepath = conf["SEQUENCE_NUMBER"]["SEQUENCE_NUMBER_FILENAME"]
        if os.path.exists(sequence_number_filepath):
            self._id = int(open(sequence_number_filepath).read())
        else:
            # default initial value
            self._id = 0
        self._file = open(sequence_number_filepath, "w")
        self._lock = threading.Lock()

    @property
    def current(self):
        """latest sequence number"""
        self._lock.acquire()
        ans = self._id
        self._lock.release()
        return ans

    def __next__(self):
        """move to next sequence number"""
        self._lock.acquire()
        self._id += 1
        ans = self._id
        self._write_value_to_disk(ans)
        self._lock.release()
        logger.debug("return next sequence number", sequence_number=ans)
        return ans

    def close(self):
        self._file.close()

    def _write_value_to_disk(self, value):
        try:
            self._file.write(str(value))
            self._file.flush()
        except Exception as e:
            logger.error(
                "write sequence number to disk failed",
                error_message=str(e),
                sequence_number=value,
            )
        logger.debug("write sequence number to disk succeeded", sequence_number=value)
