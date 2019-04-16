import sys
import os
import configparser

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)

from storage.memtable_log import ImmutableMemtableLog
from utils.skiplist import SkipList


class ImmutableMemtable(object):
    def __init__(self, skiplist=None):
        conf = configparser.ConfigParser()
        conf.read(
            os.path.join(
                os.path.join(os.path.dirname(__file__), os.path.pardir),
                "conf",
                "lsm_conf.ini",
            )
        )
        self._immutable_log = ImmutableMemtableLog(
            os.path.join(
                os.path.curdir,
                "memtable",
                conf["MEMTABLE"]["MEMTABLE_LOG_FILENAME"] + ".immutable",
            )
        )
        # immutable from memtable in-memory, just copy data structure
        if skiplist:
            self.__skiplist = skiplist
        else:
            # recover skiplist from logs
            self.__skiplist = SkipList(
                [
                    (
                        internal_key_value.extract_internal_key(),
                        internal_key_value.value,
                    )
                    for internal_key_value in self._immutable_log.logs()
                ]
            )

    def items(self):
        return self.__skiplist.items()

    def __str__(self):
        return (
            "{"
            + ", ".join(
                [
                    "(" + str(key) + ": " + str(value) + ")"
                    for key, value in self.items()
                ]
            )
            + "}"
        )
