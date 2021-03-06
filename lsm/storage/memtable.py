import sys
import os
import configparser


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from utils.skiplist import SkipList
from storage.write_batch import WriteBatch
from storage.internal_key import KeyType, InternalKey
from storage.memtable_log import MemtableLog
from storage.immutable_memtable import ImmutableMemtable


class Memtable(object):
    def __init__(self, sequence_manager):
        conf = configparser.ConfigParser()
        conf.read(
            os.path.join(
                os.path.dirname(__file__), os.path.pardir, "conf", "lsm_conf.ini"
            )
        )
        self._sequence_manager = sequence_manager
        if not os.path.exists(os.path.join(os.curdir, "memtable")):
            os.mkdir(os.path.join(os.curdir, "memtable"))
        memtable_log_filepath = os.path.join(
            os.path.curdir, "memtable", conf["MEMTABLE"]["MEMTABLE_LOG_FILENAME"]
        )
        self._memtable_log = MemtableLog(memtable_log_filepath)
        # recover from memtable log
        self._skiplist = SkipList(
            [
                (internal_key_value.extract_internal_key(), internal_key_value.value)
                for internal_key_value in self._memtable_log.logs()
            ]
        )

    def put(self, key, value):
        ops = [(key, value, KeyType.PUT)]
        self._write_batch(self._group_write_into_batch(ops))

    def remove(self, key):
        ops = [(key, None, KeyType.DELETE)]
        self._write_batch(self._group_write_into_batch(ops))

    def get(self, key, sequence_number=None):
        sequence_number = (
            sequence_number
            if sequence_number is not None
            else self._sequence_manager.current
        )
        # type is a placeholder here
        floor_key, floor_value = self._skiplist.floor(
            InternalKey(key, sequence_number, KeyType.PUT)
        )
        # need to check key again, since we use floor here
        if floor_key.key == key:
            assert floor_key.sequence_number <= sequence_number
            return floor_value
        else:
            return None

    def immutable(self):
        # frozen current skiplist
        immutable_memtable = ImmutableMemtable(self._skiplist)
        # frozen memtable_log
        self._memtable_log.immutable()
        # create a new skiplist for current memtable
        self._skiplist = SkipList()
        return immutable_memtable

    def items(self):
        return self._skiplist.items()

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

    def _group_write_into_batch(self, update_operations):
        """group writes into one batch, remove redundant writes"""
        next_sequence_number = next(self._sequence_manager)
        key_to_last_operation = {
            key: (key, value, type) for key, value, type in update_operations
        }
        return WriteBatch(list(key_to_last_operation.values()), next_sequence_number)

    def _write_batch(self, write_batch):
        """write batch to log and skiplist"""
        # log first
        self._memtable_log.write_logs_in_batch(write_batch)
        # then write data to skiplist
        for internal_key_value in write_batch:
            self._skiplist.put(
                internal_key_value.extract_internal_key(), internal_key_value.value
            )
