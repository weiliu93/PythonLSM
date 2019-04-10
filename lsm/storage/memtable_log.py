import os

from lsm.storage.internal_key_value import InternalKeyValue


class MemtableLog(object):
    """log for memtable"""

    def __init__(self, filepath):
        self._filepath = filepath
        self._file = open(filepath, "ab")

    def write_log(self, key_value):
        """append a log"""
        assert isinstance(key_value, InternalKeyValue)
        self._file.write(key_value.serialize())
        self._file.flush()

    def write_logs_in_batch(self, key_value_batch):
        """more efficient than single write for many times"""
        write_byte_array = bytearray()
        for key_value in key_value_batch:
            assert isinstance(key_value, InternalKeyValue)
            write_byte_array.extend(key_value.serialize())
        self._file.write(bytes(write_byte_array))
        self._file.flush()

    def logs(self):
        """return a log iterator, can not guarantee concurrency control between read and write"""
        with open(self._filepath, "rb") as f:
            log = InternalKeyValue.deserialize(f)
            # abort loop when corrupted data or EOF found
            while log:
                yield log
                log = InternalKeyValue.deserialize(f)

    def immutable(self):
        # transfer log from mutable to immutable
        self._file.close()
        os.rename(self._filepath, self._filepath + ".immutable")
        self._file = open(self._filepath, "ab")
        return ImmutableMemtableLog(self._filepath + ".immutable")


class ImmutableMemtableLog(object):
    """log for immutable-memtable"""

    def __init__(self, filepath):
        self._filepath = filepath
        # create a dummy file
        if not os.path.exists(filepath):
            open(filepath, "ab").close()

    def logs(self):
        """return file iterator"""
        # create file when necessary
        with open(self._filepath, "rb") as f:
            log = InternalKeyValue.deserialize(f)
            while log:
                yield log
                log = InternalKeyValue.deserialize(f)

    def remove(self):
        os.remove(self._filepath)
