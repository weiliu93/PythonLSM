import os
import configparser
import pickle
from enum import Enum

from lsm.utils import byte_utils


class Manifest(object):
    def __init__(self):
        self._sstables = set()
        conf = configparser.ConfigParser()
        conf.read(
            os.path.join(os.path.dirname(__file__), os.pardir, "conf", "lsm_conf.ini")
        )
        manifest_path = os.path.join(
            conf["SSTABLE"]["WORK_DIR"], conf["SSTABLE"]["MANIFEST_FILENAME"]
        )
        # if manifest exists, recover from it
        if os.path.exists(manifest_path):
            with open(manifest_path, "rb") as f:
                for log in self._deserialize_logs(f):
                    sstable_metadata, type = log.sstable_metadata, log.type
                    if type == ManifestLogType.ADD:
                        self._sstables.add(sstable_metadata)
                    else:
                        self._sstables.discard(sstable_metadata)
        self._manifest_path = manifest_path
        self._file = open(self._manifest_path, "ab")

    @property
    def sstables(self):
        return self._sstables

    def add_sstable(self, table_level, table_name, table_size, min_key, max_key):
        sstable_metadata = SSTableMetadata(
            table_level, table_name, table_size, min_key, max_key
        )
        manifest_log = ManifestLog(sstable_metadata, ManifestLogType.ADD)
        self._update_change(manifest_log)

    def remove_sstable(self, table_level, table_name, table_size):
        sstable_metadata = SSTableMetadata(
            table_level, table_name, table_size, None, None
        )
        manifest_log = ManifestLog(sstable_metadata, ManifestLogType.REMOVE)
        self._update_change(manifest_log)

    def close(self):
        self._file.close()

    def compact(self):
        self._file.close()
        compact_sstables = set()
        with open(self._manifest_path, "rb") as f:
            for log in self._deserialize_logs(f):
                sstable_metadata, type = log.sstable_metadata, log.type
                if type == ManifestLogType.ADD:
                    compact_sstables.add(sstable_metadata)
                else:
                    compact_sstables.discard(sstable_metadata)
        with open(self._manifest_path + ".tmp", "wb") as tmp_file:
            for sstable in compact_sstables:
                # only need add operation
                tmp_file.write(ManifestLog(sstable, ManifestLogType.ADD).serialize())
        os.replace(self._manifest_path + ".tmp", self._manifest_path)
        self._file = open(self._manifest_path, "ab")

    def _update_change(self, manifest_log):
        # write disk log first
        byte_array = manifest_log.serialize()
        self._file.write(byte_array)
        self._file.flush()
        # then update in-memory sstable set
        if manifest_log.type == ManifestLogType.ADD:
            self._sstables.add(manifest_log.sstable_metadata)
        else:
            self._sstables.discard(manifest_log.sstable_metadata)

    def _deserialize_logs(self, file_io):
        log = ManifestLog.deserialize(file_io)
        while log:
            yield log
            log = ManifestLog.deserialize(file_io)


class ManifestLog(object):
    def __init__(self, sstable_metadata, type):
        assert isinstance(sstable_metadata, SSTableMetadata)
        assert isinstance(type, ManifestLogType)
        self.sstable_metadata = sstable_metadata
        self.type = type

    def serialize(self):
        # type + table_level + table_name_length + table_name + table_size + min_key_length + min_key + max_key_length + max_key
        # 1bit +    31bit          8bit                            64bit         32bit                      32bit
        byte_array = bytearray()
        # type and table_level
        header = (
            self.sstable_metadata.table_level | (1 << 31)
            if self.type == ManifestLogType.ADD
            else self.sstable_metadata.table_level
        )
        byte_array.extend(byte_utils.integer_to_four_bytes_array(header))
        # table name
        table_name = pickle.dumps(self.sstable_metadata.table_name)
        byte_array.extend(byte_utils.integer_to_n_bytes_array(len(table_name), 1))
        byte_array.extend(table_name)
        # table size
        byte_array.extend(
            byte_utils.integer_to_n_bytes_array(self.sstable_metadata.table_size, 8)
        )
        # serialize min_key and max_key for add operation
        if self.type == ManifestLogType.ADD:
            # min_key
            min_key = pickle.dumps(self.sstable_metadata.min_key)
            byte_array.extend(byte_utils.integer_to_n_bytes_array(len(min_key), 4))
            byte_array.extend(min_key)
            # max_key
            max_key = pickle.dumps(self.sstable_metadata.max_key)
            byte_array.extend(byte_utils.integer_to_n_bytes_array(len(max_key), 4))
            byte_array.extend(max_key)
        return bytes(byte_array)

    @staticmethod
    def deserialize(file_io):
        # type and table_level
        header = file_io.read(4)
        if len(header) != 4:
            return None
        header = byte_utils.byte_array_to_integer(header)
        type = ManifestLogType.ADD if (header & (1 << 31)) else ManifestLogType.REMOVE
        table_level = header & ((1 << 31) - 1)
        # table_name
        table_name_length = file_io.read(1)
        if len(table_name_length) != 1:
            return None
        table_name_length = byte_utils.byte_array_to_integer(table_name_length)
        table_name = file_io.read(table_name_length)
        if len(table_name) != table_name_length:
            return None
        table_name = pickle.loads(table_name)
        # table_size
        table_size = file_io.read(8)
        if len(table_size) != 8:
            return None
        table_size = byte_utils.byte_array_to_integer(table_size)
        if type == ManifestLogType.ADD:
            # min_key
            min_key_length = file_io.read(4)
            if len(min_key_length) != 4:
                return None
            min_key_length = byte_utils.byte_array_to_integer(min_key_length)
            min_key = file_io.read(min_key_length)
            if len(min_key) != min_key_length:
                return None
            min_key = pickle.loads(min_key)
            # max_key
            max_key_length = file_io.read(4)
            if len(max_key_length) != 4:
                return None
            max_key_length = byte_utils.byte_array_to_integer(max_key_length)
            max_key = file_io.read(max_key_length)
            if len(max_key) != max_key_length:
                return None
            max_key = pickle.loads(max_key)
        else:
            min_key, max_key = None, None
        return ManifestLog(
            SSTableMetadata(table_level, table_name, table_size, min_key, max_key), type
        )


class ManifestLogType(Enum):
    ADD = 0
    REMOVE = 1


class SSTableMetadata(object):
    def __init__(self, table_level, table_name, table_size, min_key, max_key):
        # use (table_level, table_name, table_size) to identify a specific sstable
        assert isinstance(table_level, int)
        assert isinstance(table_name, str)
        assert isinstance(table_size, int)

        self.table_level = table_level
        self.table_name = table_name
        self.table_size = table_size
        self.min_key = min_key
        self.max_key = max_key

    def __hash__(self):
        ans = 31
        ans = ans * 37 + self.table_level
        ans = ans * 37 + hash(self.table_name)
        ans = ans * 37 + self.table_size
        return ans

    def __eq__(self, other):
        if isinstance(other, SSTableMetadata):
            return (
                self.table_level == other.table_level
                and self.table_name == other.table_name
                and self.table_size == other.table_size
            )
        else:
            return False

    def __str__(self):
        return "(table_level: {}, table_name: {}, table_size: {}, min_key: {}, max_key: {})".format(
            self.table_level,
            self.table_name,
            self.table_size,
            self.min_key,
            self.max_key,
        )
