import sys
import os
import configparser

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from storage.sstable_block import SSTableBlock
from storage.sstable_block_index import SSTableBlockIndex
from storage.sstable_block_filter import SSTableBlockFilter
from logger.log_util import logger


class SSTableWriter(object):

    def __init__(self, table_level, table_name):
        assert isinstance(table_level, int)
        assert isinstance(table_name, str)

        conf = configparser.ConfigParser()
        conf.read(os.path.join(os.path.dirname(__file__), os.path.pardir, "conf", "lsm_conf.ini"))
        self._work_dir = conf["SSTABLE"]["WORK_DIR"]
        self._table_size_limit = int(conf["SSTABLE"]["TABLE_SIZE_LIMIT"])
        self._sstable_filepath = os.path.join(self._work_dir, str(table_level), table_name)
        self._sstable_index_filepath = self._sstable_filepath + ".index"
        self._sstable_filter_filepath = self._sstable_filepath + ".filter"
        self._offset, self._filter_offset = 0, 0
        self._last_block = None

    def new_block(self):
        # flush block in-memory first
        if self._last_block:
            self._last_block.flush()
        self._last_block = None
        # allocate a new block to flush data
        file_byte_size = os.stat(self._sstable_filepath).st_size if os.path.exists(self._sstable_filepath) else 0
        if file_byte_size < self._table_size_limit:
            block = SSTableBlock(self)
            self._last_block = block
            return block
        else:
            logger.debug("block num reach threshold")
            return None

    def close(self):
        """frozen sstable"""
        if self._last_block:
            self._last_block.flush()
        self._last_block = None


class SSTableReader(object):

    def __init__(self, table_level, table_name):
        assert isinstance(table_level, int)
        assert isinstance(table_name, str)
        conf = configparser.ConfigParser()
        conf.read(os.path.join(os.path.dirname(__file__), os.path.pardir, "conf", "lsm_conf.ini"))
        self._work_dir = conf["SSTABLE"]["WORK_DIR"]
        self._sstable_filepath = os.path.join(self._work_dir, str(table_level), table_name)
        self._sstable_index_filepath = self._sstable_filepath + ".index"
        self._sstable_filter_filepath = self._sstable_filepath + ".filter"
        # deserialize index_blocks and filter_blocks
        self._data_block_file = open(self._sstable_filepath, "rb")
        self._sstable_indexes = []
        self._sstable_filters = []
        with open(self._sstable_index_filepath, "rb") as f:
            sstable_index = SSTableBlockIndex.deserialize(f)
            while sstable_index:
                self._sstable_indexes.append(sstable_index)
                sstable_index = SSTableBlockIndex.deserialize(f)
        with open(self._sstable_filter_filepath, "rb") as f:
            sstable_filter = SSTableBlockFilter.deserialize(f)
            while sstable_filter:
                self._sstable_filters.append(sstable_filter)
                sstable_filter = SSTableBlockFilter.deserialize(f)

    @property
    def sstable_indexes(self):
        return self._sstable_indexes

    @property
    def sstable_filters(self):
        return self._sstable_filters

    def load_block(self, offset, length):
        self._data_block_file.seek(offset)
        # TODO load block