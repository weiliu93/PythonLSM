import os
import sys
import configparser


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))


from storage.sstable_block_filter import SSTableBlockFilter
from storage.sstable_block_index import SSTableBlockIndex
from utils.bloomfilter import BytesBloomFilter
from logger.log_util import logger


class SSTableBlock(object):

    def __init__(self, sstable):
        conf = configparser.ConfigParser()
        conf.read(os.path.join(os.path.dirname(__file__), os.path.pardir, "conf", "lsm_conf.ini"))
        self._sstable = sstable
        # copy metadata here
        self._sstable_filepath = sstable._sstable_filepath
        self._sstable_index_filepath = sstable._sstable_index_filepath
        self._sstable_filter_filepath = sstable._sstable_filter_filepath
        # filter
        self._filter = BytesBloomFilter(size_estimate = 512)
        # max index data
        self._max_key = None
        # block offset and size limit
        self._block_offset = 0
        self._block_size = int(conf["SSTABLE"]["BLOCK_SIZE"])
        # buffer byte array
        self._byte_array = bytearray()

    def append(self, internal_key_value):
        """if append succeeded, return True, vice versa"""
        byte_array = internal_key_value.serialize()
        if len(byte_array) + self._block_offset <= self._block_size:
            # update byte array
            self._byte_array.extend(byte_array)
            self._block_offset += len(byte_array)
            # update max_key
            if self._max_key is None or internal_key_value.key > self._max_key:
                self._max_key = internal_key_value.key
            # update filter
            self._filter.add(internal_key_value.key)
            return True
        else:
            return False

    def flush(self):
        """flush block information to sstable disk file"""
        # append data
        with open(self._sstable_filepath, "ab") as f:
            f.write(self._byte_array)
            logger.debug("flush sstable data", data_length = len(self._byte_array))
        # append index
        with open(self._sstable_index_filepath, "ab") as f:
            f.write(SSTableBlockIndex(self._max_key, self._sstable._offset, self._block_offset, self._sstable._filter_offset).serialize())
            logger.debug("flush sstable index", max_key = self._max_key, offset = self._sstable._offset, length = self._block_offset)
        # append filter
        with open(self._sstable_filter_filepath, "ab") as f:
            filter_byte_array = SSTableBlockFilter(self._filter).serialize()
            f.write(filter_byte_array)
            # update filter offset
            self._sstable._filter_offset += len(filter_byte_array)
            logger.debug("flush filter", origin_offset = self._sstable._filter_offset - len(filter_byte_array),
                                         current_offset = self._sstable._filter_offset)
        # reset last block in sstable
        self._sstable._last_block = None
        logger.debug("reset last block to None")
        # update offset in sstable
        self._sstable._offset += self._block_offset
        logger.debug("update sstable offset", origin_offset = self._sstable._offset - self._block_offset, current_offset = self._sstable._offset)