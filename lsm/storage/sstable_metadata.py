import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)


from utils.byte_utils import integer_to_four_bytes_array
from utils.byte_utils import byte_array_to_integer


class SSTableMetadata(object):
    """
        index_block_offset + index_block_length + filter_block_offset + filter_block_length + data_block_offset
              32bit               32bit                32bit                  32bit               32bit
    """

    def __init__(
        self,
        index_block_offset,
        index_block_length,
        filter_block_offset,
        filter_block_length,
        data_block_offset,
    ):
        self.index_block_offset = index_block_offset
        self.index_block_length = index_block_length
        self.filter_block_offset = filter_block_offset
        self.filter_block_length = filter_block_length
        self.data_block_offset = data_block_offset

    def serialize(self):
        return (
            integer_to_four_bytes_array(self.index_block_offset)
            + integer_to_four_bytes_array(self.index_block_length)
            + integer_to_four_bytes_array(self.filter_block_offset)
            + integer_to_four_bytes_array(self.filter_block_length)
            + integer_to_four_bytes_array(self.data_block_offset)
        )

    @staticmethod
    def deserialize(file_io):
        index_block_offset = file_io.read(4)
        if len(index_block_offset) != 4:
            return None
        index_block_offset = byte_array_to_integer(index_block_offset)

        index_block_length = file_io.read(4)
        if len(index_block_length) != 4:
            return None
        index_block_length = byte_array_to_integer(index_block_length)

        filter_block_offset = file_io.read(4)
        if len(filter_block_offset) != 4:
            return None
        filter_block_offset = byte_array_to_integer(filter_block_offset)

        filter_block_length = file_io.read(4)
        if len(filter_block_length) != 4:
            return None
        filter_block_length = byte_array_to_integer(filter_block_length)

        data_block_offset = file_io.read(4)
        if len(data_block_offset) != 4:
            return None
        data_block_offset = byte_array_to_integer(data_block_offset)

        return SSTableMetadata(
            index_block_offset,
            index_block_length,
            filter_block_offset,
            filter_block_length,
            data_block_offset,
        )

    def __str__(self):
        return "(index_block_offset: {}, index_block_length: {}, filter_block_offset: {}, filter_block_length: {}, data_block_offset: {}".format(
            self.index_block_offset,
            self.index_block_length,
            self.filter_block_offset,
            self.filter_block_length,
            self.data_block_offset,
        )
