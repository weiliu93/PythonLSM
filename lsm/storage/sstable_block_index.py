import pickle
import os
import sys
import pickle


sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)

from utils.byte_utils import integer_to_four_bytes_array
from utils.byte_utils import byte_array_to_integer


class SSTableBlockIndex(object):
    """SSTable Index for each block, store maximum key"""

    def __init__(self, max_key, offset, length, filter_offset):
        self.max_key = max_key
        self.offset = offset
        self.length = length
        self.filter_offset = filter_offset

    def serialize(self):
        byte_array = bytearray()
        # append max_key first
        max_key_object = pickle.dumps(self.max_key)
        byte_array.extend(integer_to_four_bytes_array(len(max_key_object)))
        byte_array.extend(max_key_object)
        # then append offset
        byte_array.extend(integer_to_four_bytes_array(self.offset))
        # next length
        byte_array.extend(integer_to_four_bytes_array(self.length))
        # finally filter_offset
        byte_array.extend(integer_to_four_bytes_array(self.filter_offset))
        return bytes(byte_array)

    @staticmethod
    def deserialize(file_io):
        max_key_length = file_io.read(4)
        if len(max_key_length) != 4:
            return None
        max_key_length = byte_array_to_integer(max_key_length)
        max_key = file_io.read(max_key_length)
        if len(max_key) != max_key_length:
            return None
        max_key = pickle.loads(max_key)
        offset = file_io.read(4)
        if len(offset) != 4:
            return None
        offset = byte_array_to_integer(offset)
        length = file_io.read(4)
        if len(length) != 4:
            return None
        length = byte_array_to_integer(length)
        filter_offset = file_io.read(4)
        if len(filter_offset) != 4:
            return None
        filter_offset = byte_array_to_integer(filter_offset)
        return SSTableBlockIndex(max_key, offset, length, filter_offset)

    def __str__(self):
        return "(max_key: {}, offset: {}, length: {}, filter_offset: {})".format(
            self.max_key, self.offset, self.length, self.filter_offset
        )
