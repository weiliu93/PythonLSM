import os
import sys
import pickle

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)


from utils.byte_utils import integer_to_four_bytes_array
from utils.byte_utils import byte_array_to_integer


class SSTableBlockFilter(object):
    """filters(eg: bloom filter) wrapper in SSTable Block"""

    def __init__(self, block_filter):
        assert block_filter is not None
        self.block_filter = block_filter

    def serialize(self):
        byte_array = bytearray()
        filter_type_byte_array = pickle.dumps(type(self.block_filter))
        # filter type length
        byte_array.extend(integer_to_four_bytes_array(len(filter_type_byte_array)))
        # filter type pickle byte array
        byte_array.extend(filter_type_byte_array)
        # filter byte array
        filter_byte_array = self.block_filter.serialize()
        byte_array.extend(filter_byte_array)
        return byte_array

    @staticmethod
    def deserialize(file_io):
        """filter_type_length + filter_type_byte_array + filter_byte_array
               32bit                                         var
        """
        filter_type_length = file_io.read(4)
        if len(filter_type_length) != 4:
            return None
        filter_type_length = byte_array_to_integer(filter_type_length)

        filter_type_byte_array = file_io.read(filter_type_length)
        if len(filter_type_byte_array) != filter_type_length:
            return None
        filter_type = pickle.loads(filter_type_byte_array)

        filter = filter_type.deserialize(file_io)
        return SSTableBlockFilter(filter)
