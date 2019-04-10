from enum import Enum
import pickle

from lsm.utils import byte_utils


class KeyType(Enum):
    PUT = 0
    DELETE = 1


class InternalKeyValue(object):
    """Internal key value pair representation"""

    def __init__(self, key, sequence_number, type=KeyType.PUT, value=None):
        """
        type  + sequence_number +  key_size +    key    +    value_size +  value
        1bit       63bit            32bit     var-length       32bit     var-length
        """
        assert isinstance(type, KeyType)
        assert key is not None
        assert sequence_number >= 0

        self.type = type
        self.sequence_number = sequence_number
        self.key = key
        self.value = value

    def __str__(self):
        return "(type: {}, sequence_number: {}, key: {}, value: {})".format(
            self.type, self.sequence_number, self.key, self.value
        )

    def __lt__(self, other):
        if self.key < other.key:
            return True
        elif self.key > other.key:
            return False
        else:
            # sequence num could not be the same
            if self.sequence_number < other.sequence_number:
                return True
            else:
                return False

    def __gt__(self, other):
        return not self.__lt__(other)

    def serialize(self):
        """serialize internal key-value pair to byte_array, only pickle objects when necessary"""
        byte_array = bytearray()
        header = (
            self.sequence_number | (1 << 63)
            if self.type == KeyType.PUT
            else self.sequence_number
        )
        # append header first
        byte_array.extend(byte_utils.integer_to_n_bytes_array(header, 8))
        pickle_key = pickle.dumps(self.key)
        # key length
        byte_array.extend(byte_utils.integer_to_four_bytes_array(len(pickle_key)))
        # key byte array
        byte_array.extend(pickle_key)
        # it is a put operation, value is needed
        if self.type == KeyType.PUT:
            pickle_value = pickle.dumps(self.value)
            # value length
            byte_array.extend(byte_utils.integer_to_four_bytes_array(len(pickle_value)))
            # value byte array
            byte_array.extend(pickle_value)
        return bytes(byte_array)

    @staticmethod
    def deserialize(file_io):
        """return None is parsing failed"""
        header = file_io.read(8)
        if len(header) != 8:
            return None
        # parsing header
        header = byte_utils.byte_array_to_integer(header)
        type = KeyType.PUT if (header & (1 << 63)) else KeyType.DELETE
        sequence_number = header & ((1 << 63) - 1)
        # parsing key and value
        key_size = file_io.read(4)
        if len(key_size) != 4:
            return None
        key_size = byte_utils.byte_array_to_integer(key_size)
        key_byte_array = file_io.read(key_size)
        if len(key_byte_array) != key_size:
            return None
        if type == KeyType.PUT:
            value_size = file_io.read(4)
            if len(value_size) != 4:
                return None
            value_size = byte_utils.byte_array_to_integer(value_size)
            value_byte_array = file_io.read(value_size)
            if len(value_byte_array) != value_size:
                return None
            key, value = pickle.loads(key_byte_array), pickle.loads(value_byte_array)
            return InternalKeyValue(
                key=key, sequence_number=sequence_number, type=type, value=value
            )
        else:
            key = pickle.loads(key_byte_array)
            return InternalKeyValue(key=key, sequence_number=sequence_number, type=type)

    @staticmethod
    def delete_key(key, sequence_number):
        return InternalKeyValue(key, sequence_number, type=KeyType.DELETE)

    @staticmethod
    def put_key(key, value, sequence_number):
        return InternalKeyValue(key, sequence_number, type=KeyType.PUT, value=value)
