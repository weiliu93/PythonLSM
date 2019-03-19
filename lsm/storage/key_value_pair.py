from enum import Enum
import pickle
import io


class KeyValuePair(object):
    """key-value pair in-memory representation"""

    def __init__(self, key, value, flags=0):
        key_byte_array, value_byte_array = pickle.dumps(key), pickle.dumps(value)
        self.key, self.value = key, value
        self.header = KeyValuePairHeader(
            len(key_byte_array), len(value_byte_array), flags
        )

    @classmethod
    def load_from_data_stream(cls, stream):
        """load from data stream like `io`, `file` stream"""
        if isinstance(stream, bytes):
            stream = io.BytesIO(stream)

        # first five bytes is header
        byte_array = stream.read(5)
        assert (
            len(byte_array) == 5
        ), "Not enough bytes to construct a key-value pair header"
        header = KeyValuePairHeader.load_from_data_stream(byte_array)

        # then key bytes
        byte_array = stream.read(header.key_length)
        assert len(byte_array) == header.key_length, "Not enough bytes to construct key"
        key = pickle.loads(byte_array)

        # finally value bytes
        byte_array = stream.read(header.value_length)
        assert (
            len(byte_array) == header.value_length
        ), "Not enough bytes to construct value"
        value = pickle.loads(byte_array)

        return cls(key, value, header.flags)

    def to_byte_array(self):
        """use self-defined serialization function, we want to use key-value pair header to decode bytes"""
        return (
            self.header.to_byte_array()
            + pickle.dumps(self.key)
            + pickle.dumps(self.value)
        )

    def __str__(self):
        return "(key: `{}`, value: `{}`, header: `{}`)".format(
            self.key, self.value, self.header
        )


class KeyValuePairHeader(object):
    def __init__(self, key_length, value_length, flags=0):
        """
        Five bytes will be used to represent key-value pair header.
        Use two bytes to represents key length, another two bytes to represents value length.
        One bytes remained could to used to encode other information
        """
        self.key_length = key_length
        self.value_length = value_length
        # convert enum type into bitmap value
        if isinstance(flags, KeyValuePairHeaderFlag):
            flags = 1 << flags.value
        assert flags < (1 << 8)
        self.flags = flags

    @classmethod
    def load_from_data_stream(cls, stream):
        # first two bytes is key_length
        # next two bytes is value_length
        # the remained byte is flags
        if isinstance(stream, bytes):
            stream = io.BytesIO(stream)
        byte_array = stream.read(5)
        assert (
            len(byte_array) == 5
        ), "Not enough bytes to construct key-value pair header"
        key_length = (int(byte_array[0]) << 8) + int(byte_array[1])
        value_length = (int(byte_array[2]) << 8) + int(byte_array[3])
        flags = int(byte_array[4])
        return cls(key_length, value_length, flags)

    def to_byte_array(self):
        values = [
            self.key_length >> 8,
            self.key_length % (1 << 8),
            self.value_length >> 8,
            self.value_length % (1 << 8),
            self.flags % (1 << 8),
        ]
        return bytes(values)

    def set_flag(self, flag):
        """set single flag"""
        assert isinstance(flag, (KeyValuePairHeaderFlag, int))
        if isinstance(flag, KeyValuePairHeaderFlag):
            flag = flag.value
        assert flag < 8
        self.flags |= 1 << flag

    def set_flags(self, flags):
        """set multi flags"""
        assert isinstance(flags, int) and flags < (1 << 8)
        self.flags |= flags

    def reset_flags(self):
        """reset all set flags"""
        self.flags = 0

    def is_deleted(self):
        return self.flags & (1 << KeyValuePairHeaderFlag.KEY_DELETED.value)

    def __str__(self):
        return "(key_length: {}, value_length: {}, flags: {})".format(
            self.key_length, self.value_length, self.flags
        )


class KeyValuePairHeaderFlag(Enum):
    # current key should be deleted in key-value store
    KEY_DELETED = 0
