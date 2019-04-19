import sys
import os
import pickle
from bitarray import bitarray

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)

from utils.byte_utils import integer_to_four_bytes_array
from utils.byte_utils import integer_to_n_bytes_array
from utils.byte_utils import byte_array_to_bitarray
from utils.byte_utils import bitarray_to_byte_array
from utils.byte_utils import byte_array_to_integer
from utils import bloomfilter_hash_functions


class BytesBloomFilter(object):
    def __init__(
        self,
        func_dict=None,
        hash_functions=None,
        bits=1024,
        size_estimate=None,
        bitmap=None,
    ):
        if bitmap:
            assert isinstance(bitmap, bitarray)
            self._bits = self._ceiling_bits(len(bitmap))
            self._bitmap = bitarray(self._bits)
            self._bitmap.setall(False)
            self._bitmap = self._bitmap | bitmap
        else:
            self._bits = (
                self._ceiling_bits(bits)
                if size_estimate is None
                else self._ceiling_bits(size_estimate) * 16
            )
            self._bitmap = bitarray(self._bits)
            self._bitmap.setall(False)

        if hash_functions:
            self._hash_functions = hash_functions
        else:
            func_dict = func_dict or {
                bloomfilter_hash_functions.Murmur32HashFunction: 4,
                bloomfilter_hash_functions.MD5HashFunction: 4,
                bloomfilter_hash_functions.SHA1HashFunction: 4,
            }
            assert isinstance(func_dict, dict)
            self._hash_functions = []
            for hash_class, count in func_dict.items():
                self._hash_functions.extend(
                    [hash_class(self._bits) for _ in range(count)]
                )

    @property
    def bits(self):
        return self._bits

    @property
    def hash_functions(self):
        return self._hash_functions

    @property
    def bitmap(self):
        return self._bitmap

    def add(self, value):
        value = self._process_value(value)
        for hash_func in self._hash_functions:
            index = hash_func.hash(value)
            self._bitmap[index] = True

    def __or__(self, other):
        # check if two bloom filter is compatible
        assert self.bits == other.bits, "Two BytesBloomFilter should have same bits"
        assert set(map(lambda func: type(func), self._hash_functions)) == set(
            map(lambda func: type(func), other._hash_functions)
        ), "Two BytesBloomFilter should have same hash functions"
        filter = BytesBloomFilter({})
        filter._hash_functions = self._hash_functions.copy()
        filter._bits = self._bits
        filter._bitmap = self._bitmap | other._bitmap
        return filter

    def __contains__(self, item):
        item = self._process_value(item)
        for hash_func in self._hash_functions:
            index = hash_func.hash(item)
            if not self._bitmap[index]:
                return False
        return True

    def __len__(self):
        return self._bits

    def serialize(self):
        """
        bitmap_byte_array_length + bitmap + hash_functions_length + [func_length + func_bytes] * hash_functions_length
            32bit                                8bit                   32bit
        """
        byte_array = bytearray()
        # bitmap first
        bitmap_byte_array = bitarray_to_byte_array(self._bitmap)
        byte_array.extend(integer_to_four_bytes_array(len(bitmap_byte_array)))
        byte_array.extend(bitmap_byte_array)
        # then hash functions
        byte_array.extend(integer_to_n_bytes_array(len(self._hash_functions), 1))
        for func in self._hash_functions:
            func_object = pickle.dumps(func)
            byte_array.extend(integer_to_four_bytes_array(len(func_object)))
            byte_array.extend(func_object)
        return bytes(byte_array)

    @staticmethod
    def deserialize(file_io):
        bitmap_length = file_io.read(4)
        if len(bitmap_length) != 4:
            return None
        bitmap_length = byte_array_to_integer(bitmap_length)
        bitmap = file_io.read(bitmap_length)
        if len(bitmap) != bitmap_length:
            return None
        bitmap = byte_array_to_bitarray(bitmap)
        hash_function_length = file_io.read(1)
        if len(hash_function_length) != 1:
            return None
        hash_function_length = byte_array_to_integer(hash_function_length)
        hash_functions = []
        for _ in range(hash_function_length):
            func_length = file_io.read(4)
            if len(func_length) != 4:
                return None
            func_length = byte_array_to_integer(func_length)
            func_bytes = file_io.read(func_length)
            if len(func_bytes) != func_length:
                return None
            hash_functions.append(pickle.loads(func_bytes))
        return BytesBloomFilter(hash_functions=hash_functions, bitmap=bitmap)

    def _ceiling_bits(self, bits):
        # ceiling bits to the power of 8, so it could always be stored in a byte array
        ans = 1
        while ans < bits:
            ans <<= 1
        # power of 8
        while (ans & ((1 << 3) - 1)) != 0:
            ans <<= 1
        return ans

    def _process_value(self, value):
        # special check for common string usage
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif isinstance(value, int):
            value = str(value).encode("utf-8")
        assert isinstance(value, bytes)
        return value

    def __str__(self):
        output = "bits: {}, hash_functions: {}, bitmap: {}".format(
            self.bits, self.hash_functions, self.bitmap
        )
        return output
