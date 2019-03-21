import random
from bitarray import bitarray

import bloomfilter_hash_functions


class StringBloomFilter(object):
    def __init__(self, func_dict, bits=1024, size_estimate=None):
        self._hash_functions = []
        self._bits = self._ceiling_bits(bits)
        if size_estimate is not None:
            self._bits = self._ceiling_bits(size_estimate) * 16
        self._bitmap = bitarray(self._bits)
        self._bitmap.setall(False)
        assert isinstance(func_dict, dict)
        for hash_class, count in func_dict.items():
            assert issubclass(hash_class, bloomfilter_hash_functions.HashFunction)
            for _ in range(count):
                self._hash_functions.append(hash_class(self._bits))

    @property
    def bits(self):
        return self._bits

    @property
    def funcs(self):
        return len(self._hash_functions)

    def add(self, value):
        value = self._process_value(value)
        for hash_func in self._hash_functions:
            index = hash_func.hash(value)
            self._bitmap[index] = True

    def __or__(self, other):
        # check if two bloom filter is compatible
        assert self.bits == other.bits, "Two StringBloomFilter should have same bits"
        assert set(map(lambda func: type(func), self._hash_functions)) == set(
            map(lambda func: type(func), other._hash_functions)
        ), "Two StringBloomFilter should have same hash functions"
        filter = StringBloomFilter({})
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

    def _ceiling_bits(self, bits):
        ans = 1
        while ans < bits:
            ans <<= 1
        return ans

    def _process_value(self, value):
        if isinstance(value, str):
            value = value.encode("utf-8")
        assert isinstance(value, bytes)
        return value

    def __str__(self):
        return str(self._bitmap)
