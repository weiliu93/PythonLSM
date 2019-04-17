import random
import mmh3
import hashlib

__all__ = [
    "HashFunction",
    "Murmur32HashFunction",
    "Murmur128HashFunction",
    "MD5HashFunction",
    "SHA1HashFunction",
]


class HashFunction(object):
    def __init__(self, bits):
        self._bits = bits
        self._value1 = random.randint(0, self._bits - 1)
        self._value2 = random.randint(0, self._bits - 1)

    def hash(self, value):
        """bits will always be the power of two"""
        return (self._value1 * self._hash(value) + self._value2) & (self._bits - 1)

    def _hash(self, value):
        """all hash functions need to override this template function"""
        return 0

    def __str__(self):
        return "(Function name: {}, Bits: {})".format(
            self.__class__.__name__, self._bits
        )


class Murmur32HashFunction(HashFunction):
    def _hash(self, value):
        return mmh3.hash(value)


class Murmur128HashFunction(HashFunction):
    def _hash(self, value):
        return mmh3.hash128(value)


class MD5HashFunction(HashFunction):
    def _hash(self, value):
        md5 = hashlib.md5()
        md5.update(value)
        return int(md5.hexdigest(), 16)


class SHA1HashFunction(HashFunction):
    def _hash(self, value):
        sha1 = hashlib.sha1()
        sha1.update(value)
        return int(sha1.hexdigest(), 16)


class SHA256HashFunction(HashFunction):
    def _hash(self, value):
        sha256 = hashlib.sha256()
        sha256.update(value)
        return int(sha256.hexdigest(), 16)
