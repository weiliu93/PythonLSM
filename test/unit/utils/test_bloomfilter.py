import sys
import os
import inspect
import shutil
import random
import string


sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            os.path.pardir,
            os.path.pardir,
            os.path.pardir,
            "lsm",
            "utils",
        )
    )
)


package_root = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        os.path.pardir,
        "packages",
        "utils",
        "bloomfilter",
    )
)

from bloomfilter import StringBloomFilter
from bloomfilter_hash_functions import (
    Murmur32HashFunction,
    MD5HashFunction,
    SHA1HashFunction,
)


def test_basic_add_and_existing_check():
    func_dict = {Murmur32HashFunction: 3, MD5HashFunction: 3, SHA1HashFunction: 3}
    filter = StringBloomFilter(func_dict, bits=1024)
    s = set()
    values = ["sdf", "bw", "we", "bsd", "were", "hey"]
    missing_values = ["bwer", "bdsaf", "zsdf", "fewe"]
    for value in values:
        assert (value in s) == (value in filter)
        s.add(value)
        filter.add(value)
    for missing_value in missing_values:
        assert (missing_value in s) == (missing_value in filter)


def test_bloom_filter_bits_is_always_power_of_two():
    func_dict = {Murmur32HashFunction: 3, MD5HashFunction: 3, SHA1HashFunction: 3}
    filter = StringBloomFilter(func_dict, bits=random.randint(1, 10000))
    bits = filter.bits
    assert bits & (-bits) == bits


def test_bloom_filter_or():
    func_dict = {Murmur32HashFunction: 3, MD5HashFunction: 3, SHA1HashFunction: 3}
    filter1 = StringBloomFilter(func_dict, bits=20)
    filter2 = StringBloomFilter(func_dict, bits=20)
    filter1.add("dsf")
    filter1.add("bsdf")
    filter2.add("bdfdsf")
    filter2.add("were")
    filter = filter1 | filter2
    assert filter.bitmap == filter1.bitmap | filter2.bitmap
    # check if we have created a new object
    assert id(filter.bitmap) != id(filter1.bitmap) and id(filter.bitmap) != id(
        filter2.bitmap
    )
    assert id(filter1) != id(filter) and id(filter2) != id(filter)


def test_bloom_filter_in_real_scenario():
    func_dict = {Murmur32HashFunction: 5, MD5HashFunction: 5, SHA1HashFunction: 5}
    epoch, cnt, vis = 100000, 0, set()
    filter = StringBloomFilter(func_dict, size_estimate=epoch)
    for _ in range(epoch):
        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=20)
        )
        if (random_string in filter) != (random_string in vis):
            cnt += 1
        filter.add(random_string)
        vis.add(random_string)
    assert cnt <= 100


def _test_case_package_root():
    frame = inspect.currentframe()
    while frame:
        name = frame.f_code.co_name
        if name.startswith("test_"):
            path = os.path.join(package_root, name)
            return path
        frame = frame.f_back
    assert Exception("Test case package path is not found")


def _clean_up():
    frame = inspect.currentframe()
    while frame:
        name = frame.f_code.co_name
        if name.startswith("test_"):
            path = os.path.join(package_root, name)
            if os.path.exists(path):
                # remove directory temporary
                shutil.rmtree(path)
            os.mkdir(path)
            break
        frame = frame.f_back
