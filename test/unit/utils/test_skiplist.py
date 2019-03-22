import sys
import os
import inspect
import shutil
import random
import concurrent.futures


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
        "skiplist",
    )
)

from skiplist import SkipList


def test_basic_skiplist_put_and_get():
    l = SkipList()

    l.put(10, 20)
    l.put(1, 100)
    l.put(7, 200)

    assert l.get(1) == 100 and l.get(10) == 20 and l.get(7) == 200

    for _ in range(10):
        value = random.randint(1, 10)
        if value in [1, 7, 10]:
            assert l.get(value) is not None
        else:
            assert l.get(value) is None


def test_advanced_skiplist_put_and_get():
    comp_dict = {}
    l = SkipList()
    for _ in range(10000):
        if random.random() <= 0.7:
            key, value = random.randint(1, 100), random.randint(1, 10000)
            l.put(key, value)
            comp_dict[key] = value
        else:
            key = random.randint(1, 100)
            assert l.get(key) == comp_dict.get(key, None)


def test_skiplist_remove():
    l = SkipList({1: 2, 3: 4, 5: 6})

    assert l.remove(1) == True
    assert l.size() == 2

    assert l.remove(2) == False
    assert l.size() == 2

    assert l.remove(5) == True
    assert l.size() == 1

    assert l.remove(5) == False
    assert l.size() == 1

    assert list(l) == [3]
    assert list(l.items()) == [(3, 4)]


def test_skiplist_clear():
    l = SkipList()
    for _ in range(100):
        l.put(random.randint(1, 10), random.randint(1, 1000))
    l.clear()
    assert len(l) == 0 and list(l) == [] and list(l.items()) == []


def test_skiplist_size():
    l = SkipList()
    comp_dict = {}
    for _ in range(10000):
        key = random.randint(1, 100)
        value = random.randint(1, 100000)
        if random.random() <= 0.5:
            l.put(key, value)
            comp_dict[key] = value
        else:
            l.remove(key)
            comp_dict.pop(key, None)
        assert l.size() == len(comp_dict) and l.size() == len(l)


def test_skiplist_keys():
    l, keys = SkipList(), set()
    for _ in range(100):
        key, value = random.randint(1, 10), random.randint(1, 100)
        l.put(key, value)
        keys.add(key)
        assert list(l.keys()) == sorted(list(keys))


def test_skiplist_items():
    keys = [random.randint(1, 1000) for _ in range(100)]
    values = [random.randint(1, 1000) for _ in range(100)]
    pair_dict = {key: value for key, value in zip(keys, values)}

    pairs = []
    l = SkipList()
    for key, value in pair_dict.items():
        l.put(key, value)
        pairs.append((key, value))
        assert sorted(pairs) == list(l.items())


def test_skiplist_real_scenario():
    l = SkipList()
    comp_dict = {}
    # 4, 3, 2, 1
    ops = ["set", "get", "remove", "clear"]
    for _ in range(1000000):
        index = random.randint(1, 10)
        # set
        if index <= 4:
            key, value = random.randint(1, 200), random.randint(1, 10000)
            l.put(key, value)
            comp_dict[key] = value
        elif index <= 7:
            key = random.randint(1, 200)
            assert l.get(key) == comp_dict.get(key, None)
        elif index <= 9:
            # remove
            key = random.randint(1, 1000)
            if key in comp_dict:
                assert l.remove(key) == True
                comp_dict.pop(key, None)
            else:
                assert l.remove(key) == False
        else:
            # clear
            l.clear()
            comp_dict.clear()
        # check keys, items, size
        assert sorted(list(comp_dict.keys())) == list(l.keys())
        assert sorted(list(comp_dict.items())) == list(l.items())
        assert len(comp_dict) == len(l)


def test_skiplist_multi_thread_put():
    def thread_func(start_value, skiplist):
        for _ in range(50):
            skiplist.put(start_value, start_value)
            start_value += 1

    l = SkipList()
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for start_value in [1, 100, 1000, 10000, 100000, 1000000]:
            result = pool.submit(thread_func, start_value, l)
            result.result()
    assert len(l) == 300 and l.size() == 300

    l.clear()
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for start_value in [1, 2, 3, 4, 5, 6]:
            result = pool.submit(thread_func, start_value, l)
            result.result()
    assert len(l) == 55 and l.size() == 55
    assert list(l) == [value for value in range(1, 56)]


def test_skiplist_build_from_iterable():
    data_dict = {}
    for _ in range(1000):
        key, value = random.randint(1, 1000), random.randint(1, 1000)
        data_dict[key] = value

    l = SkipList(iterable =data_dict)
    assert len(l) == len(data_dict)
    assert list(l) == sorted(list(data_dict.keys()))


def test_skiplist_build_from_iterable_and_used_in_real_scenario():
    comp_dict = {}
    for _ in range(1000):
        key, value = random.randint(1, 1000) , random.randint(1, 10000)
        comp_dict[key] = value
    l = SkipList(comp_dict)
    # 4, 3, 2, 1
    ops = ["set", "get", "remove", "clear"]
    for _ in range(1000000):
        index = random.randint(1, 10)
        # set
        if index <= 4:
            key, value = random.randint(1, 1000), random.randint(1, 10000)
            l.put(key, value)
            comp_dict[key] = value
        elif index <= 7:
            key = random.randint(1, 1000)
            assert l.get(key) == comp_dict.get(key, None)
        elif index <= 9:
            # remove
            key = random.randint(1, 1000)
            if key in comp_dict:
                assert l.remove(key) == True
                comp_dict.pop(key, None)
            else:
                assert l.remove(key) == False
        else:
            # clear
            l.clear()
            comp_dict.clear()
        # check keys, items, size
        assert sorted(list(comp_dict.keys())) == list(l.keys())
        assert sorted(list(comp_dict.items())) == list(l.items())
        assert len(comp_dict) == len(l)


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
