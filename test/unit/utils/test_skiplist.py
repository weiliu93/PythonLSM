import sys
import os
import inspect
import shutil
import random

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


# TODO add more test cases for naive SkipList


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
