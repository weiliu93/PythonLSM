import sys
import os
import inspect
import shutil


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
        "lookup_table",
    )
)

from lookup_table import LookupTable


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
