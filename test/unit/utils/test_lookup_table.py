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


def test_initialize_lookup_table_with_given_key_offset_pairs():
    test_case_package = _test_case_package_root()
    _clean_up()

    test_file_path = os.path.abspath(os.path.join(test_case_package, "bootstrap"))
    data = [("hello", 10), ("morning", 46), ("ready", 100), ("year", 190)]
    table = LookupTable(test_file_path, data)

    assert table.ceiling("a") == ("hello", 10) and table.floor("a") == None
    assert table.ceiling("morninh") == ("ready", 100) and table.floor("morninh") == (
        "morning",
        46,
    )
    assert table.ceiling("v") == ("year", 190) and table.floor("v") == ("ready", 100)
    assert table.ceiling("z") == None and table.floor("z") == ("year", 190)
    assert table.size == 4
    assert table.filepath == test_file_path

    _clean_up()


def test_initialize_lookup_table_with_exists_data_file():
    test_case_package = _test_case_package_root()
    _clean_up()

    test_file_path = os.path.abspath(os.path.join(test_case_package, "bootstrap"))
    with open(test_file_path, "w") as f:
        f.write("a,100\n")
        f.write("ef,125\n")
        f.write("nwe,235\n")

    table = LookupTable(test_file_path)
    assert table.ceiling("a") == ("a", 100)
    assert table.ceiling("eg") == ("nwe", 235)
    assert table.ceiling("nze") == None

    assert table.floor("z") == ("nwe", 235)
    assert table.floor("ea") == ("a", 100)

    assert table.size == 3
    assert table.filepath == test_file_path

    _clean_up()


def test_initialize_lookup_table_with_corrupted_data_file():
    test_case_package = _test_case_package_root()
    _clean_up()

    test_file_path = os.path.abspath(os.path.join(test_case_package, "bootstrap"))
    with open(test_file_path, "w") as f:
        f.write("a\n")
        f.write("sad,231\n")
        f.write("ads,62\n")
        f.write("sd,bsd\n")
        f.write("zsdf,532\n")

    table = LookupTable(test_file_path)
    assert table.size == 3
    assert table.filepath == test_file_path
    assert table.ceiling("a") == ("ads", 62)
    assert table.ceiling("saf") == ("zsdf", 532)
    assert table.ceiling("zz") == None
    assert table.floor("e") == ("ads", 62)
    assert table.floor("sad") == ("sad", 231)
    assert table.floor("zz") == ("zsdf", 532)

    _clean_up()


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
