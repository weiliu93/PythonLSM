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
            "storage",
        )
    )
)


package_root = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        os.path.pardir,
        "packages",
        "storage",
        "key_value_pair",
    )
)


from key_value_pair import KeyValuePair, KeyValuePairHeader, KeyValuePairHeaderFlag


def test_key_value_pair_serialization():
    package_path = _test_case_package_root()
    _clean_up()

    pair = KeyValuePair("hello", "world", 24)
    byte_array = pair.to_byte_array()
    tmp_path = os.path.join(package_path, "tmp.txt")
    with open(tmp_path, "wb") as f:
        f.write(byte_array)

    another_pair = KeyValuePair.load_from_data_stream(open(tmp_path, "rb"))
    assert (
        another_pair.key == "hello"
        and another_pair.value == "world"
        and another_pair.header.flags == 24
    )

    _clean_up()


def test_key_value_pair_header_flag_check():
    pair = KeyValuePair("hey", "world", KeyValuePairHeaderFlag.KEY_DELETED)
    assert pair.header.is_deleted() == True


def test_key_value_pair_header_flag_update():
    pair = KeyValuePair("haha", "world")
    assert pair.header.is_deleted() == False
    pair.header.set_flag(KeyValuePairHeaderFlag.KEY_DELETED)
    assert pair.header.is_deleted() == True


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
