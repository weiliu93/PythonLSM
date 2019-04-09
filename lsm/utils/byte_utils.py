def byte_array_to_integer(byte_array):
    """convert byte_array into integer"""
    result = 0
    for b in byte_array:
        result = (result << 8) + int(b)
    return result


def integer_to_four_bytes_array(value):
    """convert integer into four bytes array"""
    return integer_to_n_bytes_array(value, 4)


def integer_to_n_bytes_array(value, n):
    """convert integer into n bytes array"""
    n *= 8
    byte_array = bytearray()
    for i in range(n, 0, -8):
        byte_array.append(int((value >> (i - 8)) & ((1 << 8) - 1)))
    return bytes(byte_array)
