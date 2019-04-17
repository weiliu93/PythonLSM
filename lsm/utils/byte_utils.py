from bitarray import bitarray


def byte_array_to_integer(byte_array):
    """convert byte_array into integer"""
    result = 0
    for b in byte_array:
        result = (result << 8) + int(b)
    return result


def byte_array_to_bitarray(byte_array):
    """byte_array to bitarray, from left to right. First length bit will be chose"""
    length = len(byte_array) * 8
    result = bitarray(length)
    result.setall(False)
    for index, b in enumerate(byte_array):
        int_value = int(b)
        for i in reversed(range(8)):
            if (int_value & (1 << i)) != 0:
                result[index * 8 + 7 - i] = True
    return result


def bitarray_to_byte_array(bit_array):
    assert isinstance(bit_array, bitarray)
    index, byte_array = 0, bytearray()
    while index < len(bit_array):
        start, end = index, min(index + 7, len(bit_array) - 1)
        # convert bits[start: end + 1] into byte
        int_value, highest_digit = 0, 7
        for i in range(start, end + 1, 1):
            int_value = int_value | (1 << highest_digit) if bit_array[i] else int_value
            highest_digit -= 1
        byte_array.append(int_value)
        index += 8
    return byte_array


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
