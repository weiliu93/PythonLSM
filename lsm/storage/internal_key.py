class InternalKey(object):
    def __init__(self, key, sequence_number, type):
        assert key is not None
        assert isinstance(sequence_number, int) and sequence_number >= 0

        self.key = key
        self.sequence_number = sequence_number
        self.type = type

    def __lt__(self, other):
        return (self.key, self.sequence_number) < (other.key, other.sequence_number)

    def __le__(self, other):
        return (self.key, self.sequence_number) <= (other.key, other.sequence_number)

    def __gt__(self, other):
        return (self.key, self.sequence_number) > (other.key, other.sequence_number)

    def __ge__(self, other):
        return (self.key, self.sequence_number) >= (other.key, other.sequence_number)

    def __eq__(self, other):
        return (self.key, self.sequence_number) == (other.key, other.sequence_number)

    def __hash__(self):
        result = 31
        result = result * 37 + hash(self.key)
        result = result * 37 + self.sequence_number
        return result

    def __str__(self):
        return "(key: {}, sequence_number: {}, type: {})".format(
            self.key, self.sequence_number, self.type
        )


class KeyType(object):
    PUT = 0
    DELETE = 1
