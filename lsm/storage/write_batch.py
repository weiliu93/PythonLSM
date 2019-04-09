from lsm.storage.internal_key_value import InternalKeyValue


class WriteBatch(object):
    """batch writing to storage"""

    def __init__(self, key_value_pairs, sequence_num):
        # all write operations share same sequence_num
        self._batch = []
        for key, value in key_value_pairs:
            self._batch.append(InternalKeyValue.put_key(key, value, sequence_num))

    def __iter__(self):
        for pair in self._batch:
            yield pair

    def __str__(self):
        return "(" + ", ".join(list(map(str, self._batch))) + ")"
