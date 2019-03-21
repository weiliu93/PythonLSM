import os


class LookupTable(object):
    """key -> offset search table, read-only data structure"""

    def __init__(self, filepath, key_offset_pairs=None):
        """
        :param filepath:            disk file to persit in-memory lookup table
        :param key_offset_pairs:    client part should guarantee key_offset pairs are sorted
        """
        self._lookup_list = []
        self._filepath = os.path.abspath(filepath)
        # create a new file with key_offset_pairs
        if not os.path.exists(filepath):
            with open(filepath, "w") as f:
                key_offset_pairs = key_offset_pairs or []
                for key, offset in key_offset_pairs:
                    self._lookup_list.append((key, offset))
                    f.write("{},{}\n".format(key, offset))
        else:
            # check if lookup table is corrupted
            is_sorted, last_key, last_offset = True, None, None
            key_set = set()
            with open(filepath, "r") as f:
                for line in map(lambda line: line.strip(), f.readlines()):
                    try:
                        key, offset = line.split(",")[0], int(line.split(",")[1])
                        # if duplicate keys found, use first occurrence
                        if key not in key_set:
                            self._lookup_list.append((key, offset))
                            if last_key is not None and last_key > key:
                                is_sorted = False
                            if last_offset is not None and last_offset > offset:
                                is_sorted = False
                            last_key, last_offset = key, offset
                            key_set.add(key)
                    except:
                        # skip illegal line
                        pass
            # only sort lookup list when disk file is corrupted
            if not is_sorted:
                self._lookup_list.sort()

    @property
    def filepath(self):
        return self._filepath

    @property
    def size(self):
        return len(self._lookup_list)

    def ceiling(self, key):
        assert isinstance(key, str)
        low, high = 0, len(self._lookup_list)
        while low < high:
            mid = low + (high - low) // 2
            if self._lookup_list[mid][0] < key:
                low = mid + 1
            else:
                high = mid
        if low < len(self._lookup_list):
            # return a shallow copy, since both string and integer
            # are immutable, it won't have side effect
            return tuple(self._lookup_list[low])
        else:
            return None

    def floor(self, key):
        assert isinstance(key, str)
        low, high = 0, len(self._lookup_list)
        while low < high:
            mid = low + (high - low) // 2
            if self._lookup_list[mid][0] <= key:
                low = mid + 1
            else:
                high = mid
        high -= 1
        if high >= 0:
            return tuple(self._lookup_list[high])
        else:
            return None
