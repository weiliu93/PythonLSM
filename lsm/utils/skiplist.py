import random
import threading


class SkipList(object):
    """naive version concurrent SkipList, basic functionality"""

    def __init__(self, iterable=None):
        iterable = iterable if iterable else []
        assert (
            iter(iterable) != iterable
        ), "Iterable collection should be provided, not iterable or other illegal data types"
        iterable = iterable if iterable else []
        self._build_from_iterable(iterable)
        self._global_lock = threading.Lock()

    @property
    def layer(self):
        return len(self._heads)

    def put(self, key, value):
        self._global_lock.acquire()
        predecessors, current, find = [], self._heads[-1], False
        while current:
            while current.right and current.right.key < key:
                current = current.right
            if current.right and current.right.key == key:
                current.right.value = value
                find = True
            predecessors.append(current)
            current = current.down
        # if we need to insert new key-value pair
        if not find:
            predecessors.reverse()
            level = self._random_level()
            # add more headers when necessary
            while len(predecessors) < level + 1:
                new_head = SkipListNode(-1, -1, right=None, down=self._heads[-1])
                self._heads.append(new_head)
                predecessors.append(new_head)
            prev_insert_node = None
            for predecessor in predecessors[: level + 1]:
                new_node = SkipListNode(
                    key, value, right=predecessor.right, down=prev_insert_node
                )
                predecessor.right = new_node
                prev_insert_node = new_node
            self._size += 1
        self._global_lock.release()

    def remove(self, key):
        self._global_lock.acquire()
        predecessors = []
        current, find = self._heads[-1], False
        while current:
            while current.right and current.right.key < key:
                current = current.right
            if current.right and current.right.key == key:
                find = True
            predecessors.append(current)
            current = current.down
        if find:
            for predecessor in predecessors:
                if predecessor.right and predecessor.right.key == key:
                    predecessor.right = predecessor.right.right
                if not self._heads[-1].right and len(self._heads) > 1:
                    self._heads.pop()
            self._size -= 1
        self._global_lock.release()
        return find

    def get(self, key, default=None):
        self._global_lock.acquire()
        # start from top
        current = self._heads[-1]
        while current:
            while current.right and current.right.key < key:
                current = current.right
            if current.right and current.right.key == key:
                result = current.right.value
                self._global_lock.release()
                return result
            else:
                # jump to next row
                current = current.down
        self._global_lock.release()
        return default

    def keys(self):
        return map(lambda item: item[0], self.items())

    def items(self):
        self._global_lock.acquire()
        head = self._heads[0]
        while head.right:
            yield head.right.key, head.right.value
            head = head.right
        self._global_lock.release()

    def clear(self):
        self._global_lock.acquire()
        self._heads = [SkipListNode(-1, -1)]
        self._size = 0
        self._global_lock.release()

    def size(self, accurate=False):
        if accurate:
            self._global_lock.acquire()
            ans = self._size
            self._global_lock.release()
            return ans
        else:
            return self._size

    def _random_level(self):
        level = 0
        while random.random() <= 0.5:
            level += 1
        return level

    def _build_from_iterable(self, iterable):
        # prepare key-value pairs sorted in key
        if isinstance(iterable, dict):
            iterable_dict = {key: value for key, value in iterable.items()}
        else:
            iterable_dict = {key: value for key, value in iterable}
        keys = sorted(list(iterable_dict.keys()))
        key_value_pairs = [(key, iterable_dict[key]) for key in keys]
        self._size = len(keys)
        self._heads = []
        # build SkipList layer by layer
        key_to_last_node, layer = {}, 0
        while key_value_pairs:
            self._heads.append(
                SkipListNode(-1, -1, down=self._heads[-1] if self._heads else None)
            )
            head = self._heads[-1]
            for key, value in key_value_pairs:
                head.right = SkipListNode(
                    key, value, right=None, down=key_to_last_node.get(key, None)
                )
                head = head.right
                key_to_last_node[key] = head
            if len(key_value_pairs) == 1:
                break
            else:
                key_value_pairs = key_value_pairs[::2]
        # iterable is empty
        if not key_value_pairs:
            self._heads.append(SkipListNode(-1, -1))

    def __len__(self):
        return self.size()

    def __iter__(self):
        return self.keys()

    def __str__(self):
        key_value_pairs = list(self.items())
        return "{" + (", ".join(map(str, key_value_pairs))) + "}"


# TODO
class LockFreeSkipList(object):
    """better solution for concurrent SkipList"""

    pass


class SkipListNode(object):

    __slots__ = ["key", "value", "right", "down"]

    def __init__(self, key, value, *, right=None, down=None):
        self.key = key
        self.value = value
        self.right = right
        self.down = down
