import random
import threading
from collections import deque
import os
import sys


sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)

from logger.log_util import logger


class SkipList(object):
    """naive version concurrent SkipList, basic functionality"""

    def __init__(self, iterable=None, global_lock=False):
        iterable = iterable if iterable else []
        assert (
            iter(iterable) != iterable
        ), "Iterable collection should be provided, not iterable or other illegal data types"
        iterable = iterable if iterable else []
        self._build_from_iterable(iterable)
        if global_lock:
            self._global_lock = threading.Lock()
        else:
            self._global_lock = DummyLock()

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

    def ceiling(self, key):
        self._global_lock.acquire()
        current = self._heads[-1]
        while current:
            while current.right and current.right.key < key:
                current = current.right
            # we found the key
            if current.right and current.right.key == key:
                result = current.right
                self._global_lock.release()
                return result.key, result.value if result else None
            else:
                # if we can jump to the next layer
                if current.down:
                    current = current.down
                else:
                    # In bottom layer, current.right is what we want
                    result = current.right if current.right else None
                    self._global_lock.release()
                    return (result.key, result.value) if result else None
        logger.error("ceiling code couldn't reach here", key=key)

    def floor(self, key):
        self._global_lock.acquire()
        current, is_head = self._heads[-1], True
        while current:
            while current.right and current.right.key <= key:
                current = current.right
                is_head = False
            # if we can jump to the next layer
            if current.down:
                current = current.down
            else:
                # it is head node, it means floor found nothing
                result = current if not is_head else None
                self._global_lock.release()
                return (result.key, result.value) if result else None
        logger.error("floor code couldn't reach here", key=key)

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
        if key_value_pairs:
            self._size = len(keys)
            self._heads = []
            # build SkipList layer by layer, it should be a BST structure
            key_to_last_node = {}
            queue = deque()
            queue.append([0, len(key_value_pairs) - 1])
            while queue:
                next_layer = []
                self._heads.append(SkipListNode(-1, -1))
                current_head = self._heads[-1]
                # link head nodes together in next linkedlist
                if len(self._heads) > 1:
                    self._heads[-2].down = self._heads[-1]
                # has_next_layer means we have intervals need to be split in next layer
                # otherwise current layer is the bottom layer
                has_next_layer = False
                while queue:
                    element = queue.popleft()
                    # interval
                    if isinstance(element, list):
                        # split interval into [interval1] + mid_element + [interval2],
                        # interval1 and interval2 could be empty
                        low, high = element[0], element[1]
                        mid = low + (high - low) // 2
                        new_node = SkipListNode(
                            key_value_pairs[mid][0], key_value_pairs[mid][1]
                        )
                        current_head.right = new_node
                        current_head = current_head.right
                        if mid > low:
                            next_layer.append([low, mid - 1])
                            has_next_layer = True
                        next_layer.append(key_value_pairs[mid])
                        if mid < high:
                            next_layer.append([mid + 1, high])
                            has_next_layer = True
                        if new_node.key in key_to_last_node:
                            key_to_last_node[new_node.key].down = new_node
                        key_to_last_node[new_node.key] = new_node
                    # key-value tuple
                    elif isinstance(element, tuple):
                        new_node = SkipListNode(element[0], element[1])
                        current_head.right = new_node
                        current_head = current_head.right
                        # just sink current element to next layer, since it should
                        # exists in all layers below
                        next_layer.append(element)
                        if new_node.key in key_to_last_node:
                            key_to_last_node[new_node.key].down = new_node
                        key_to_last_node[new_node.key] = new_node
                    else:
                        raise Exception("Illegal data type in SkipList construction")
                # need to break here, since queue need to be empty for outer check
                if not has_next_layer:
                    break
                queue.extend(next_layer)
            self._heads.reverse()
        else:
            # iterable is empty, add a dummy head in first layer
            self._heads = [SkipListNode(-1, -1)]
            self._size = 0

    def __len__(self):
        return self.size()

    def __iter__(self):
        return self.keys()

    def __str__(self):
        key_value_pairs = list(self.items())
        return "{" + (", ".join(map(str, key_value_pairs))) + "}"


class DummyLock(object):
    """dummy lock, won't do anything, just use same interfaces"""

    def acquire(self):
        pass

    def release(self):
        pass


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
