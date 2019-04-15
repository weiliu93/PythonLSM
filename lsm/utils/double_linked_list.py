class DoubleLinkedList(object):
    """standard double linked list"""

    class ListNode(object):
        __slots__ = ["prev", "next", "value"]

        def __init__(self, value, prev=None, next=None):
            self.value = value
            self.prev = prev
            self.next = next

    def __init__(self):
        self._head = self._tail = self.ListNode(None)
        self._node_set = set()

    def add_last(self, value):
        self._insert_after(self._tail, self.ListNode(value))

    def add_first(self, value):
        self._insert_after(self._head, self.ListNode(value))

    def peek_last(self):
        self._empty_check()
        return self._tail.value

    def peek_first(self):
        self._empty_check()
        return self._head.next.value

    def pop_first(self):
        self._empty_check()
        node = self._head.next
        self.remove(node)
        return node

    def pop_last(self):
        self._empty_check()
        node = self._tail
        self.remove(node)
        return node

    def remove(self, list_node):
        if list_node in self._node_set:
            self._node_set.remove(list_node)
            self._remove(list_node)
            return True
        else:
            return False

    def _insert_after(self, previous_list_node, insert_list_node):
        insert_list_node.prev = insert_list_node.next = None
        if previous_list_node == self._tail:
            self._tail.next = insert_list_node
            self._tail.next.prev = self._tail
            self._tail = insert_list_node
        else:
            insert_list_node.prev = previous_list_node
            insert_list_node.next = previous_list_node.next
            insert_list_node.prev.next = insert_list_node
            insert_list_node.next.prev = insert_list_node

    def _remove(self, list_node):
        if list_node == self._tail:
            self._tail = self._tail.prev
            self._tail.next = None
        else:
            list_node.prev.next = list_node.next
            list_node.next.prev = list_node.prev

    def _empty_check(self):
        if len(self) == 0:
            raise Exception("double linked list is empty")

    def __len__(self):
        return len(self._node_set)

    def __iter__(self):
        node = self._head.next
        while node:
            yield node
            node = node.next
