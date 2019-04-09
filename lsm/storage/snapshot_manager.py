class SnapshotManager(object):
    """maintain all active snapshots"""

    class ListNode(object):
        def __init__(self, prev=None, next=None, snapshot=-1):
            self.prev = prev
            self.next = next
            self.snapshot = snapshot

    def __init__(self, max_snapshot_num=100):
        self._head = self.ListNode()
        self._tail = self._head
        self._size = 0
        self._max_snapshot_num = max_snapshot_num

    def __iter__(self):
        node = self._head.next
        while node:
            yield node
            node = node.next

    def append(self, snapshot):
        list_node = self.ListNode(snapshot=snapshot)
        self._append_list_node(list_node)
        self._size += 1
        if self._size > self._max_snapshot_num:
            self._remove_list_node(self._head.next)

    def _append_list_node(self, list_node):
        self._tail.next = list_node
        list_node.prev = self._tail
        self._tail = list_node

    def _remove_list_node(self, list_node):
        if list_node == self._tail:
            self._tail = self._tail.prev
            self._tail.next = None
        else:
            list_node.prev.next = list_node.next
            list_node.next.prev = list_node.prev
