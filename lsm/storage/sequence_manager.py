import threading


class SequenceManager(object):
    """trivial solution..."""

    def __init__(self, initial_id=0):
        self._id = max(initial_id, 0)
        self._lock = threading.Lock()

    @property
    def current(self):
        """latest sequence number"""
        self._lock.acquire()
        ans = self._id
        self._lock.release()
        return ans

    def __next__(self):
        """move to next sequence number"""
        self._lock.acquire()
        self._id += 1
        ans = self._id
        self._lock.release()
        return ans
