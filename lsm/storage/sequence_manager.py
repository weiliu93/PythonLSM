import threading


class SequenceManager(object):
    """trivial solution..."""

    def __init__(self):
        self._id = 0
        self._lock = threading.Lock()

    @property
    def current(self):
        self._lock.lock()
        ans = self._id
        self._lock.release()
        return ans

    def __next__(self):
        self._lock.acquire()
        ans = self._id
        self._id += 1
        self._lock.release()
        return ans
