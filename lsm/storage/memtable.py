from lsm.utils.skiplist import SkipList


class Memtable(object):

    # TODO keep designing

    def __init__(self, sequence_manager):
        self._skiplist = SkipList()
        self._sequence_manager = sequence_manager

    def put(self, key, value):

        pass

    def get(self, key, sequence_number=None):

        pass

    def remove(self, key):

        pass

    def immutable(self):

        pass

    def _batch_put(self, write_batch):

        pass

    def _group_write_into_batch(self):

        pass
