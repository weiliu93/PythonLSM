import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)
from logger.log_util import logger
from storage.internal_key_value import InternalKeyValue


class WriteBatch(object):
    """batch writing to storage"""

    def __init__(self, update_operations, sequence_number):
        # all write operations share same sequence_number
        self._batch = []
        for key, value, type in update_operations:
            # batch element is InternalKeyValue
            self._batch.append(InternalKeyValue(key, sequence_number, type, value))
        logger.debug(
            "Batch construction succeeded",
            sequence_number=sequence_number,
            length=len(self._batch),
        )

    def __iter__(self):
        for pair in self._batch:
            yield pair

    def __str__(self):
        return "(" + ", ".join(list(map(str, self._batch))) + ")"
