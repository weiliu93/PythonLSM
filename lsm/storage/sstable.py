import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from storage.internal_key_value import InternalKeyValue
from utils.bloomfilter import BytesBloomFilter


class SSTable(object):
    """SSTableMetadata  +  SSTableBlockIndex  +  SSTableBlockFilter  +  SSTableDataBlock"""

    def __init__(self):
        # TODO keep working

        pass
