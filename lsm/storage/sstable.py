import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from storage.internal_key_value import InternalKeyValue


class SSTable(object):
    """
    DataBlocks + FilterBlocks + IndexBlocks
    """

    # TODO more design

    pass
