from abc import abstractmethod
from enum import Enum
import config


class StorageBase:
    SYSTEM_TIMESTAMP_COL = 'indexed_timestamp_'
    SYSTEM_SYMBOL_COL = 'symbol_'

    def __init__(self):
        pass

class DatabaseStorage(StorageBase):
    '''this is a DatabaseStorage'''

class RedshiftStorage(StorageBase):
    '''this is a RedshiftStorage'''

class EncapDatabaseStorage(StorageBase):
    '''this is a EncapDatabaseStorage'''

class KafkaStorage(StorageBase):
    '''this is a KafkaStorage'''

class StorageType(Enum):
    # storage = {"DatabaseStorage":DatabaseStorage,"EncapDatabaseStorage":EncapDatabaseStorage,"RedshiftStorage":RedshiftStorage}
    DatabaseStorage = DatabaseStorage
    EncapDatabaseStorage = EncapDatabaseStorage
    RedshiftStorage = RedshiftStorage
    KafkaStorage = KafkaStorage
