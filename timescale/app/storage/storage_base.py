from abc import abstractmethod
from common import config
import pandas as pd


class StorageBase:
    SYSTEM_TIMESTAMP_COL = 'indexed_timestamp_'
    SYSTEM_SYMBOL_COL = 'symbol_'

    def __init__(self):
        pass

    @abstractmethod
    def append(self, record, commit_every: int = 1000):
        raise NotImplementedError("Should implement this!")

    @abstractmethod
    def get_record(self, indexed_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, target_symbols=None, filter_query=None) -> pd.DataFrame:
        raise NotImplementedError("Should implement this!")

    # @abstractmethod
    # def get_record_v2(self, indexed_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, timestamp_column=config.SYSTEM_TIMESTAMP_COL, target_symbols=None, filter_query=None) -> pd.DataFrame:
    #     raise NotImplementedError("Should implement this!")

    @abstractmethod
    def get_record_range(self, included_min_timestamp, included_max_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, target_symbols=None, filter_query=None, ) -> pd.DataFrame:
        raise NotImplementedError("Should implement this!")

    # @abstractmethod
    # def get_record_range_v2(self, included_min_timestamp, included_max_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, timestamp_column=config.SYSTEM_TIMESTAMP_COL, target_symbols=None, filter_query=None, ) -> pd.DataFrame:
    #     raise NotImplementedError("Should implement this!")

    @abstractmethod
    def get_latest_timestamp(self):
        raise NotImplementedError("Should implement this!")

    @abstractmethod
    def get_distinct_symbol(self, symbol_column: str, table_name: str):
        raise NotImplementedError("Should implement this!")

    @classmethod
    @abstractmethod
    def find(cls, identified_name):
        """
        Args:
            identified_name: the name of the stream in the normalized format. E.g., MACD_1d_v1
        Returns:
            bool: True if the stream already existed in the storage
        """
        raise NotImplementedError("Should implement this!")
