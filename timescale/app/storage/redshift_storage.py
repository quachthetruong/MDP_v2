from pandas import DataFrame

from storage.storage_base import StorageBase
from common.utils import *
from common import config
from streams.stream_cfg import StreamCfg
from typing import Union, List

from fastapi import Depends
from backend.session import create_session
from services.stream import StreamService


class RedshiftStorage(StorageBase):

    def __init__(self, stream_config: Union[dict, StreamCfg], session=Depends(create_session)):
        self.backend = StreamService(session=session)

        # legacy compatibility
        if isinstance(stream_config, dict):
            if 'same_table_name' not in stream_config:
                stream_config['same_table_name'] = False
            stream_config = StreamCfg(**stream_config)

        assert isinstance(stream_config, StreamCfg)
        assert stream_config.to_create == False, "Redshift storage does not support to_create=True"

        if stream_config.same_table_name:
            self.table_name = stream_config.signal_name
        else:
            self.table_name = generated_identified_name(
                stream_config.signal_name,
                stream_config.timestep,
                stream_config.version
            )

        super().__init__()

    def append(self, df_record: DataFrame, commit_every=1000):
        raise NotImplementedError("Not supported at the moment!")

    def get_record(
        self,
        indexed_timestamp,
        symbol_column=config.SYSTEM_SYMBOL_COL,
        target_symbols=None,
        filter_query=None
    ):
        raise NotImplementedError("Not supported at the moment!")

    def get_record_v2(
        self,
        indexed_timestamp: str,
        symbol_column: str = config.SYSTEM_SYMBOL_COL,
        timestamp_column: str = config.SYSTEM_TIMESTAMP_COL,
        target_symbols: List = None,
        filter_query: str = None
    ):
        return self.backend.get_record_v2(
            table_name=self.table_name,
            indexed_timestamp=indexed_timestamp,
            symbol_column=symbol_column,
            timestamp_column=timestamp_column,
            target_symbols=target_symbols, filter_query=filter_query
        )

    def get_record_range(
        self,
        included_min_timestamp: str,
        included_max_timestamp: str,
        symbol_column: str = config.SYSTEM_SYMBOL_COL,
        target_symbols: List = None,
        filter_query: str = None
    ):
        raise NotImplementedError("Not supported at the moment!")

    def get_record_range_v2(
        self,
        included_min_timestamp: str,
        included_max_timestamp: str,
        symbol_column: str = config.SYSTEM_SYMBOL_COL,
        timestamp_column: str = config.SYSTEM_TIMESTAMP_COL,
        target_symbols: List = None,
        filter_query: str = None
    ) -> DataFrame:
        return self.backend.get_record_range_v2(
            table_name=self.table_name,
            included_min_timestamp=included_min_timestamp,
            included_max_timestamp=included_max_timestamp,
            symbol_column=symbol_column,
            timestamp_column=timestamp_column,
            target_symbols=target_symbols,
            filter_query=filter_query
        )

    def get_distinct_symbol(self, symbol_column):
        return self.backend.get_distinct_symbol(table_name=self.table_name, symbol_column=symbol_column)

    def get_latest_timestamp(self):
        return self.backend.get_latest_timestamp(self.table_name)

    def find(self, identified_name):
        return
