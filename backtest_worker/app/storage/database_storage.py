from pandas import DataFrame
from sqlalchemy.orm import Session

from .storage_base import StorageBase
from common.utils import *
from common import config
from streams.stream_cfg import StreamCfg
from typing import Dict, Iterator, List, Union
from backend.session import create_session
from services.timescale_service import TimescaleService



class DatabaseStorage(StorageBase):

    def __init__(self, stream_config: Union[dict, StreamCfg]):
        session: Iterator[Session] = create_session()
        self.backend = TimescaleService(next(session))
        if isinstance(stream_config, dict):
            if 'same_table_name' not in stream_config:
                stream_config['same_table_name'] = False
            stream_config = StreamCfg(**stream_config)

        assert isinstance(stream_config, StreamCfg)

        if stream_config.same_table_name:
            self.table_name = stream_config.signal_name
        else:
            self.table_name = generated_identified_name(stream_config.signal_name, stream_config.timestep,
                                                        stream_config.version)

        self.name_fields = [item[0] for item in stream_config.stream_fields]

        if config.SYSTEM_TIMESTAMP_COL not in self.name_fields:
            self.name_fields.append(config.SYSTEM_TIMESTAMP_COL)
        if config.SYSTEM_SYMBOL_COL not in self.name_fields:
            self.name_fields.append(config.SYSTEM_SYMBOL_COL)
        super().__init__()

    def append(self, df_record: DataFrame, commit_every=1000):
        logging.info(f"Commit sorage : {commit_every}")
        self.backend.append(self.table_name, df_record,
                            commit_every=commit_every)
        return

    # def get_record(self, indexed_timestamp, symbol_column, target_symbols=None, filter_query=None):
    #     return self.backend.get_record(self.table_name, indexed_timestamp, symbol_column, target_symbols, filter_query)

    def get_record(
        self,
        indexed_timestamp: str,
        symbol_column: str = "indexed_timestamp_",
        timestamp_column: str = "symbol_",
        target_symbols: List = None,
        filter_query: str = None
    ):
        return self.backend.get_record(
            table_name=self.table_name,
            indexed_timestamp=indexed_timestamp,
            symbol_column=symbol_column,
            timestamp_column=timestamp_column,
            target_symbols=target_symbols, filter_query=filter_query
        )

    # def get_record_range(self, included_min_timestamp, included_max_timestamp, symbol_column, target_symbols=None, filter_query=None, ):
    #     return self.backend.get_record_range(self.table_name, included_min_timestamp, included_max_timestamp, symbol_column, target_symbols, filter_query)

    def get_record_range(
        self,
        included_min_timestamp: str,
        included_max_timestamp: str,
        symbol_column: str = config.SYSTEM_SYMBOL_COL,
        timestamp_column: str = config.SYSTEM_TIMESTAMP_COL,
        target_symbols: List = None,
        filter_query: str = None
    ) -> pd.DataFrame:
        return self.backend.get_record_range(
            table_name=self.table_name,
            included_min_timestamp=included_min_timestamp,
            included_max_timestamp=included_max_timestamp,
            symbol_column=symbol_column,
            timestamp_column=timestamp_column,
            target_symbols=target_symbols,
            filter_query=filter_query
        )

    # def get_distinct_symbol(self, symbol_column):
    #     return self.backend.get_distinct_symbol(symbol_column, self.table_name)

    # def get_distinct_symbol_exclude_future(self, symbol_column):
    #     return self.backend.get_distinct_symbol_exclude_future(symbol_column, self.table_name)

    # def get_latest_timestamp(self):
    #     return self.backend.get_latest_timestamp(self.table_name)

    # def find(self, identified_name):
    #     return
