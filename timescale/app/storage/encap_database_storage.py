from pandas import DataFrame

from .storage_base import StorageBase
from common.utils import *
from common import config
from streams.stream_cfg import StreamCfg
from typing import Union, List


class EncapDatabaseStorage(StorageBase):

    def __init__(self, stream_config: Union[dict, StreamCfg]):
        self.backend = EncapPSQLHook()

        # legacy compatibility
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

    def get_record(self, indexed_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, target_symbols=None, filter_query=None):
        return self.backend.get_record(self.table_name, indexed_timestamp, symbol_column, target_symbols, filter_query)

    def get_record_v2(self, indexed_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, timestamp_column=config.SYSTEM_TIMESTAMP_COL, target_symbols=None, filter_query=None):
        raise NotImplementedError("Not supported at the moment!")

    def get_record_range(self, included_min_timestamp, included_max_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, target_symbols=None, filter_query=None, ):
        return self.backend.get_record_range(self.table_name, included_min_timestamp, included_max_timestamp, symbol_column, target_symbols, filter_query)

    def get_record_range_v2(self, included_min_timestamp, included_max_timestamp, symbol_column=config.SYSTEM_SYMBOL_COL, timestamp_column=config.SYSTEM_TIMESTAMP_COL, target_symbols=None, filter_query=None, ):
        raise NotImplementedError("Not supported at the moment!")

    def get_distinct_symbol(self, symbol_column):
        return self.backend.get_distinct_symbol(table_name=self.table_name, symbol_column=symbol_column)

    def get_latest_timestamp(self):
        return self.backend.get_latest_timestamp(self.table_name)

    def find(self, identified_name):
        return

    def get_distinct_symbol_exclude_future(self, symbol_column):
        return self.backend.get_distinct_symbol_exclude_future(symbol_column, self.table_name)
