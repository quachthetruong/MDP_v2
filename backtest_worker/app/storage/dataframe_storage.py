import pandas as pd 
import numpy as np 
from .storage_base import StorageBase
from common import config


class DataFrameStorage(StorageBase):
    
    #def __init__(self, stream_cfg, storage_folder='/tmp/enmdp'):
    #    columns = [(config.SYSTEM_TIMESTAMP_COL, np.datetime64)] + stream_cfg['stream_fields']
    #    self.backend = pd.DataFrame(np.empty(0,np.dtype(columns)))
    #    self.storage_folder = storage_folder
    
    def __init__(self, df: pd.DataFrame):
        self.backend = df
    
    def append(self, record):
        #self.backend = pd.concat([self.backend, record.to_frame().T])
        self.backend = pd.concat([self.backend, record])
        
    def get_record(self, indexed_timestamp, symbol_column, target_symbols, filter_query=None):
        if target_symbols is None:
            tmp = self.backend
        else:
            tmp = self.backend.query('{} in {}'.format(symbol_column, target_symbols))
            
        if filter_query is None:
            return tmp[self.backend[config.SYSTEM_TIMESTAMP_COL]==indexed_timestamp]
        else:
            return tmp.query("{}=='{}' and({})".format(config.SYSTEM_TIMESTAMP_COL, indexed_timestamp, filter_query))
    
    def get_record_range(self, included_min_timestamp, included_max_timestamp, symbol_column, target_symbols, filter_query=None):
        if target_symbols is None:
            tmp = self.backend
        else:
            tmp = self.backend.query('{} in {}'.format(symbol_column, target_symbols))

        if filter_query is None:
            return tmp[(self.backend[config.SYSTEM_TIMESTAMP_COL]>=included_min_timestamp) & (self.backend[config.SYSTEM_TIMESTAMP_COL]<=included_max_timestamp)]    
        else:
            return tmp.query("{}>='{}' and {} <= '{}' and({})".format(symbol_column, included_min_timestamp, config.SYSTEM_TIMESTAMP_COL, included_max_timestamp, filter_query))
        
    def get_latest_timestamp(self):
        return self.backend.iloc[-1][config.SYSTEM_TIMESTAMP_COL]