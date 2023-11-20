from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from miners.miner_back_test_base import MinerBackTestBase
from datetime import timedelta,datetime
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from storage.redshift_storage import RedshiftStorage
from storage.encap_database_storage import EncapDatabaseStorage
from common import config
from streams.stream_cfg import StreamCfg
from dateutil.relativedelta import relativedelta
from typing import Dict,List
import logging
import pandas as pd
import numpy as np
from pandas import DataFrame
from schemas.miner_unit import Node
from validator.validate import validate_nodes, validate_code,validate_process_per_symbol
import logging
import talib as ta
import inspect

class MinerBacktest(MinerBackTestBase):
    """
    Bollinger Bands
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg = [
        StreamCfg(signal_name='market_.stock_ohlc_days',
                  same_table_name=True,
                  timestep=timedelta(days=1,hours=0,minutes=0),
                  version='1',
                  timestamp_field='time',
                  symbol_field='symbol',
                  to_create=False,
                  stream_fields = [
                      ('volume', 'double precision'),
                      ('high', 'double precision'),
                      ('last_updated', 'bigint'),
                      ('low', 'double precision'),
                      ('time', 'timestamp without time zone'),
                      ('close', 'double precision'),
                      ('open', 'double precision'),
                      ('timestamp', 'bigint'),
                      ('_airbyte_emitted_at', 'timestamp with time zone'),
                      ('_airbyte_normalized_at', 'timestamp with time zone'),
                      ('_airbyte_unique_key', 'text'),
                      ('symbol', 'text'),
                      ('resolution', 'text'),
                      ('_airbyte_ab_id', 'character varying'),
                      ('_airbyte_stock_ohlc_days_hashid', 'text'),
                  ],
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='wifeed_bctc_ket_qua_kinh_doanh_doanh_nghiep_san_xuat',
                  same_table_name=True,
                  timestep=timedelta(days=1,hours=0,minutes=0),
                  version='1',
                  timestamp_field='indexed_timestamp_',
                  symbol_field='symbol_',
                  to_create=False,
                  stream_fields = [
                      ('quy', 'numeric'),
                      ('nam', 'numeric'),
                      ('doanhthubanhangvacungcapdichvu', 'numeric'),
                      ('cackhoangiamtrudoanhthu', 'numeric'),
                      ('doanhthuthuanvebanhangvacungcapdichvu', 'numeric'),
                      ('giavonhangban', 'numeric'),
                      ('loinhuangopvebanhangvacungcapdichvu', 'numeric'),
                      ('doanhthuhoatdongtaichinh', 'numeric'),
                      ('chiphitaichinh', 'numeric'),
                      ('trongdochiphilaivay', 'numeric'),
                      ('phanlailohoaclotrongcongtyliendoanhlienket', 'numeric'),
                      ('chiphibanhang', 'numeric'),
                      ('chiphiquanlydoanhnghiep', 'numeric'),
                      ('thunhapkhac', 'numeric'),
                      ('chiphikhac', 'numeric'),
                      ('loinhuankhac', 'numeric'),
                      ('tongloinhuanketoantruocthue', 'numeric'),
                      ('chiphithuetndnhienhanh', 'numeric'),
                      ('chiphithuetndnhoanlai', 'numeric'),
                      ('loinhuansauthuethunhapdoanhnghiep', 'numeric'),
                      ('loiichcuacodongthieuso_bctn', 'numeric'),
                      ('loinhuansauthuecuacongtyme', 'numeric'),
                      ('laicobantrencophieu', 'numeric'),
                      ('laisuygiamtrencophieu', 'numeric'),
                      ('indexed_timestamp_', 'timestamp without time zone'),
                      ('code', 'text'),
                      ('type', 'text'),
                      ('symbol_', 'text'),
                      ('donvikiemtoan', 'text'),
                      ('ykienkiemtoan', 'text'),
                      ('loinhuanthuantuhoatdongkinhdoanh', 'numeric'),
                  ],
                  storage_backend=DatabaseStorage),
    ]


    def __init__(self, target_symbols=['NAV', 'HT1', 'HAP', 'SVC', 'LHC', 'LAF', 'AMV', 'CII', 'BCE', 'IMP', 'PVD', 'DMC', 'BBC', 'TCT', 'FPT', 'SSC', 'TSC', 'CAN', 'TS4', 'SDN', 'HBC', 'SFN', 'PMS', 'TYA', 'KDC', 'PGC', 'PVT', 'SAV', 'ITA', 'GMC', 'HSG', 'SMC', 'ANV', 'MHC', 'GMD', 'TDH', 'COM', 'SPM', 'VNM', 'TMS', 'REE', 'TRC', 'SGC', 'HRC', 'HAS', 'TCR', 'VC6', 'SAM', 'GIL', 'VHG', 'BMP', 'ASP', 'CMG']):
        input_streams = self.init_input_streams(self.input_cfg)
        ############################
        # get_target_symbols 
        ############################
        first_input_stream=input_streams[self.input_cfg[0].signal_name]
        if target_symbols is None or len(target_symbols) == 0:
            list_symbol = list(
                first_input_stream.backend.get_distinct_symbol(first_input_stream.symbol_field)[
                    first_input_stream.symbol_field
                ]
            )
            target_symbols = list_symbol

        super().__init__(target_symbols, input_streams)

    @validate_nodes
    @validate_code
    def get_inputs(self, timestamp:datetime,code:str)->Dict[str,Node]:
        try:
            result: Dict[str, Node]={}
            local_params={'result':result,'timestamp':timestamp, 'input_streams':self.input_streams, 'target_symbols':self.target_symbols}
            # user_code="def get_inputs(timestamp: datetime, target_symbols: List[str], input_streams: Dict[str, DataStreamBase]) -> List[Node]:\n    stock_price_data_300 = input_streams['market_.stock_ohlc_days'].get_record_range(\n        included_min_timestamp=timestamp + relativedelta(days=-300),\n        included_max_timestamp=datetime.strftime(\n            timestamp, '%Y-%m-%d 23:59:59'),\n        target_symbols=target_symbols,\n        filter_query=None\n    )\n    stock_price_data_100 = input_streams['market_.stock_ohlc_days'].get_record_range(\n        included_min_timestamp=timestamp + relativedelta(days=-100),\n        included_max_timestamp=datetime.strftime(\n            timestamp, '%Y-%m-%d 23:59:59'),\n        target_symbols=target_symbols,\n        filter_query=None\n    )\n    bctc_ket_qua_kinh_doanh_dnsx_timestamp = datetime(\n        timestamp.year, 1, 1)\n    bctc_ket_qua_kinh_doanh_dnsx = input_streams['wifeed_bctc_ket_qua_kinh_doanh_doanh_nghiep_san_xuat'].get_record(\n        indexed_timestamp=bctc_ket_qua_kinh_doanh_dnsx_timestamp,\n        target_symbols=target_symbols,\n        filter_query=None,\n    )\n    stock_price_data_100['symbol_'] = stock_price_data_100['symbol']\n    stock_price_data_100['indexed_timestamp_'] = stock_price_data_100['timestamp']\n    stock_price_data_300['symbol_'] = stock_price_data_300['symbol']\n    stock_price_data_300['indexed_timestamp_'] = stock_price_data_300['timestamp']\n    return [Node(name=\"stock_price_data_100\", dataframe=stock_price_data_100),\n            Node(name=\"stock_price_data_300\",\n                 dataframe=stock_price_data_300),\n            Node(name=\"bctc_ket_qua_kinh_doanh_dnsx\", dataframe=bctc_ket_qua_kinh_doanh_dnsx)]"
            user_code=code
            final_code=f"{user_code}\nnodes = get_inputs(timestamp=timestamp, input_streams=input_streams, target_symbols=target_symbols)\nresult: Dict[str, Node] = {{node.name: node for node in nodes}}"
            exec(final_code,globals(),local_params)
            return local_params['result']

        except Exception as e:
            logging.error(f"error catch {str(e)}")
            raise e

            
        
    
    @validate_process_per_symbol
    @validate_code
    def process_per_symbol(self, inputs:Dict[str, Node], symbol:str, timestamp)->Node:
        pass