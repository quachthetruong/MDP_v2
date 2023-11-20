from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from common.data_utils import DataUtils
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
from commons.logger import logger
import talib as ta

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
    def get_inputs(self, timestamp)->Dict[str,Node]:
        def get_inputs(timestamp: datetime, target_symbols: List[str], input_streams: Dict[str, DataStreamBase]) -> List[Node]:
            stock_price_data_300 = input_streams['market_.stock_ohlc_days'].get_record_range(
                included_min_timestamp=timestamp + relativedelta(days=-300),
                included_max_timestamp=datetime.strftime(
                    timestamp, '%Y-%m-%d 23:59:59'),
                target_symbols=target_symbols,
                filter_query=None
            )
            stock_price_data_100 = input_streams['market_.stock_ohlc_days'].get_record_range(
                included_min_timestamp=timestamp + relativedelta(days=-100),
                included_max_timestamp=datetime.strftime(
                    timestamp, '%Y-%m-%d 23:59:59'),
                target_symbols=target_symbols,
                filter_query=None
            )
            bctc_ket_qua_kinh_doanh_dnsx_timestamp = datetime(
                timestamp.year, 1, 1)
            bctc_ket_qua_kinh_doanh_dnsx = input_streams['wifeed_bctc_ket_qua_kinh_doanh_doanh_nghiep_san_xuat'].get_record(
                indexed_timestamp=bctc_ket_qua_kinh_doanh_dnsx_timestamp,
                target_symbols=target_symbols,
                filter_query=None,
            )
            stock_price_data_100['symbol_'] = stock_price_data_100['symbol']
            stock_price_data_100['indexed_timestamp_'] = stock_price_data_100['timestamp']
            stock_price_data_300['symbol_'] = stock_price_data_300['symbol']
            stock_price_data_300['indexed_timestamp_'] = stock_price_data_300['timestamp']
            return [Node(name="stock_price_data_100", dataframe=stock_price_data_100),
                    Node(name="stock_price_data_300",
                         dataframe=stock_price_data_300),
                    Node(name="bctc_ket_qua_kinh_doanh_dnsx", dataframe=bctc_ket_qua_kinh_doanh_dnsx)]
        nodes = get_inputs(timestamp=timestamp, input_streams=self.input_streams, target_symbols=self.target_symbols)
        result: Dict[str, Node] = {node.name: node for node in nodes}
        return result
        
    
    @validate_process_per_symbol
    @validate_code
    def process_per_symbol(self, inputs:Dict[str, Node], symbol:str, timestamp)->Node:
        def process_per_symbol(symbol: str, timestamp: datetime, inputs: Dict[str, Node]) -> Node:
            current_symbol_inputs, other_symbol_inputs = DataUtils.symbol_split(
            symbol=symbol, inputs=inputs)
            stock_price_data = current_symbol_inputs['stock_price_data_100']
            if len(stock_price_data) < 14:
                return Node(name="name_node", dataframe=pd.DataFrame([]))

            stock_price_data=stock_price_data.sort_values(by=['time'], ascending=True)

            # get 3 type price of symbol
            close_price = stock_price_data['close']
            low_price = stock_price_data['low']
            high_price = stock_price_data['high']

            # get new price
            nearest_close_price = high_price.iloc[-1]
            nearest_low_price = low_price.iloc[-1]
            nearest_high_price = high_price.iloc[-1]

            rsi = ta.RSI(close_price, timeperiod=14).iloc[-1]
            rsi_ytd = ta.RSI(close_price, timeperiod=14).iloc[-2]
            if rsi_ytd <= 30 and rsi > 30:
                rsi_tag = 'mua'
            elif rsi_ytd >= 70 and rsi < 70:
                rsi_tag = 'ban'
            elif rsi > 70:
                rsi_tag = 'qua_mua'
            elif rsi < 30:
                rsi_tag = 'qua_ban'
            elif 43 <= rsi <= 47:
                rsi_tag = 'trung_tinh'
            else:
                rsi_tag = 'trung_tinh'

            stochk = ta.STOCH(high=high_price, low=low_price, close=close_price, fastk_period=14,
                              slowk_period=3, slowk_matype=0, slowd_period=1, slowd_matype=0)[1].iloc[-1]
            stochk_ytd = ta.STOCH(high=high_price, low=low_price, close=close_price, fastk_period=14,
                                  slowk_period=3, slowk_matype=0, slowd_period=1, slowd_matype=0)[1].iloc[-1]
            if stochk < 20:
                stochk_tag = 'qua_ban'
            elif stochk > 80:
                stochk_tag = 'qua_mua'
            elif stochk_ytd >= 80 and stochk < 80:
                stochk_tag = 'ban'
            elif stochk_ytd <= 20 and stochk > 20:
                stochk_tag = 'mua'
            elif 44 <= stochk <= 55:
                stochk_tag = 'trung_tinh'
            else:
                stochk_tag = 'trung_tinh'
            stochrsi_fasttk = ta.STOCHRSI(
                close_price, timeperiod=14, fastk_period=3, fastd_period=3, fastd_matype=0)[0].iloc[-1]
            stochrsi_fasttk_ytd = ta.STOCHRSI(
                close_price, timeperiod=14, fastk_period=3, fastd_period=3, fastd_matype=0)[0].iloc[-2]
            if stochrsi_fasttk < 20:
                stochrsi_fasttk_tag = 'qua_ban'
            elif stochrsi_fasttk > 80:
                stochrsi_fasttk_tag = 'qua_mua'
            elif stochrsi_fasttk_ytd >= 80 and stochrsi_fasttk < 80:
                stochrsi_fasttk_tag = 'ban'
            elif stochrsi_fasttk_ytd <= 20 and stochrsi_fasttk > 20:
                stochrsi_fasttk_tag = 'mua'
            elif 44 <= stochrsi_fasttk <= 55:
                stochrsi_fasttk_tag = 'trung_tinh'
            else:
                stochrsi_fasttk_tag = 'trung_tinh'

            macd = ta.MACD(close_price, fastperiod=12,
                           slowperiod=26, signalperiod=9)[0].iloc[-1]
            macd_ytd = ta.MACD(close_price, fastperiod=12,
                               slowperiod=26, signalperiod=9)[0].iloc[-2]
            macd_signal = ta.MACD(close_price, fastperiod=12,
                                  slowperiod=26, signalperiod=9)[1].iloc[-1]
            macd_signal_ytd = ta.MACD(
                close_price, fastperiod=12, slowperiod=26, signalperiod=9)[1].iloc[-2]

            if macd_ytd <= macd_signal_ytd and macd > macd_signal:
                macd_tag = 'mua'
            elif macd_ytd >= macd_signal_ytd and macd < macd_signal:
                macd_tag = 'ban'
            else:
                macd_tag = 'trung_tinh'

            macd_histogram = ta.MACD(close_price, fastperiod=12,
                                     slowperiod=26, signalperiod=9)[2].iloc[-1]
            macd_histogram_tag = ''
            if macd_histogram > 0 and macd_ytd < macd:
                macd_histogram_tag = 'mua'
            if macd_histogram < 0 and macd_ytd > macd:
                macd_histogram_tag = 'ban'

            wpr = ta.WILLR(high_price, low_price, close_price, timeperiod=14).iloc[-1]
            wpr_ytd = ta.WILLR(high_price, low_price, close_price,
                               timeperiod=14).iloc[-2]
            if wpr < -80 and wpr_ytd < wpr:
                wpr_tag = 'mua'
            elif wpr > - 20 and wpr_ytd > wpr:
                wpr_tag = 'ban'
            elif wpr > -20:
                wpr_tag = 'qua_mua'
            elif wpr < -80:
                wpr_tag = 'qua_ban'
            elif -44 <= wpr <= 55:
                wpr_tag = 'trung_tinh'
            else:
                wpr_tag = 'trung_tinh'

            cci = ta.CCI(high_price, low_price, close_price, timeperiod=20).iloc[-1]
            if 0 < cci <= 100:
                cci_tag = 'mua'
            elif 0 > cci >= -100:
                cci_tag = 'ban'
            else:
                cci_tag = 'trung_tinh'

            roc = ta.ROC(close_price, timeperiod=14).iloc[-1]
            roc_ytd = ta.ROC(close_price, timeperiod=14).iloc[-2]
            if roc > 0 and roc_ytd < roc:
                roc_tag = 'mua'
            elif roc < 0 and roc_ytd > roc:
                roc_tag = 'ban'
            elif roc > 40:
                roc_tag = 'qua_mua'
            elif roc < 20:
                roc_tag = 'qua_ban'
            else:
                roc_tag = 'trung_tinh'

            ultosc = ta.ULTOSC(high_price, low_price, close_price, timeperiod1=7, timeperiod2=14,
                               timeperiod3=28).iloc[-1]
            ultosc_ytd = ta.ULTOSC(high_price, low_price, close_price, timeperiod1=7, timeperiod2=14,
                                   timeperiod3=28).iloc[-2]

            if ultosc_ytd <= 30 and ultosc_ytd < ultosc:
                ultosc_tag = 'mua'
            elif ultosc_ytd >= 70 and ultosc_ytd > ultosc:
                ultosc_tag = 'ban'
            elif 45 <= ultosc <= 55:
                ultosc_tag = 'trung_tinh'
            else:
                ultosc_tag = 'trung_tinh'
            adx = ta.ADX(high_price, low_price, close_price, timeperiod=14).iloc[-1]
            if adx > 20:
                adx_tag = 'mua'
            elif adx < 20:
                adx_tag = 'ban'
            else:
                adx_tag = 'trung_tinh'

            upper, middle, lower = ta.BBANDS(
                close_price, timeperiod=20, nbdevup=2, nbdevdn=2)
            bb_width = (upper.iloc[-1] - lower.iloc[-1]) / middle.iloc[-1]
            bb_width_ytd = (upper.iloc[-2] - lower.iloc[-2]) / middle.iloc[-2]
            if close_price.iloc[-2] < close_price.iloc[-1] and bb_width_ytd < bb_width:
                bb_width_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and bb_width_ytd > bb_width:
                bb_width_tag = 'ban'
            else:
                bb_width_tag = 'trung_tinh'

            sar = ta.SAR(high_price, low_price,
                         acceleration=0.02, maximum=0.2).iloc[-1]
            if sar < close_price.iloc[-1]:
                sar_tag = 'mua'
            elif sar > close_price.iloc[-1]:
                sar_tag = 'ban'
            else:
                sar_tag = 'trung_tinh'

            ma5 = np.mean(close_price.iloc[-5:])
            ma5_ytd = np.mean(close_price.iloc[-6:-2])
            if close_price.iloc[-2] < close_price.iloc[-1] and ma5 < close_price.iloc[-1] and close_price.iloc[-2] < ma5_ytd:
                ma5_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ma5 > close_price.iloc[-1] and close_price.iloc[-2] > ma5_ytd:
                ma5_tag = 'ban'
            else:
                ma5_tag = 'trung_tinh'

            ma10 = np.mean(close_price.iloc[-10:])
            ma10_ytd = np.mean(close_price.iloc[-11:-2])
            if close_price.iloc[-2] < close_price.iloc[-1] and ma10 < close_price.iloc[-1] and close_price.iloc[-2] < ma10_ytd:
                ma10_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ma10 > close_price.iloc[-1] and close_price.iloc[-2] > ma10_ytd:
                ma10_tag = 'ban'
            else:
                ma10_tag = 'trung_tinh'

            ma20 = np.mean(close_price.iloc[-20:])
            ma20_ytd = np.mean(close_price.iloc[-21:-2])
            if close_price.iloc[-2] < close_price.iloc[-1] and ma20 < close_price.iloc[-1] and close_price.iloc[-2] < ma20_ytd:
                ma20_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ma20 > close_price.iloc[-1] and close_price.iloc[-2] > ma20_ytd:
                ma20_tag = 'ban'
            else:
                ma20_tag = 'trung_tinh'

            ma50 = np.mean(close_price.iloc[-50:])
            ma50_ytd = np.mean(close_price.iloc[-51:-2])
            if close_price.iloc[-2] < close_price.iloc[-1] and ma50 < close_price.iloc[-1] and close_price.iloc[-2] < ma50_ytd:
                ma50_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ma20 > close_price.iloc[-1] and close_price.iloc[-2] > ma50_ytd:
                ma50_tag = 'ban'
            else:
                ma50_tag = 'trung_tinh'

            ma100 = np.mean(close_price.iloc[-100:])
            ma100_ytd = np.mean(close_price.iloc[-101:-2])
            if close_price.iloc[-2] < close_price.iloc[-1] and ma100 < close_price.iloc[-1] and close_price.iloc[-2] < ma100_ytd:
                ma100_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ma100 > close_price.iloc[-1] and close_price.iloc[-2] > ma100_ytd:
                ma100_tag = 'ban'
            else:
                ma100_tag = 'trung_tinh'

            ma200 = np.mean(close_price.iloc[-200:])
            ma200_ytd = np.mean(close_price.iloc[-201:-2])
            if close_price.iloc[-2] < close_price.iloc[-1] and ma200 < close_price.iloc[-1] and close_price.iloc[-2] < ma200_ytd:
                ma200_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ma200 > close_price.iloc[-1] and close_price.iloc[-2] > ma200_ytd:
                ma200_tag = 'ban'
            else:
                ma200_tag = 'trung_tinh'

            ema5 = close_price.ewm(span=5, adjust=False).mean().iloc[-1]
            ema5_ytd = close_price.iloc[:-1].ewm(span=5, adjust=False).mean().iloc[-1]
            if close_price.iloc[-2] < close_price.iloc[-1] and ema5 < close_price.iloc[-1] and close_price.iloc[-2] < ema5_ytd:
                ema5_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ema5 > close_price.iloc[-1] and close_price.iloc[-2] > ema5_ytd:
                ema5_tag = 'ban'
            else:
                ema5_tag = 'trung_tinh'

            ema10 = close_price.ewm(span=10, adjust=False).mean().iloc[-1]
            ema10_ytd = close_price.iloc[:-
                                         1].ewm(span=10, adjust=False).mean().iloc[-1]
            if close_price.iloc[-2] < close_price.iloc[-1] and ema10 < close_price.iloc[-1] and close_price.iloc[-2] < ema10_ytd:
                ema10_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ema10 > close_price.iloc[-1] and close_price.iloc[-2] > ema10_ytd:
                ema10_tag = 'ban'
            else:
                ema10_tag = 'trung_tinh'

            ema20 = close_price.ewm(span=20, adjust=False).mean().iloc[-1]
            ema20_ytd = close_price.iloc[:-
                                         1].ewm(span=20, adjust=False).mean().iloc[-1]
            if close_price.iloc[-2] < close_price.iloc[-1] and ema20 < close_price.iloc[-1] and close_price.iloc[-2] < ema20_ytd:
                ema20_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ema20 > close_price.iloc[-1] and close_price.iloc[-2] > ema20_ytd:
                ema20_tag = 'ban'
            else:
                ema20_tag = 'trung_tinh'

            ema50 = close_price.ewm(span=50, adjust=False).mean().iloc[-1]
            ema50_ytd = close_price.iloc[:-
                                         1].ewm(span=50, adjust=False).mean().iloc[-1]
            if close_price.iloc[-2] < close_price.iloc[-1] and ema50 < close_price.iloc[-1] and close_price.iloc[-2] < ema50_ytd:
                ema50_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ema50 > close_price.iloc[-1] and close_price.iloc[-2] > ema50_ytd:
                ema50_tag = 'ban'
            else:
                ema50_tag = 'trung_tinh'

            ema100 = close_price.ewm(span=100, adjust=False).mean().iloc[-1]
            ema100_ytd = close_price.iloc[:-
                                          1].ewm(span=100, adjust=False).mean().iloc[-1]
            if close_price.iloc[-2] < close_price.iloc[-1] and ema100 < close_price.iloc[-1] and close_price.iloc[-2] < ema100_ytd:
                ema100_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ema100 > close_price.iloc[-1] and close_price.iloc[-2] > ema100_ytd:
                ema100_tag = 'ban'
            else:
                ema100_tag = 'trung_tinh'

            ema200 = close_price.ewm(span=200, adjust=False).mean().iloc[-1]
            ema200_ytd = close_price.iloc[:-
                                          1].ewm(span=200, adjust=False).mean().iloc[-1]
            if close_price.iloc[-2] < close_price.iloc[-1] and ema200 < close_price.iloc[-1] and close_price.iloc[-2] < ema200_ytd:
                ema200_tag = 'mua'
            elif close_price.iloc[-2] > close_price.iloc[-1] and ema200 > close_price.iloc[-1] and close_price.iloc[-2] > ema200_ytd:
                ema200_tag = 'ban'
            else:
                ema200_tag = 'trung_tinh'

            out_dict = {}
            out_dict[config.SYSTEM_SYMBOL_COL] = symbol
            out_dict[config.SYSTEM_TIMESTAMP_COL] = timestamp
            out_dict['rsi'] = rsi
            out_dict['rsi_tag'] = rsi_tag
            out_dict['stochk'] = stochk
            out_dict['stochk_tag'] = stochk_tag
            out_dict['stochrsi_fasttk'] = stochrsi_fasttk
            out_dict['stochrsi_fasttk_tag'] = stochrsi_fasttk_tag
            out_dict['macd_histogram'] = macd_histogram
            out_dict['macd_histogram_tag'] = macd_histogram_tag
            out_dict['macd'] = macd
            out_dict['macd_tag'] = macd_tag
            out_dict['adx'] = adx
            out_dict['adx_tag'] = adx_tag
            out_dict['wpr'] = wpr
            out_dict['wpr_tag'] = wpr_tag
            out_dict['cci'] = cci
            out_dict['cci_tag'] = cci_tag
            out_dict['roc'] = roc
            out_dict['roc_tag'] = roc_tag
            out_dict['sar'] = sar
            out_dict['sar_tag'] = sar_tag
            out_dict['ultosc'] = ultosc
            out_dict['ultosc_tag'] = ultosc_tag
            out_dict['bb_width'] = bb_width
            out_dict['bb_width_tag'] = bb_width_tag

            out_dict['ma5'] = ma5
            out_dict['ma5_tag'] = ma5_tag
            out_dict['ma10'] = ma10
            out_dict['ma10_tag'] = ma10_tag
            out_dict['ma20'] = ma20
            out_dict['ma20_tag'] = ma20_tag
            out_dict['ma50'] = ma50
            out_dict['ma50_tag'] = ma50_tag
            out_dict['ma100'] = ma100
            out_dict['ma100_tag'] = ma100_tag
            out_dict['ma200'] = ma200
            out_dict['ma200_tag'] = ma200_tag

            out_dict['ema5'] = ema5
            out_dict['ema5_tag'] = ema5_tag

            out_dict['ema10'] = ema10
            out_dict['ema10_tag'] = ema10_tag

            out_dict['ema20'] = ema20
            out_dict['ema20_tag'] = ema20_tag

            out_dict['ema50'] = ema50
            out_dict['ema50_tag'] = ema50_tag

            out_dict['ema100'] = ema100
            out_dict['ema100_tag'] = ema100_tag

            out_dict['ema200'] = ema200
            out_dict['ema200_tag'] = ema200_tag

            pivot = (nearest_close_price + nearest_low_price + nearest_high_price) / 3
            # pivot classic
            out_dict['pivot_classic_s1'] = 2 * pivot - nearest_high_price
            out_dict['pivot_classic_r1'] = 2 * pivot - nearest_low_price
            out_dict['pivot_classic_s2'] = pivot - \
                (nearest_high_price - nearest_low_price)
            out_dict['pivot_classic_r2'] = pivot + \
                (nearest_high_price - nearest_low_price)
            out_dict['pivot_classic_s3'] = nearest_low_price - \
                2 * (nearest_high_price - pivot)
            out_dict['pivot_classic_r3'] = nearest_high_price + \
                2 * (pivot - nearest_low_price)

            # pivot_fibonacy
            out_dict['pivot_fibo_classic_s1'] = pivot - \
                0.382 * (nearest_high_price - nearest_low_price)
            out_dict['pivot_fibo_classic_r1'] = pivot + \
                0.382 * (nearest_high_price - nearest_low_price)
            out_dict['pivot_fibo_classic_s2'] = pivot - \
                0.618 * (nearest_high_price - nearest_low_price)
            out_dict['pivot_fibo_classic_r2'] = pivot + \
                0.618 * (nearest_high_price - nearest_low_price)
            out_dict['pivot_fibo_classic_s3'] = pivot - \
                (nearest_high_price - nearest_low_price)
            out_dict['pivot_fibo_classic_r3'] = pivot + \
                (nearest_high_price - nearest_low_price)

            # pivot Camarilla
            out_dict['pivot_camarilla_s1'] = nearest_close_price - \
                (nearest_high_price - nearest_low_price) * 1.0833 / 12
            out_dict['pivot_camarilla_r1'] = nearest_close_price + \
                (nearest_high_price - nearest_low_price) * 1.0833 / 12
            out_dict['pivot_camarilla_s2'] = nearest_close_price - \
                (nearest_high_price - nearest_low_price) * 1.1666 / 12
            out_dict['pivot_camarilla_r2'] = nearest_close_price + \
                (nearest_high_price - nearest_low_price) * 1.1666 / 12
            out_dict['pivot_camarilla_s3'] = nearest_close_price - \
                (nearest_high_price - nearest_low_price) * 1.25 / 12
            out_dict['pivot_camarilla_r3'] = nearest_close_price + \
                (nearest_high_price - nearest_low_price) * 1.25 / 12
            out_dict['pivot_camarilla_s4'] = nearest_close_price - \
                (nearest_high_price - nearest_low_price) * 1.5 / 12
            out_dict['pivot_camarilla_r4'] = nearest_close_price + \
                (nearest_high_price - nearest_low_price) * 1.5 / 12

            # woodie_pivot
            woodie_pivot = (nearest_high_price + nearest_low_price +
                            2 * nearest_close_price) / 4
            out_dict['woodie_pivot_s1'] = 2 * woodie_pivot - nearest_high_price
            out_dict['woodie_pivot_r1'] = 2 * woodie_pivot - nearest_low_price
            out_dict['woodie_pivot_s2'] = woodie_pivot - \
                (nearest_high_price - nearest_low_price)
            out_dict['woodie_pivot_r2'] = woodie_pivot + \
                (nearest_high_price - nearest_low_price)
            out_dict['woodie_pivot_s3'] = nearest_low_price - \
                2 * (nearest_high_price - woodie_pivot)
            out_dict['woodie_pivot_r3'] = nearest_high_price + \
                2 * (woodie_pivot - nearest_low_price)

            return Node(name="name_node", dataframe=pd.DataFrame.from_records([out_dict]))


        return process_per_symbol(inputs=inputs, symbol=symbol, timestamp=timestamp)
        