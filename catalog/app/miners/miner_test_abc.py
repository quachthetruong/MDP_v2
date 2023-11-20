from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from miners.miner_base_v2 import MinerBaseV2
from datetime import timedelta,datetime
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from storage.mock_storage import MockStorage
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
from schemas.other import Node

class MinerTestAbc(MinerBaseV2):
    """
    test miner 2
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg:List[StreamCfg] = [
        StreamCfg(signal_name='wifeed_bctc_thuyet_minh_chung_khoan',
                  same_table_name=True,
                  timestep=timedelta(days=1,hours=0,minutes=0),
                  version=1,
                  timestamp_field='indexed_timestamp_',
                  symbol_field='symbol_',
                  to_create=False,
                  stream_fields = [
                      ('quy', 'numeric'),
                      ('nam', 'numeric'),
                      ('tm_taisantcniemyetdangkygdtaivsdcuactck', 'numeric'),
                      ('tm_taisantcdaluukytaivsdvachuagdcuactck', 'numeric'),
                      ('tm_taisantcchovecuactck', 'numeric'),
                      ('tm_taisantcchualuukytaivsdcuactck', 'numeric'),
                      ('tm_taisantcniemyetdangkygdtaivsd', 'numeric'),
                      ('tm_taisantcdaluukyvsdchuagdcuandt', 'numeric'),
                      ('tm_taisantcchovecuandt', 'numeric'),
                      ('tm_taisantcchualuukytaivsdcuandt', 'numeric'),
                      ('tm_tienguicuandt', 'numeric'),
                      ('tm_tienguivehoatdongmoigioick', 'numeric'),
                      ('tm_tienguicuandtvegdckctckql', 'numeric'),
                      ('tm_tiencuandtvegdcknhtmql', 'numeric'),
                      ('tm_tienguitonghopgdckcuandt', 'numeric'),
                      ('tm_tienguibutruthanhtoangdck', 'numeric'),
                      ('tm_tienguicuatochucphathanhck', 'numeric'),
                      ('tm_phaitrandtvetienguigdckctckql', 'numeric'),
                      ('tm_phaitrandttrongnuoctienguigdckctckql', 'numeric'),
                      ('tm_phaitrandtnuocngoaitienguigdckctckql', 'numeric'),
                      ('tm_phaitrandtvetienguigdcknhtmql', 'numeric'),
                      ('tm_phaitrandttrongnuoctienguigdcknhtmql', 'numeric'),
                      ('tm_phaitrandtnuocngoaitienguigdcknhtmql', 'numeric'),
                      ('tm_giatrighisotaisantcfvtpl', 'numeric'),
                      ('tm_cophieuniemyetfvtplghiso', 'numeric'),
                      ('tm_cophieuchuaniemyetfvtplghiso', 'numeric'),
                      ('tm_chungchiquyfvtplghiso', 'numeric'),
                      ('tm_traiphieufvtplghiso', 'numeric'),
                      ('tm_taisantckhacfvtplghiso', 'numeric'),
                      ('tm_giatrihoplytaisantcfvtpl', 'numeric'),
                      ('tm_cophieuniemyetfvtplhoply', 'numeric'),
                      ('tm_cophieuchuaniemyetfvtplhoply', 'numeric'),
                      ('tm_chungchiquyfvtplhoply', 'numeric'),
                      ('tm_traiphieufvtplhoply', 'numeric'),
                      ('tm_taisantckhacfvtplhoply', 'numeric'),
                      ('tm_giatrighisotaisantcafs', 'numeric'),
                      ('tm_cophieuniemyetafsghiso', 'numeric'),
                      ('tm_cophieuchuaniemyetafsghiso', 'numeric'),
                      ('tm_chungchiquyafsghiso', 'numeric'),
                      ('tm_traiphieuafsghiso', 'numeric'),
                      ('tm_taisantckhacafsghiso', 'numeric'),
                      ('tm_giatrihoplytaisantcafs', 'numeric'),
                      ('tm_cophieuniemyetafshoply', 'numeric'),
                      ('tm_cophieuchuaniemyetafshoply', 'numeric'),
                      ('tm_chungchiquyafshoply', 'numeric'),
                      ('tm_traiphieuafshoply', 'numeric'),
                      ('tm_taisantckhacafshoply', 'numeric'),
                      ('tm_giatrighisotaisantchtm', 'numeric'),
                      ('tm_traiphieuhtm', 'numeric'),
                      ('tm_congcuthitruongtientehtm', 'numeric'),
                      ('tm_taisantckhachtm', 'numeric'),
                      ('tm_cackhoanchovayvaphaithu', 'numeric'),
                      ('tm_chovaynghiepvukyquymargin', 'numeric'),
                      ('tm_chovayungtruoctienbanckcuakhachhang', 'numeric'),
                      ('tm_phaithukhac', 'numeric'),
                      ('indexed_timestamp_', 'timestamp without time zone'),
                      ('symbol_', 'text'),
                      ('donvikiemtoan', 'text'),
                      ('ykienkiemtoan', 'text'),
                      ('type', 'text'),
                      ('code', 'text'),
                  ],
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='calculate_bctc_nam_chung_khoan',
                  same_table_name=False,
                  timestep=timedelta(days=1,hours=0,minutes=0),
                  version=1,
                  timestamp_field='indexed_timestamp_',
                  symbol_field='symbol_',
                  to_create=False,
                  stream_fields = [
                      ('last_updated', 'timestamp without time zone'),
                      ('nam', 'numeric'),
                      ('quy', 'numeric'),
                      ('doanhthuhoatdong', 'numeric'),
                      ('doanhthuhoatdongtangtruongsvck', 'numeric'),
                      ('chovaymargin', 'numeric'),
                      ('chovaymargintangtruongsvck', 'numeric'),
                      ('loinhuansauthue', 'numeric'),
                      ('loinhuansauthuetangtruongsvck', 'numeric'),
                      ('laitucactaisantaichinh', 'numeric'),
                      ('laitucactaisantaichinhtangtruongsvck', 'numeric'),
                      ('laitucackhoandautunamgiudenngaydaohan', 'numeric'),
                      ('laitucackhoandautunamgiudenngaydaohantangtruongsvck', 'numeric'),
                      ('laitucackhoanchovayvaphaithu', 'numeric'),
                      ('laitucackhoanchovayvaphaithutangtruongsvck', 'numeric'),
                      ('laitutaisantaichinhsansangdeban', 'numeric'),
                      ('laitutaisantaichinhsansangdebantangtruongsvck', 'numeric'),
                      ('laitucaccongcuphaisinhphongnguaruiro', 'numeric'),
                      ('laitucaccongcuphaisinhphongnguaruirotangtruongsvck', 'numeric'),
                      ('doanhthuhoatdongmoigioichungkhoan', 'numeric'),
                      ('doanhthuhoatdongmoigioichungkhoantangtruongsvck', 'numeric'),
                      ('doanhthubaolanhdailyphathanhchungkhoan', 'numeric'),
                      ('doanhthubaolanhdailyphathanhchungkhoantangtruongsvck', 'numeric'),
                      ('doanhthutuvandautuchungkhoan', 'numeric'),
                      ('doanhthutuvandautuchungkhoantangtruongsvck', 'numeric'),
                      ('doanhthuhoatdonguythacdaugia', 'numeric'),
                      ('doanhthuhoatdonguythacdaugiatangtruongsvck', 'numeric'),
                      ('doanhthuhoatdongluukychungkhoan', 'numeric'),
                      ('doanhthuhoatdongluukychungkhoantangtruongsvck', 'numeric'),
                      ('doanhthuhoatdongtuvantaichinh', 'numeric'),
                      ('doanhthuhoatdongtuvantaichinhtangtruongsvck', 'numeric'),
                      ('doanhthuhoatdongkhac', 'numeric'),
                      ('doanhthuhoatdongkhactangtruongsvck', 'numeric'),
                      ('tongchiphihoatdong', 'numeric'),
                      ('tongchiphihoatdongtangtruongsvck', 'numeric'),
                      ('lotucactaisantaichinh', 'numeric'),
                      ('lotucactaisantaichinhtangtruongsvck', 'numeric'),
                      ('lotucackhoandautunamgiudenngaydaohan', 'numeric'),
                      ('lotucackhoandautunamgiudenngaydaohantangtruongsvck', 'numeric'),
                      ('chiphilaivaylotucackhoanchovayvaphaithu', 'numeric'),
                      ('chiphilaivaylotucackhoanchovayvaphaithutangtruongsvck', 'numeric'),
                      ('lovadanhgiachenhlechlaitaisantangtruongsvck', 'numeric'),
                      ('chiphiduphongxulytaisankhodoi', 'numeric'),
                      ('chiphiduphongxulytaisankhodoitangtruongsvck', 'numeric'),
                      ('lotucactaisantaichinhphaisinh', 'numeric'),
                      ('lotucactaisantaichinhphaisinhtangtruongsvck', 'numeric'),
                      ('chiphihoatdongtudoanh', 'numeric'),
                      ('chiphihoatdongtudoanhtangtruongsvck', 'numeric'),
                      ('chiphihoatdongmoigioichungkhoan', 'numeric'),
                      ('chiphihoatdongmoigioichungkhoantangtruongsvck', 'numeric'),
                      ('chiphihoatdongbaolanhdailyphathanhchungkhoan', 'numeric'),
                      ('chiphihoatdongbaolanhdailyphathanhchungkhoantangtruongsvck', 'numeric'),
                      ('chiphihoatdongtuvandautuchungkhoan', 'numeric'),
                      ('chiphihoatdongtuvandautuchungkhoantangtruongsvck', 'numeric'),
                      ('chiphihoatdonguythacdaugia', 'numeric'),
                      ('chiphihoatdonguythacdaugiatangtruongsvck', 'numeric'),
                      ('chiphihoatdongluukychungkhoan', 'numeric'),
                      ('chiphihoatdongluukychungkhoantangtruongsvck', 'numeric'),
                      ('chiphihoatdongtuvantaichinh', 'numeric'),
                      ('chiphihoatdongtuvantaichinhtangtruongsvck', 'numeric'),
                      ('chiphihoatdongkhac', 'numeric'),
                      ('chiphihoatdongkhactangtruongsvck', 'numeric'),
                      ('indexed_timestamp_', 'timestamp without time zone'),
                      ('type', 'character varying'),
                      ('lovadanhgiachenhlechlaitaisan', 'numeric'),
                      ('symbol_', 'text'),
                  ],
                  storage_backend=DatabaseStorage),
    ]

    output_cfg:StreamCfg = StreamCfg(
        signal_name='miner_test_abc',
        timestep=timedelta(days=1,hours=0,minutes=0),
        same_table_name=True,
        version=1,
        timestamp_field='indexed_timestamp_',
        symbol_field='symbol_',
        storage_backend=MockStorage,
        stream_fields=[
                      ('symbol_', 'text'),
                      ('indexed_timestamp_', 'timestamp'),
                      ('code', 'text'),
                      ('type', 'text'),
                      ('quy', 'numeric'),
                      ('nam', 'numeric'),
                      ('tm_taisantcniemyetdangkygdtaivsdcuactck', 'numeric'),
                      ('tm_taisantcdaluukytaivsdvachuagdcuactck', 'numeric'),
                      ('tm_taisantcchovecuactck', 'numeric'),
                      ('tm_taisantcchualuukytaivsdcuactck', 'numeric'),
                      ('tm_taisantcniemyetdangkygdtaivsd', 'numeric'),
                      ('tm_taisantcdaluukyvsdchuagdcuandt', 'numeric'),
                      ('tm_taisantcchovecuandt', 'numeric'),
                      ('tm_taisantcchualuukytaivsdcuandt', 'text'),
                      ('tm_tienguicuandt', 'numeric'),
                      ('tm_tienguivehoatdongmoigioick', 'numeric'),
                      ('tm_tienguicuandtvegdckctckql', 'numeric'),
                      ('tm_tiencuandtvegdcknhtmql', 'text'),
                      ('tm_tienguitonghopgdckcuandt', 'numeric'),
                      ('tm_tienguibutruthanhtoangdck', 'numeric'),
                      ('tm_tienguicuatochucphathanhck', 'numeric'),
                      ('tm_phaitrandtvetienguigdckctckql', 'numeric'),
                      ('tm_phaitrandttrongnuoctienguigdckctckql', 'numeric'),
                      ('tm_phaitrandtnuocngoaitienguigdckctckql', 'numeric'),
                      ('tm_phaitrandtvetienguigdcknhtmql', 'text'),
                      ('tm_phaitrandttrongnuoctienguigdcknhtmql', 'text'),
                      ('tm_phaitrandtnuocngoaitienguigdcknhtmql', 'text'),
                      ('tm_giatrighisotaisantcfvtpl', 'numeric'),
                      ('tm_cophieuniemyetfvtplghiso', 'numeric'),
                      ('tm_cophieuchuaniemyetfvtplghiso', 'text'),
                      ('tm_chungchiquyfvtplghiso', 'text'),
                      ('tm_traiphieufvtplghiso', 'numeric'),
                      ('tm_taisantckhacfvtplghiso', 'text'),
                      ('tm_giatrihoplytaisantcfvtpl', 'numeric'),
                      ('tm_cophieuniemyetfvtplhoply', 'numeric'),
                      ('tm_cophieuchuaniemyetfvtplhoply', 'text'),
                      ('tm_chungchiquyfvtplhoply', 'text'),
                      ('tm_traiphieufvtplhoply', 'numeric'),
                      ('tm_taisantckhacfvtplhoply', 'text'),
                      ('tm_giatrighisotaisantcafs', 'numeric'),
                      ('tm_cophieuniemyetafsghiso', 'numeric'),
                      ('tm_cophieuchuaniemyetafsghiso', 'text'),
                      ('tm_chungchiquyafsghiso', 'text'),
                      ('tm_traiphieuafsghiso', 'numeric'),
                      ('tm_taisantckhacafsghiso', 'numeric'),
                      ('tm_giatrihoplytaisantcafs', 'numeric'),
                      ('tm_cophieuniemyetafshoply', 'numeric'),
                      ('tm_cophieuchuaniemyetafshoply', 'text'),
                      ('tm_chungchiquyafshoply', 'text'),
                      ('tm_traiphieuafshoply', 'numeric'),
                      ('tm_taisantckhacafshoply', 'numeric'),
                      ('tm_giatrighisotaisantchtm', 'numeric'),
                      ('tm_traiphieuhtm', 'text'),
                      ('tm_congcuthitruongtientehtm', 'numeric'),
                      ('tm_taisantckhachtm', 'text'),
                      ('tm_cackhoanchovayvaphaithu', 'numeric'),
                      ('tm_chovaynghiepvukyquymargin', 'numeric'),
                      ('tm_chovayungtruoctienbanckcuakhachhang', 'numeric'),
                      ('tm_phaithukhac', 'text'),
                      ('donvikiemtoan', 'text'),
                      ('ykienkiemtoan', 'text'),
                      ('Error', 'text'),
        ]
    )

    def __init__(self, target_symbols=['VND', 'VUA']):
        output_stream=DataStreamBase.from_config(self.output_cfg)

        input_streams = self.init_input_streams(self.input_cfg)
        ############################
        # get_target_symbols 
        ############################
        if target_symbols is None:
            list_symbol = list(
                input_streams[
                    "wifeed_bctc_thuyet_minh_chung_khoan"
                ].backend.get_distinct_symbol(config.SYSTEM_SYMBOL_COL)[
                    config.SYSTEM_SYMBOL_COL
                ]
            )
            target_symbols = list_symbol

        super().__init__(target_symbols=target_symbols, input_streams=input_streams,output_stream=output_stream)

    def get_inputs(self, timestamp)->List[Node]:
        def get_inputs(timestamp:datetime,target_symbols:List[str],input_streams:Dict[str,DataStreamBase])->List[Node]:    
            advanced_where_thuyet_minh = "order by symbol_"
            thuyet_minh_dnsx_timestamp = datetime(timestamp.year, 1, 1)
            wifeed_bctc_thuyet_minh_hientai = self.input_streams[
                "wifeed_bctc_thuyet_minh_chung_khoan"
            ].get_record(
                thuyet_minh_dnsx_timestamp,
                tuple(self.target_symbols),
                advanced_where_thuyet_minh,
            )

            thuyet_minh_dnsx_ky_truoc_timestamp = datetime(timestamp.year - 1, 1, 1)
            wifeed_bctc_thuyet_minh_ckng = self.input_streams[
                "wifeed_bctc_thuyet_minh_chung_khoan"
            ].get_record(
                thuyet_minh_dnsx_ky_truoc_timestamp,
                tuple(self.target_symbols),
                advanced_where_thuyet_minh,
            )
            calculate_bctc_nam_chung_khoan_hientai=self.input_streams[
            "calculate_bctc_nam_chung_khoan"
            ].get_record(
              thuyet_minh_dnsx_timestamp,
              tuple(self.target_symbols),
              advanced_where_thuyet_minh
            )
            logging.info(f"{calculate_bctc_nam_chung_khoan_hientai.info()}")
            inputs={
                    "wifeed_bctc_thuyet_minh_hientai":Node(name="wifeed_bctc_thuyet_minh_hientai",source=['wifeed_bctc_thuyet_minh_chung_khoan'],dataframe=wifeed_bctc_thuyet_minh_hientai),
                    "wifeed_bctc_thuyet_minh_ckng":Node(name="wifeed_bctc_thuyet_minh_ckng",source=['wifeed_bctc_thuyet_minh_chung_khoan'],dataframe=wifeed_bctc_thuyet_minh_ckng),
                    "calculate_bctc_nam_chung_khoan_hientai":Node(name="calculate_bctc_nam_chung_khoan_hientai",source=['calculate_bctc_nam_chung_khoan'],dataframe=calculate_bctc_nam_chung_khoan_hientai)
            }
            return inputs
        return get_inputs(timestamp=timestamp,input_streams=self.input_streams,target_symbols=self.target_symbols)
        
        

    def process_per_symbol(self, inputs:List[pd.DataFrame], symbol:str, timestamp):
        def process_per_symbol(symbol:str, timestamp:datetime, inputs:Dict[str,Node]):
            def symbol_split(symbol:str,inputs:Dict[str,Node]):
                current_symbol_inputs:Dict[str,DataFrame]={}
                other_symbol_inputs:Dict[str,DataFrame]={}
                for input_name,input_node in inputs.items():
                    input_data=input_node.dataframe
                    if len(input_data) != 0:
                        current_symbol_inputs[input_name]=input_data[input_data['symbol_'] == symbol]
                        other_symbol_inputs[input_name]=input_data[input_data['symbol_'] != symbol]
                    else:
                        current_symbol_inputs[input_name]=pd.DataFrame(columns=input_data.columns)
                        other_symbol_inputs[input_name]=pd.DataFrame(columns=input_data.columns)
                return current_symbol_inputs,other_symbol_inputs

            def diff_series(series1, series2):
                result = []
                for value1, value2 in zip(series1, series2):
                    if pd.isna(value1) or pd.isna(value2) or value2==0:
                        result.append(None)
                        continue
                    result.append(value1 / value2)
                return result
            current_symbol_inputs,other_symbol_inputs=symbol_split(symbol=symbol,inputs=inputs)
                      
            wifeed_thuyet_minh_hientai_record = current_symbol_inputs['wifeed_bctc_thuyet_minh_hientai']
            wifeed_thuyet_minh_ckng_record=current_symbol_inputs['wifeed_bctc_thuyet_minh_ckng']
            calculate_nam_chung_khoan_record=current_symbol_inputs['calculate_bctc_nam_chung_khoan_hientai']
            logging.info(f"symbol: {symbol} va timestamp: {timestamp} {len(wifeed_thuyet_minh_ckng_record)} va {len(calculate_nam_chung_khoan_record)}")
            if len(wifeed_thuyet_minh_hientai_record)==0 or len(wifeed_thuyet_minh_ckng_record)==0 or len(calculate_nam_chung_khoan_record)==0:
                return
            wifeed_thuyet_minh_ckng_record=wifeed_thuyet_minh_ckng_record.add_suffix('_prev')
            wifeed_thuyet_minh_merged=pd.merge(wifeed_thuyet_minh_hientai_record,wifeed_thuyet_minh_ckng_record,
                                                    left_on=['symbol_','type','quy'],right_on=['symbol__prev','type_prev','quy_prev'])
            logging.info(f"wifeed_thuyet_minh_merged mid {len(wifeed_thuyet_minh_merged)}")

            wifeed_thuyet_minh_merged.insert(0,'symbol_',wifeed_thuyet_minh_merged.pop('symbol_'))
            wifeed_thuyet_minh_merged.insert(1,'indexed_timestamp_',wifeed_thuyet_minh_merged.pop('indexed_timestamp_'))
            for c in wifeed_thuyet_minh_merged.columns:
                if c.endswith("_prev"):
                    wifeed_thuyet_minh_merged=wifeed_thuyet_minh_merged.drop(c,axis=1)
                if not c.endswith("_prev") and np.issubdtype(wifeed_thuyet_minh_merged[c].dtype, np.number):
                    wifeed_thuyet_minh_merged[c]=diff_series(wifeed_thuyet_minh_merged[c], wifeed_thuyet_minh_merged[f'{c}_prev'])
            # logging.info(f"wifeed_thuyet_minh_merged {wifeed_thuyet_minh_merged['indexed_timestamp_']}")
            # logging.info(f"calculate_nam_chung_khoan_record {calculate_nam_chung_khoan_record['indexed_timestamp_']}")
            
            out_df=wifeed_thuyet_minh_merged.merge(calculate_nam_chung_khoan_record,
                                                    how='inner', on='indexed_timestamp_')
            logging.info(f"wifeed_thuyet_minh_merged after {wifeed_thuyet_minh_merged.info()}")
            return wifeed_thuyet_minh_merged
        return process_per_symbol(inputs=inputs, symbol=symbol, timestamp=timestamp)
        