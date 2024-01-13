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

class Output(MinerBaseV2):
    """
    Chao buoi sang
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg:List[StreamCfg] = [
        StreamCfg(signal_name='wifeed_bctc_thuyet_minh_chung_khoan',
                  same_table_name=True,
                  timestep=timedelta(days=1,hours=0,minutes=0),
                  version='1',
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
                  version='1',
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
        signal_name='output',
        timestep=timedelta(days=1,hours=0,minutes=0),
        same_table_name=True,
        version='1',
        timestamp_field='indexed_timestamp_',
        symbol_field='symbol_',
        storage_backend=MockStorage,
        stream_fields=[
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
                      ('indexed_timestamp_', 'timestamp'),
                      ('symbol_', 'text'),
                      ('donvikiemtoan', 'text'),
                      ('ykienkiemtoan', 'text'),
                      ('n', 'bigint'),
                      ('indexed_timestamp_test', 'numeric'),
                      ('symbol_test', 'text'),
        ]
    )

    def __init__(self, target_symbols=['VND', 'VUA', 'TCB']):
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
            
            node_get_record_0=input_streams['wifeed_bctc_thuyet_minh_chung_khoan'].get_record(
                      indexed_timestamp=datetime(timestamp.year, 1, 1))

            node_get_record_1=input_streams['calculate_bctc_nam_chung_khoan'].get_record(
                      indexed_timestamp=datetime(timestamp.year, 1, 1))

            
            node_get_record_range_0=input_streams['wifeed_bctc_thuyet_minh_chung_khoan'].get_record_range(
                      included_min_timestamp=timestamp - timedelta(days=100),
                      included_max_timestamp= timestamp)

            node_get_record_range_1=input_streams['calculate_bctc_nam_chung_khoan'].get_record_range(
                      included_min_timestamp=timestamp - timedelta(days=100),
                      included_max_timestamp= timestamp)
            
            return [Node(name='node_get_record_0',dataframe=node_get_record_0),
                Node(name='node_get_record_range_0',dataframe=node_get_record_range_0),Node(name='node_get_record_1',dataframe=node_get_record_1),
                Node(name='node_get_record_range_1',dataframe=node_get_record_range_1)]
        return get_inputs(timestamp=timestamp,input_streams=self.input_streams,target_symbols=self.target_symbols)
        
        

    def process_per_symbol(self, inputs:List[pd.DataFrame], symbol:str, timestamp):
        def process_per_symbol(symbol: str, timestamp: datetime, inputs: Dict[str, Node]) -> Node:
              
            node_get_record_0=inputs['node_get_record_0'].dataframe
            node_get_record_range_0=inputs['node_get_record_range_0'].dataframe

            node_get_record_1=inputs['node_get_record_1'].dataframe
            node_get_record_range_1=inputs['node_get_record_range_1'].dataframe
            if not node_get_record_range_0.empty:     
              node_get_record_range_0['indexed_timestamp_test']=node_get_record_range_0['indexed_timestamp_'].dt.year
              node_get_record_range_0['symbol_test']=node_get_record_range_0['symbol_' ].str[::-1]
            return Node(name='output',dataframe=node_get_record_range_0)
          
        return process_per_symbol(inputs=inputs, symbol=symbol, timestamp=timestamp)
        