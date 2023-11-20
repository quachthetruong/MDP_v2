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

class MinerNewTest(MinerBaseV2):
    """
    Test miner
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg:List[StreamCfg] = [
        StreamCfg(signal_name='wifeed_bctc_ket_qua_kinh_doanh_bao_hiem',
                  same_table_name=True,
                  timestep=timedelta(days=1,hours=0,minutes=0),
                  version=1,
                  timestamp_field='indexed_timestamp_',
                  symbol_field='symbol_',
                  to_create=False,
                  stream_fields = [
                      ('quy', 'numeric'),
                      ('nam', 'numeric'),
                      ('doanhthuphibaohiem', 'numeric'),
                      ('phibaohiemgoc', 'numeric'),
                      ('phinhantaibaohiem', 'numeric'),
                      ('tgduphongphibaohiemgoc', 'numeric'),
                      ('phinhuongtaibaohiem', 'numeric'),
                      ('tongphinhuongtaibaohiem', 'numeric'),
                      ('tgduphongphinhuongtaibaohiem', 'numeric'),
                      ('cackhoangiamtrukhac', 'numeric'),
                      ('doanhthuphibaohiemthuan', 'numeric'),
                      ('hoahongnhuongtaibaohiem', 'numeric'),
                      ('thuhoahongnhuongtaibaohiem', 'numeric'),
                      ('thukhachdkdbaohiem', 'numeric'),
                      ('doanhthuthuan', 'numeric'),
                      ('chiboithuong', 'numeric'),
                      ('tongchiboithuong', 'numeric'),
                      ('chiboithuongbaohiemgoc', 'numeric'),
                      ('chiboithuongnhantaibaohiem', 'numeric'),
                      ('chiboithuongkhac', 'numeric'),
                      ('cackhoangiamtru', 'numeric'),
                      ('thudoinguoithuba', 'numeric'),
                      ('thuhangdaxulyboithuong100', 'numeric'),
                      ('cackhoangiamtrukhac_chibt', 'numeric'),
                      ('thuboithuongnhuongtaibaohiem', 'numeric'),
                      ('tangduphongnghiepvubaohiemgoc', 'numeric'),
                      ('tgduphongtoanhoc', 'numeric'),
                      ('tanggiamduphongcamketdaututoithieu', 'numeric'),
                      ('tanggiamduphongchialai', 'numeric'),
                      ('tanggiamduphongdambaocandoi', 'numeric'),
                      ('tanggiamduphongnghiepvubaohiemgockhac', 'numeric'),
                      ('tgduphongboithuongbaohiemgoc', 'numeric'),
                      ('tgduphongboithuongnhuongtaibaohiem', 'numeric'),
                      ('tongchiboithuongvatratienbaohiem', 'numeric'),
                      ('trichduphongdaodonglon', 'numeric'),
                      ('chikhachdkdbaohiemgoc', 'numeric'),
                      ('chihoahongbaohiemgoc', 'numeric'),
                      ('chidoinguoithu3', 'numeric'),
                      ('chixulyhangboithuong100', 'numeric'),
                      ('chidephonghanchetonthat', 'numeric'),
                      ('chigiamdinhchidanhgiaruirodoituongduocbaohiemchikhac', 'numeric'),
                      ('chiphibanhangbaohiemgoc', 'numeric'),
                      ('chikhachdbaohiemgoc', 'numeric'),
                      ('chikhachdkdnhantaibaohiem', 'numeric'),
                      ('chihdnhuongtaibaohiem', 'numeric'),
                      ('chikhachoatdongkinhdoanhbaohiem', 'numeric'),
                      ('tongchitructiephdkdbaohiem', 'numeric'),
                      ('lngop', 'numeric'),
                      ('lntuhdkdkhac', 'numeric'),
                      ('doanhthuhdkdkhac', 'numeric'),
                      ('cphdkdkhac', 'numeric'),
                      ('lnhdtaichinh', 'numeric'),
                      ('doanhthuhdtaichinh', 'numeric'),
                      ('chiphitaichinh', 'numeric'),
                      ('lailotucongtyliendoanhlienket', 'numeric'),
                      ('chiphiquanlydn', 'numeric'),
                      ('lnthuantuhdkd', 'numeric'),
                      ('lnkhac', 'numeric'),
                      ('thunhapkhac', 'numeric'),
                      ('chiphikhac', 'numeric'),
                      ('tonglnketoantruocthue_bs', 'numeric'),
                      ('chiphithuetndnhienhanh', 'numeric'),
                      ('chiphithuetndnhoanlai', 'numeric'),
                      ('lnstthunhapdn', 'numeric'),
                      ('loiichcuacodongthieuso', 'numeric'),
                      ('lnstcuacongtyme', 'numeric'),
                      ('laicobantrencophieu', 'numeric'),
                      ('indexed_timestamp_', 'timestamp without time zone'),
                      ('code', 'text'),
                      ('symbol_', 'text'),
                      ('donvikiemtoan', 'text'),
                      ('ykienkiemtoan', 'text'),
                      ('type', 'text'),
                  ],
                  storage_backend=DatabaseStorage),
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
        StreamCfg(signal_name='wifeed_bctc_chi_so_tai_chinh_ttm',
                  same_table_name=True,
                  timestep=timedelta(days=1,hours=0,minutes=0),
                  version=1,
                  timestamp_field='indexed_timestamp_',
                  symbol_field='symbol_',
                  to_create=False,
                  stream_fields = [
                      ('quy', 'numeric'),
                      ('nam', 'numeric'),
                      ('soluongluuhanh_ttm', 'numeric'),
                      ('eps_ttm', 'numeric'),
                      ('bookvalue_ttm', 'numeric'),
                      ('vonhoa_ttm', 'numeric'),
                      ('ev_ttm', 'numeric'),
                      ('fcff_ttm', 'numeric'),
                      ('bienlaigop_ttm', 'numeric'),
                      ('bienlaithuan_ttm', 'numeric'),
                      ('bienlaiebit_ttm', 'numeric'),
                      ('bienlaitruocthue_ttm', 'numeric'),
                      ('vongquaytaisan_ttm', 'numeric'),
                      ('vongquayphaithu_ttm', 'numeric'),
                      ('vongquaytonkho_ttm', 'numeric'),
                      ('vongquayphaitra_ttm', 'numeric'),
                      ('roa_ttm', 'numeric'),
                      ('roe_ttm', 'numeric'),
                      ('roic_ttm', 'numeric'),
                      ('dongtien_hdkd_lnthuan_ttm', 'numeric'),
                      ('tongno_tongtaisan_ttm', 'numeric'),
                      ('congnonganhan_tongtaisan_ttm', 'numeric'),
                      ('congnodaihan_tongtaisan_ttm', 'numeric'),
                      ('thanhtoan_hienhanh_ttm', 'numeric'),
                      ('thanhtoan_nhanh_ttm', 'numeric'),
                      ('thanhtoan_tienmat_ttm', 'numeric'),
                      ('novay_dongtienhdkd_ttm', 'numeric'),
                      ('ebit_laivay_ttm', 'numeric'),
                      ('pe_ttm', 'numeric'),
                      ('pb_ttm', 'numeric'),
                      ('ps_ttm', 'numeric'),
                      ('p_ocf_ttm', 'numeric'),
                      ('ev_ocf_ttm', 'numeric'),
                      ('ev_ebit_ttm', 'numeric'),
                      ('nim_ttm', 'numeric'),
                      ('cof_ttm', 'numeric'),
                      ('yea_ttm', 'numeric'),
                      ('cir_ttm', 'numeric'),
                      ('doanhthu_ttm', 'numeric'),
                      ('lairong_ttm', 'numeric'),
                      ('novay_ttm', 'numeric'),
                      ('tonkho_ttm', 'numeric'),
                      ('ocf_ttm', 'numeric'),
                      ('thunhaplaithuan_ttm', 'numeric'),
                      ('tongthunhaphoatdong_ttm', 'numeric'),
                      ('car_ttm', 'numeric'),
                      ('tt_thunhaplaithuan_ttm_yoy', 'numeric'),
                      ('tt_doanhthu_ttm_yoy', 'numeric'),
                      ('tt_lairong_ttm_yoy', 'numeric'),
                      ('tt_ocf_ttm_yoy', 'numeric'),
                      ('indexed_timestamp_', 'timestamp without time zone'),
                      ('type', 'text'),
                      ('symbol_', 'text'),
                      ('code', 'text'),
                  ],
                  storage_backend=DatabaseStorage),
    ]

    output_cfg:StreamCfg = StreamCfg(
        signal_name='miner_new_test',
        timestep=timedelta(days=1,hours=0,minutes=0),
        same_table_name=True,
        version=1,
        timestamp_field='indexed_timestamp_',
        symbol_field='symbol_',
        storage_backend=MockStorage,
        stream_fields=[
                      ('symbol_', 'text'),
                      ('other_symbol', 'text'),
                      ('indexed_timestamp_', 'timestamp'),
                      ('code', 'text'),
                      ('type', 'text'),
                      ('quy', 'numeric'),
                      ('nam', 'numeric'),
                      ('doanhthuphibaohiem', 'numeric'),
                      ('phibaohiemgoc', 'numeric'),
                      ('phinhantaibaohiem', 'numeric'),
                      ('tgduphongphibaohiemgoc', 'numeric'),
                      ('phinhuongtaibaohiem', 'numeric'),
                      ('tongphinhuongtaibaohiem', 'numeric'),
                      ('tgduphongphinhuongtaibaohiem', 'numeric'),
                      ('cackhoangiamtrukhac', 'text'),
                      ('doanhthuphibaohiemthuan', 'numeric'),
                      ('hoahongnhuongtaibaohiem', 'numeric'),
                      ('thuhoahongnhuongtaibaohiem', 'numeric'),
                      ('thukhachdkdbaohiem', 'numeric'),
                      ('doanhthuthuan', 'numeric'),
                      ('chiboithuong', 'numeric'),
                      ('tongchiboithuong', 'numeric'),
                      ('chiboithuongbaohiemgoc', 'text'),
                      ('chiboithuongnhantaibaohiem', 'text'),
                      ('chiboithuongkhac', 'text'),
                      ('cackhoangiamtru', 'text'),
                      ('thudoinguoithuba', 'text'),
                      ('thuhangdaxulyboithuong100', 'text'),
                      ('cackhoangiamtrukhac_chibt', 'text'),
                      ('thuboithuongnhuongtaibaohiem', 'numeric'),
                      ('tangduphongnghiepvubaohiemgoc', 'text'),
                      ('tgduphongtoanhoc', 'text'),
                      ('tanggiamduphongcamketdaututoithieu', 'text'),
                      ('tanggiamduphongchialai', 'text'),
                      ('tanggiamduphongdambaocandoi', 'text'),
                      ('tanggiamduphongnghiepvubaohiemgockhac', 'text'),
                      ('tgduphongboithuongbaohiemgoc', 'numeric'),
                      ('tgduphongboithuongnhuongtaibaohiem', 'numeric'),
                      ('tongchiboithuongvatratienbaohiem', 'numeric'),
                      ('trichduphongdaodonglon', 'numeric'),
                      ('chikhachdkdbaohiemgoc', 'numeric'),
                      ('chihoahongbaohiemgoc', 'numeric'),
                      ('chidoinguoithu3', 'text'),
                      ('chixulyhangboithuong100', 'text'),
                      ('chidephonghanchetonthat', 'text'),
                      ('chigiamdinhchidanhgiaruirodoituongduocbaohiemchikhac', 'text'),
                      ('chiphibanhangbaohiemgoc', 'text'),
                      ('chikhachdbaohiemgoc', 'text'),
                      ('chikhachdkdnhantaibaohiem', 'text'),
                      ('chihdnhuongtaibaohiem', 'text'),
                      ('chikhachoatdongkinhdoanhbaohiem', 'text'),
                      ('tongchitructiephdkdbaohiem', 'numeric'),
                      ('lngop', 'numeric'),
                      ('lntuhdkdkhac', 'text'),
                      ('doanhthuhdkdkhac', 'text'),
                      ('cphdkdkhac', 'text'),
                      ('lnhdtaichinh', 'numeric'),
                      ('doanhthuhdtaichinh', 'numeric'),
                      ('chiphitaichinh', 'numeric'),
                      ('lailotucongtyliendoanhlienket', 'text'),
                      ('chiphiquanlydn', 'numeric'),
                      ('lnthuantuhdkd', 'numeric'),
                      ('lnkhac', 'numeric'),
                      ('thunhapkhac', 'text'),
                      ('chiphikhac', 'text'),
                      ('tonglnketoantruocthue_bs', 'numeric'),
                      ('chiphithuetndnhienhanh', 'numeric'),
                      ('chiphithuetndnhoanlai', 'text'),
                      ('lnstthunhapdn', 'numeric'),
                      ('loiichcuacodongthieuso', 'text'),
                      ('lnstcuacongtyme', 'numeric'),
                      ('laicobantrencophieu', 'text'),
                      ('donvikiemtoan', 'text'),
                      ('ykienkiemtoan', 'text'),
        ]
    )

    def __init__(self, target_symbols=['ABI', 'BIC']):
        output_stream=DataStreamBase.from_config(self.output_cfg)

        input_streams = self.init_input_streams(self.input_cfg)
        ############################
        # get_target_symbols 
        ############################
        if target_symbols is None:
            list_symbol = list(
                input_streams[
                    "wifeed_bctc_ket_qua_kinh_doanh_bao_hiem"
                ].backend.get_distinct_symbol(config.SYSTEM_SYMBOL_COL)[
                    config.SYSTEM_SYMBOL_COL
                ]
            )
            target_symbols = list_symbol

        super().__init__(target_symbols=target_symbols, input_streams=input_streams,output_stream=output_stream)

    def get_inputs(self, timestamp)->List[Node]:
        def get_inputs(timestamp:datetime,target_symbols:List[str],input_streams:Dict[str,DataStreamBase])->List[Node]:    
          advanced_where = "and type = 'quarter' and quy !=0 order by indexed_timestamp_"
          ketquakinhdoanh_trongnam_start_timestamp = datetime(timestamp.year, 1, 1)
          ketquakinhdoanh_trongnam = input_streams['wifeed_bctc_ket_qua_kinh_doanh_bao_hiem'].get_record_range(
              ketquakinhdoanh_trongnam_start_timestamp,
              timestamp,
              tuple(target_symbols),
              advanced_where,
          )
          ketquakinhdoanh_namtruoc = input_streams['wifeed_bctc_ket_qua_kinh_doanh_bao_hiem'].get_record_range(
          ketquakinhdoanh_trongnam_start_timestamp - relativedelta(years=1),
          timestamp - relativedelta(years=1),
          tuple(target_symbols),
          advanced_where
          )
          thuyet_minh_chung_khoan_timestamp = datetime(2022,1,1,0)
          thuyet_minh_chung_khoan = input_streams[
                      "wifeed_bctc_thuyet_minh_chung_khoan"
                  ].get_record(
                      thuyet_minh_chung_khoan_timestamp,
                      tuple(['AGR','PHS','TVS']),
                      advanced_where,
                      config.SYSTEM_SYMBOL_COL,
                  )
          inputs={
                  "ketquakinhdoanh_trongnam":Node(name="ketquakinhdoanh_trongnam",source=['wifeed_bctc_ket_qua_kinh_doanh_bao_hiem'],dataframe=ketquakinhdoanh_trongnam),
                  "ketquakinhdoanh_namtruoc":Node(name="ketquakinhdoanh_namtruoc",source=['wifeed_bctc_ket_qua_kinh_doanh_bao_hiem'],dataframe=ketquakinhdoanh_namtruoc),
                  "thuyet_minh_chung_khoan":Node(name="thuyet_minh_chung_khoan",source=['wifeed_bctc_thuyet_minh_chung_khoan'],dataframe=thuyet_minh_chung_khoan),
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

          def divide_series(series1, series2):
              result = []
              for value1, value2 in zip(series1, series2):
                  if pd.isna(value1) or pd.isna(value2) or value2==0:
                      result.append("empty")
                      continue
                  result.append(value1 / value2)
              return result

          current_symbol_inputs,other_symbol_inputs=symbol_split(symbol=symbol,inputs=inputs)
              
          ketquakinhdoanh_trongnam_current = current_symbol_inputs['ketquakinhdoanh_trongnam']
          ketquakinhdoanh_trongnam_other= other_symbol_inputs['ketquakinhdoanh_trongnam']

          ketquakinhdoanh_trongnam_other=ketquakinhdoanh_trongnam_other.add_suffix('_other')
          ketquakinhdoanh_trongnam_merged=pd.merge(ketquakinhdoanh_trongnam_current,ketquakinhdoanh_trongnam_other,
                                                  left_on='indexed_timestamp_',right_on='indexed_timestamp__other')

          ketquakinhdoanh_trongnam_merged.insert(0,'symbol_',ketquakinhdoanh_trongnam_merged.pop('symbol_'))
          ketquakinhdoanh_trongnam_merged.insert(1,'other_symbol',ketquakinhdoanh_trongnam_merged.pop('symbol__other'))
          ketquakinhdoanh_trongnam_merged.insert(2,'indexed_timestamp_',ketquakinhdoanh_trongnam_merged.pop('indexed_timestamp_'))

          for c in ketquakinhdoanh_trongnam_merged.columns:
              if c.endswith("_other"):
                  ketquakinhdoanh_trongnam_merged=ketquakinhdoanh_trongnam_merged.drop(c,axis=1)
              if not c.endswith("_other") and np.issubdtype(ketquakinhdoanh_trongnam_merged[c].dtype, np.number):
                  ketquakinhdoanh_trongnam_merged[c]=divide_series(ketquakinhdoanh_trongnam_merged[c], ketquakinhdoanh_trongnam_merged[f'{c}_other'])
                  
          out_df = ketquakinhdoanh_trongnam_merged.replace({np.nan: None})
          return out_df
        return process_per_symbol(inputs=inputs, symbol=symbol, timestamp=timestamp)
        