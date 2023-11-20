from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from miners.miner_back_test_base import MinerBackTestBase
from datetime import timedelta, datetime
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from storage.redshift_storage import RedshiftStorage
from storage.encap_database_storage import EncapDatabaseStorage
from common import config
from streams.stream_cfg import StreamCfg
from dateutil.relativedelta import relativedelta
from typing import Dict, List
import logging
import pandas as pd
import numpy as np
from pandas import DataFrame
from schemas.miner_unit import Node


class MinerTest3(MinerBackTestBase):
    """
    test miner 3
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg = [
        StreamCfg(signal_name='wifeed_bctc_can_doi_ke_toan_chung_khoan',
                  same_table_name=True,
                  timestep=timedelta(days=1, hours=0, minutes=0),
                  version='1',
                  timestamp_field='indexed_timestamp_',
                  symbol_field='symbol_',
                  to_create=False,
                  stream_fields=[
                      ('quy', 'numeric'),
                      ('nam', 'numeric'),
                      ('tongcongtaisan', 'numeric'),
                      ('taisannganhan', 'numeric'),
                      ('taisantcnganhan', 'numeric'),
                      ('tienvacackhoantuongduongtien', 'numeric'),
                      ('tien', 'numeric'),
                      ('cackhoantuongduongtien', 'numeric'),
                      ('taisantcfvtpl', 'numeric'),
                      ('dautugiudenngaydaohanhtm', 'numeric'),
                      ('cackhoanchovay', 'numeric'),
                      ('cackhoantcsansangdebanafs', 'numeric'),
                      ('dpsuygiamtaisantc', 'numeric'),
                      ('tongcackhoanphaithunganhan', 'numeric'),
                      ('cackhoanphaithu', 'numeric'),
                      ('phaithuvaduthucotuctienlaicactaisantc', 'numeric'),
                      ('phaithucotuctienlaidenngaynhan', 'numeric'),
                      ('duthucotuctienlaichuadenngaynhan', 'numeric'),
                      ('phaithukhachhang', 'numeric'),
                      ('phaithucacdichvuctckcungcap', 'numeric'),
                      ('phaithuhoatdonggiaodichchungkhoan', 'numeric'),
                      ('phaithuveloigiaodichchungkhoan', 'numeric'),
                      ('tratruocchonguoiban', 'numeric'),
                      ('phaithunoibonganhan', 'numeric'),
                      ('cackhoanphaithukhac', 'numeric'),
                      ('dpsuygiamgiatricackhoanphaithu', 'numeric'),
                      ('duphongcackhoanphaithunganhankhodoi', 'numeric'),
                      ('hangtonkhotong', 'numeric'),
                      ('hangtonkho', 'numeric'),
                      ('duphonggiamgiahangtonkho', 'numeric'),
                      ('taisannganhankhac_tong', 'numeric'),
                      ('tamung', 'numeric'),
                      ('vattuvanphongcongcudungcu', 'numeric'),
                      ('cptratruocnganhan', 'numeric'),
                      ('camcokycuockyquynganhan', 'numeric'),
                      ('thuevacackhoankhacphaithucuanhanuoc', 'numeric'),
                      ('giaodichmuabanlaitraiphieuchinhphuts', 'numeric'),
                      ('taisannganhankhac', 'numeric'),
                      ('duphongsuygiamgiatritaisannganhankhac', 'numeric'),
                      ('taisandaihan', 'numeric'),
                      ('taisantcdaihan', 'numeric'),
                      ('cackhoanphaithudaihan', 'numeric'),
                      ('phaithudaihancuakhachhang', 'numeric'),
                      ('vonkinhdoanhocacdonvitructhuoc', 'numeric'),
                      ('phaithudaihannoibo', 'numeric'),
                      ('phaithudaihankhac', 'numeric'),
                      ('duphongphaithudaihankhodoi', 'numeric'),
                      ('cackhoandautu', 'numeric'),
                      ('dautuvaocaccongtycon', 'numeric'),
                      ('dautuvaocongtylienketliendoanh', 'numeric'),
                      ('dautuchungkhoandaihan', 'numeric'),
                      ('chungkhoansansangdeban', 'numeric'),
                      ('dautunamgiudenngaydaohan', 'numeric'),
                      ('dautudaihankhac', 'numeric'),
                      ('duphonggiamgiadautudaihan', 'numeric'),
                      ('taisancodinh', 'numeric'),
                      ('taisancodinhhuuhinh', 'numeric'),
                      ('nguyengiahuuhinh', 'numeric'),
                      ('haomonhuuhinh', 'numeric'),
                      ('danhgiatscdhhtheogiatrihoply', 'numeric'),
                      ('taisancodinhthuetc', 'numeric'),
                      ('nguyengiathuetc', 'numeric'),
                      ('haomonthuetc', 'numeric'),
                      ('danhgiatscdttctheogiatrihoply', 'numeric'),
                      ('taisancodinhvohinh', 'numeric'),
                      ('nguyengiavohinh', 'numeric'),
                      ('danhgiatscdvhtheogiatrihoply', 'numeric'),
                      ('batdongsandautu', 'numeric'),
                      ('nguyengiabatdongsandautu', 'numeric'),
                      ('haomonbatdongsandautu', 'numeric'),
                      ('danhgiabdsdttheogiatrihoply', 'numeric'),
                      ('taisandodangdaihan', 'numeric'),
                      ('cpsanxuatkinhdoanhdodangdaihan', 'numeric'),
                      ('cpxaydungcobandodang', 'numeric'),
                      ('taisandaihankhac_tong', 'numeric'),
                      ('camcokyquykycuocdaihankhac', 'numeric'),
                      ('cptratruocdaihan', 'numeric'),
                      ('taisanthuethunhaphoanlai', 'numeric'),
                      ('tiennopquyhotrothanhtoan', 'numeric'),
                      ('taisandaihankhac', 'numeric'),
                      ('loithethuongmai', 'numeric'),
                      ('duphongsuygiamgiatritaisandaihan', 'numeric'),
                      ('tongnguonvon', 'numeric'),
                      ('nophaitra', 'numeric'),
                      ('nonganhan', 'numeric'),
                      ('vayvanothuetcnganhan', 'numeric'),
                      ('vaynganhan', 'numeric'),
                      ('nothuetaisantcnganhan', 'numeric'),
                      ('vaytaisantcnganhan', 'numeric'),
                      ('traiphieuchuyendoinganhan', 'numeric'),
                      ('traiphieuphathanhnganhan', 'numeric'),
                      ('vayquyhotrothanhtoan', 'numeric'),
                      ('phaitrahoatdonggiaodichchungkhoan', 'numeric'),
                      ('phaitraveloigiaodichcactaisantc', 'numeric'),
                      ('phaitranhacungcapnganhan', 'numeric'),
                      ('nguoimuatratientruocnganhan', 'numeric'),
                      ('thuevacackhoanphainopnhanuoc', 'numeric'),
                      ('phaitranguoilaodong', 'numeric'),
                      ('cackhoantrichnopphucloinhanvien', 'numeric'),
                      ('cpphaitranganhan', 'numeric'),
                      ('phaitranoibonganhan', 'numeric'),
                      ('doanhthuchuathuchiennganhan', 'numeric'),
                      ('nhankyquykycuocnganhan', 'numeric'),
                      ('phaitrahocotucgocvalaitraiphieu', 'numeric'),
                      ('phaitratochucphathanhchungkhoan', 'numeric'),
                      ('cackhoanphaitraphainopnganhankhac', 'numeric'),
                      ('duphongphaitranganhan', 'numeric'),
                      ('quykhenthuongphucloi', 'numeric'),
                      ('giaodichmuabanlaitraiphieuchinhphu', 'numeric'),
                      ('nodaihan', 'numeric'),
                      ('vayvanothuetcdaihan', 'numeric'),
                      ('vaydaihan', 'numeric'),
                      ('nothuetaisantcdaihan', 'numeric'),
                      ('vaytaisantcdaihan', 'numeric'),
                      ('traiphieuchuyendoidaihan', 'numeric'),
                      ('traiphieuphathanhdaihan', 'numeric'),
                      ('phaitradaihannguoiban', 'numeric'),
                      ('nguoimuatratruocdaihan', 'numeric'),
                      ('cpphaitradaihan', 'numeric'),
                      ('phaitranoibodaihan', 'numeric'),
                      ('doanhthuchuathuchiendaihan', 'numeric'),
                      ('nhankyquykycuocdaihan', 'numeric'),
                      ('cophieuuudai_no', 'numeric'),
                      ('phaitradaihankhac', 'numeric'),
                      ('thuethunhaphoanlaiphaitra', 'numeric'),
                      ('duphongtrocapmatvieclam', 'numeric'),
                      ('duphongcackhoannodaihan', 'numeric'),
                      ('quyduphongbaovenhadautu', 'numeric'),
                      ('quyphattrienkhoahoccongnghe', 'numeric'),
                      ('vonchusohuu_tong', 'numeric'),
                      ('vonchusohuu', 'numeric'),
                      ('vondautucuachusohuu', 'numeric'),
                      ('vongopcuachusohuu', 'numeric'),
                      ('cophieuphothong', 'numeric'),
                      ('cophieuuudai', 'numeric'),
                      ('thangduvoncophan', 'numeric'),
                      ('quyenchonchuyendoitraiphieu', 'numeric'),
                      ('vonkhaccuachusohuu', 'numeric'),
                      ('cophieuquy', 'numeric'),
                      ('chenhlechdgltaisantheogiahoply', 'numeric'),
                      ('chenhlechtygiahoidoai', 'numeric'),
                      ('quydutrubosungvondieule', 'numeric'),
                      ('quydautuphattrien', 'numeric'),
                      ('quydptcvaruironghiepvu', 'numeric'),
                      ('quykhacthuocvonchusohuu', 'numeric'),
                      ('lnsauthuechuaphanphoi', 'numeric'),
                      ('lndathuchien_bs', 'numeric'),
                      ('lnchuathuchien_bs', 'numeric'),
                      ('nguonvondautuxdcb', 'numeric'),
                      ('quyhotrosapxepdoanhnghiep', 'numeric'),
                      ('loiichcodongkhongkiemsoat', 'numeric'),
                      ('indexed_timestamp_', 'timestamp without time zone'),
                      ('donvikiemtoan', 'text'),
                      ('ykienkiemtoan', 'text'),
                      ('code', 'text'),
                      ('type', 'text'),
                      ('symbol_', 'text'),
                      ('haomonvohinh', 'numeric'),
                  ],
                  storage_backend=DatabaseStorage),
    ]

    def __init__(self, target_symbols=['VND', 'VUA']):
        input_streams = self.init_input_streams(self.input_cfg)
        ############################
        # get_target_symbols
        ############################
        if target_symbols is None:
            list_symbol = list(
                input_streams[
                    "wifeed_bctc_can_doi_ke_toan_chung_khoan"
                ].backend.get_distinct_symbol(config.SYSTEM_SYMBOL_COL)[
                    config.SYSTEM_SYMBOL_COL
                ]
            )
            target_symbols = list_symbol

        super().__init__(target_symbols, input_streams)

    def get_inputs(self, timestamp) -> List[Node]:
        def get_inputs(timestamp: datetime, target_symbols: List[str], input_streams: Dict[str, DataStreamBase]) -> List[Node]:
            advanced_where = "and type = 'quarter' and quy = 0 and donvikiemtoan notnull order by indexed_timestamp_"
            can_doi_ke_toan = self.input_streams[
                "wifeed_bctc_can_doi_ke_toan_chung_khoan"
            ].get_record(
                timestamp,
                tuple(self.target_symbols),
                advanced_where,
            )
            inputs = {
                "can_doi_ke_toan_hientai": Node(name="can_doi_ke_toan_hientai", source=['wifeed_bctc_can_doi_ke_toan_chung_khoan'], dataframe=can_doi_ke_toan),
            }
            logging.info(f"input return {inputs}")
            return inputs
        return get_inputs(timestamp=timestamp, input_streams=self.input_streams, target_symbols=self.target_symbols)

    def process_per_symbol(self, inputs: List[pd.DataFrame], symbol: str, timestamp):
        pass
