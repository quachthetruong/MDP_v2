from .miner_base import MinerBase
from datetime import timedelta
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
import numpy as np
from common import config
from datetime import datetime
from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from streams.stream_cfg import StreamCfg
from storage.redshift_storage import RedshiftStorage


class DnseBCTCInsuranceQuarterlyMiner(MinerBase):
    """
    Trading ideas miner for Doanh Nghiep Bao Hiem
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg = [
        StreamCfg(signal_name='wifeed_bctc_ket_qua_kinh_doanh_bao_hiem',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  timestamp_field=config.SYSTEM_TIMESTAMP_COL,
                  version='1',
                  symbol_field=config.SYSTEM_SYMBOL_COL,
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='wifeed_bctc_chi_so_tai_chinh_quarter',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  timestamp_field=config.SYSTEM_TIMESTAMP_COL,
                  version='1',
                  symbol_field=config.SYSTEM_SYMBOL_COL,
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='wifeed_bctc_chi_so_tai_chinh_ttm',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  timestamp_field=config.SYSTEM_TIMESTAMP_COL,
                  version='1',
                  symbol_field=config.SYSTEM_SYMBOL_COL,
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='dnsebctcinsurancequarterlyminer',
                  timestep=timedelta(days=1),
                  version='1',
                  timestamp_field=config.SYSTEM_TIMESTAMP_COL,
                  symbol_field=config.SYSTEM_SYMBOL_COL,
                  storage_backend=DatabaseStorage),
        StreamCfg(signal_name='market_.stock_ohlc_days',
                  same_table_name=True,
                  timestep=timedelta(days=1),
                  version='1',
                  timestamp_field='time',
                  symbol_field='symbol',
                  to_create=False,
                  stream_fields=[
                      ("time", "timestamp"),
                      ("symbol", "varchar(20)"),
                  ],
                  storage_backend=RedshiftStorage)
    ]

    output_cfg = StreamCfg(signal_name='dnsebctcinsurancequarterlyminer',
                           timestep=timedelta(days=1),
                           version='1',
                           timestamp_field=config.SYSTEM_TIMESTAMP_COL,
                           symbol_field=config.SYSTEM_SYMBOL_COL,
                           storage_backend=DatabaseStorage,
                           stream_fields=[
                               ("type", "varchar(20)"),  # quaterly, yearly
                               ("quy", "numeric"),  # quy 1, 2, 3, 4
                               ("nam", "numeric"),  # 2018, 2019, 2020
                               ("doanhthuphibaohiem", "float"),
                               ("doanhthuphibaohiemtangtruongqoq", "float"),
                               ("doanhthuphibaohiemtangtruongsvck", "float"),
                               ("luykedoanhthuphibaohiem", "float"),
                               ("luykedoanhthuphibaohiemtangtruongsvck", "float"),
                               ("loinhuansauthuecuachusohuutapdoan", "float"),
                               ("loinhuansauthuecuachusohuutapdoantangtruongqoq", "float"),
                               ("loinhuansauthuecuachusohuutapdoantangtruongsvck", "float"),
                               ("luykeloinhuansauthuecuachusohuutapdoan", "float"),
                               ("luykeloinhuansauthuecuachusohuutapdoantangtruongsvck", "float"),
                               ("loinhuanthuantuhoatdongtaichinh", "float"),
                               ("loinhuantuhoatdongtaichinhtangtruongqoq", "float"),
                               ("loinhuantuhoatdongtaichinhtangtruongsvck", "float"),
                               ("luykeloinhuantuhoatdongtaichinh", "float"),
                               ("luykeloinhuantuhoatdongtaichinhtangtruongsvck", "float"),
                               ("loinhuangophoatdongkinhdoanhbaohiem", "float"),
                               ("loinhuangophoatdongkinhdoanhbaohiemtangtruongqoq", "float"),
                               ("loinhuangophoatdongkinhdoanhbaohiemtangtruongsvck", "float"),
                               ("luykeloinhuangophoatdongkinhdoanhbaohiem", "float"),
                               ("luykeloinhuangophoatdongkinhdoanhbaohiemtangtruongsvck", "float"),
                               ("eps_ttm", "float"),  # chisotaichinh.eps
                               ("pe_ttm", "float"),  # chisotaichinh.pe_ttm
                               ("pb_ttm", "float"),  # chisotaichinh.pb_ttm
                               # chisotaichinh.pbtb3nam
                               ("pbtb3nam", "float"),
                               # chisotaichinh.pbtb5nam
                               ("petb3nam", "float"),
                               # time at which this record is updated
                               ("last_updated", "timestamp"),
                               # ('timestamp', 'timestamp'), # timestamp of the reported period
                               # ('__symbol', 'varchar(20)'),         # should not define this field
                               # ('__indexed_timestamp', 'timestamp')   # should not define this field
                               ("giacophieu", "numeric"),
                               ("chiboithuongvatrabaohiem", "numeric"),
                               ("chiboithuongvatrabaohiemtangtruongsvck", "numeric"),
                               ("giacophieu_last_updated", "text")
                           ])

    def __init__(self, target_symbols):
        output_stream = DataStreamBase.from_config(self.output_cfg)

        input_streams = self.init_input_streams(self.input_cfg)

        if target_symbols is None:
            list_symbol = list(
                input_streams[
                    "wifeed_bctc_ket_qua_kinh_doanh_bao_hiem"
                ].backend.get_distinct_symbol(config.SYSTEM_SYMBOL_COL)[
                    config.SYSTEM_SYMBOL_COL
                ]
            )
            target_symbols = list_symbol

        logging.info(f"Done init: {input_streams}")

        super().__init__(target_symbols, output_stream, input_streams)

    def get_inputs(self, timestamp):

        # logging timestamp
        logging.info(f"Logging timestamp: {timestamp}")

        advanced_where = "and type = 'quarter' and quy !=0 order by {}".format(
            config.SYSTEM_TIMESTAMP_COL
        )

        ttm_advanced_where = "and type = 'ttm' and quy !=0  order by {}".format(
            config.SYSTEM_TIMESTAMP_COL
        )

        limit_advanced_where = "and type = 'quarter' and quy !=0 order by {} LIMIT 1".format(
            config.SYSTEM_TIMESTAMP_COL
        )

        ketquakinhdoanh_trongnam_start_timestamp = datetime(
            timestamp.year, 1, 1)
        ketquakinhdoanh_trongnam = self.input_streams[
            "wifeed_bctc_ket_qua_kinh_doanh_bao_hiem"
        ].get_record_range(
            ketquakinhdoanh_trongnam_start_timestamp,
            timestamp,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            advanced_where,
        )

        logging.info("ketquakinhdoanh_trongnam: ")
        logging.info(ketquakinhdoanh_trongnam)

        ketquakinhdoanh_svck_advanced_where = (
            "and type = 'quarter' order by {} LIMIT 1".format(
                config.SYSTEM_TIMESTAMP_COL
            )
        )

        ketquakinhdoanh_svck_timestamp = datetime(
            timestamp.year - 1, timestamp.month, timestamp.day
        )

        ketquakinhdoanh_svck = self.input_streams[
            "wifeed_bctc_ket_qua_kinh_doanh_bao_hiem"
        ].get_record(
            ketquakinhdoanh_svck_timestamp,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            advanced_where,
        )

        ketquakinhdoanh_quytruoc_timestamp = timestamp - \
            relativedelta(months=3)
        ketquakinhdoanh_quytruoc = self.input_streams[
            "wifeed_bctc_ket_qua_kinh_doanh_bao_hiem"
        ].get_record(
            ketquakinhdoanh_quytruoc_timestamp,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            advanced_where,
        )

        output_namtruoc_timestamp = datetime(timestamp.year - 1, 1, 1)
        output_namtruoc = self.input_streams[
            "dnsebctcinsurancequarterlyminer"
        ].get_record_range(
            output_namtruoc_timestamp,
            timestamp,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            advanced_where,
        )

        output_quytruoc_timestamp = timestamp - relativedelta(months=3)
        output_quytruoc = self.input_streams[
            "dnsebctcinsurancequarterlyminer"
        ].get_record(
            output_quytruoc_timestamp,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            advanced_where,
        )

        ketquakinhdoanh_kytruoc_timestamp_strat = datetime(
            timestamp.year - 1, 1, 1)
        ketquakinhdoanh_kytruoc_timestamp_end = datetime(
            timestamp.year - 1, timestamp.month, timestamp.day
        )

        ketquakinhdoanh_ky_truoc = self.input_streams[
            "wifeed_bctc_ket_qua_kinh_doanh_bao_hiem"
        ].get_record_range(
            ketquakinhdoanh_kytruoc_timestamp_strat,
            ketquakinhdoanh_kytruoc_timestamp_end,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            advanced_where,
        )

        chisotaichinh_quynay_ttm = self.input_streams[
            "wifeed_bctc_chi_so_tai_chinh_ttm"
        ].get_record(
            timestamp,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            ttm_advanced_where,
        )

        timestamp_3y_ago = datetime(
            timestamp.year - 3, timestamp.month, timestamp.day)
        timestamp_end_3y_ago = datetime(timestamp.year, 1, 1)
        advanced_where_quy4 = "and type = 'ttm' and quy != 0 order by {}".format(
            config.SYSTEM_TIMESTAMP_COL
        )

        chisotaichinhquy4_3y_ago = self.input_streams[
            "wifeed_bctc_chi_so_tai_chinh_ttm"
        ].get_record_range(
            timestamp_3y_ago,
            timestamp_end_3y_ago,
            config.SYSTEM_SYMBOL_COL,
            tuple(self.target_symbols),
            advanced_where_quy4,
        )

        included_max_timestamp = datetime.today()
        giacophieu = self.input_streams['market_.stock_ohlc_days'].get_record_range_v2(
            included_min_timestamp=timestamp + relativedelta(days=-5),
            included_max_timestamp=included_max_timestamp,
            symbol_column='symbol',
            timestamp_column='time',
            target_symbols=self.target_symbols,
            filter_query='order by time desc limit {}'.format(
                len(self.target_symbols))
        )
        giacophieu['rank'] = giacophieu.groupby(
            'symbol')['time'].rank(method='first')
        giacophieu = giacophieu[giacophieu['rank'] == 1]
        giacophieu['symbol_'] = giacophieu['symbol']

        return (
            ketquakinhdoanh_trongnam,
            ketquakinhdoanh_svck,
            ketquakinhdoanh_quytruoc,
            ketquakinhdoanh_ky_truoc,
            output_quytruoc,
            output_namtruoc,
            chisotaichinh_quynay_ttm,
            chisotaichinhquy4_3y_ago,
            giacophieu
        )

    def process_per_symbol(self, inputs, symbol, timestamp):

        # get input records

        (
            ketquakinhdoanh_trongnam,
            ketquakinhdoanh_svck,
            ketquakinhdoanh_quytruoc,
            ketquakinhdoanh_ky_truoc,
            output_quytruoc,
            output_namtruoc,
            chisotaichinh_quynay_ttm,
            chisotaichinhquy4_3y_ago,
            giacophieu
        ) = inputs

        current_ketquakinhdoanh_record, flag = ResponseUtils.filter_one_record_res(
            ketquakinhdoanh_trongnam,
            timestamp,
            "Khong co du lieu quy hien tai in ketquakinhdoanh_trongnam for symbol {} at timestamp {}".format(
                symbol, timestamp
            ),
        )

        if not flag:
            return
        if int(current_ketquakinhdoanh_record["quy"]) not in [1, 2, 4]:
            print('not quy 4 or 1 or 2')
            return

        ketquakinhdoanh_quytruoc_record = ResponseUtils.one_record_res(
            ketquakinhdoanh_quytruoc,
            self.input_warn_msg.format(
                "ketquakinhdoanh_quytruoc", symbol, timestamp),
        )

        ketquakinhdoanh_svck_record = ResponseUtils.one_record_res(
            ketquakinhdoanh_svck,
            self.input_warn_msg.format(
                "ketquakinhdoanh_svck", symbol, timestamp),
        )

        chisotaichinh_quynay_ttm_record = ResponseUtils.one_record_res(
            chisotaichinh_quynay_ttm,
            self.input_warn_msg.format(
                "chisotaichinh_quynay_ttm", symbol, timestamp),
        )

        giacophieu_record = ResponseUtils.one_record_res(
            giacophieu, 'Chua co gia cua {}'.format(symbol))

        giacophieu_last_updated = None if np.isnan(
            giacophieu_record.close) else datetime.strftime(giacophieu_record.time, '%d-%m-%Y')

        giacophieu = None if np.isnan(
            giacophieu_record.close) else giacophieu_record.close
        chiboithuongvatrabaohiemtangtruongsvck = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.tongchiboithuongvatratienbaohiem,
            ketquakinhdoanh_svck_record.tongchiboithuongvatratienbaohiem,
            self.output_warn_msg.format(
                symbol, timestamp, "chiboithuongvatrabaohiem"
            )
        )

        # fill output record
        out_dict = {}
        out_dict[config.SYSTEM_SYMBOL_COL] = symbol
        out_dict[config.SYSTEM_TIMESTAMP_COL] = timestamp
        out_dict["type"] = "quarter"
        out_dict["last_updated"] = datetime.now()

        out_dict["quy"] = current_ketquakinhdoanh_record.quy
        out_dict["nam"] = current_ketquakinhdoanh_record.nam

        out_dict["doanhthuphibaohiem"] = current_ketquakinhdoanh_record.doanhthuphibaohiem

        out_dict['giacophieu'] = giacophieu
        out_dict['giacophieu_last_updated'] = giacophieu_last_updated
        out_dict['chiboithuongvatrabaohiem'] = current_ketquakinhdoanh_record.tongchiboithuongvatratienbaohiem
        out_dict['chiboithuongvatrabaohiemtangtruongsvck'] = abs(
            chiboithuongvatrabaohiemtangtruongsvck) if chiboithuongvatrabaohiemtangtruongsvck else None

        out_dict["doanhthuphibaohiemtangtruongqoq"] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.doanhthuphibaohiem,
            ketquakinhdoanh_quytruoc_record.doanhthuphibaohiem,
            self.output_warn_msg.format(
                symbol, timestamp, "doanhthuphibaohiemtangtruongqoq"
            ),
        )

        out_dict["doanhthuphibaohiemtangtruongsvck"] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.doanhthuphibaohiem,
            ketquakinhdoanh_svck_record.doanhthuphibaohiem,
            self.output_warn_msg.format(
                symbol, timestamp, "doanhthuphibaohiemtangtruongsvck"
            ),
        )

        out_dict["luykedoanhthuphibaohiem"] = MathUtils.calculate_sum(
            ketquakinhdoanh_trongnam.doanhthuphibaohiem,
            self.output_warn_msg.format(
                symbol, timestamp, "luykedoanhthuphibaohiemtangtruongsvck"
            ),
        )

        luykedoanhthuphibaohiem_kytruoc = MathUtils.calculate_sum(
            ketquakinhdoanh_ky_truoc.doanhthuphibaohiem,
            self.output_warn_msg.format(
                symbol, timestamp, "luykedoanhthuphibaohiemtangtruongsvck"
            ),
        )

        out_dict["luykedoanhthuphibaohiemtangtruongsvck"] = MathUtils.calculate_ratio(
            out_dict["luykedoanhthuphibaohiem"],
            luykedoanhthuphibaohiem_kytruoc,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuansauthuecuachusohuutapdoantangtruongqoq"
            ),
        )

        out_dict[
            "loinhuansauthuecuachusohuutapdoan"
        ] = current_ketquakinhdoanh_record.lnstcuacongtyme

        out_dict[
            "loinhuansauthuecuachusohuutapdoantangtruongqoq"
        ] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.lnstcuacongtyme,
            ketquakinhdoanh_quytruoc_record.lnstcuacongtyme,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuansauthuecuachusohuutapdoantangtruongqoq"
            ),
        )

        out_dict[
            "loinhuansauthuecuachusohuutapdoantangtruongsvck"
        ] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.lnstcuacongtyme,
            ketquakinhdoanh_svck_record.lnstcuacongtyme,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuansauthuecuachusohuutapdoantangtruongsvck"
            ),
        )

        out_dict["luykeloinhuansauthuecuachusohuutapdoan"] = MathUtils.calculate_sum(
            ketquakinhdoanh_trongnam.lnstcuacongtyme,
            self.output_warn_msg.format(
                symbol, timestamp, "luykeloinhuansauthuecuachusohuutapdoan"
            ),
        )

        luykeloinhuansauthuecuachusohuutapdoan_svck = MathUtils.calculate_sum(
            ketquakinhdoanh_ky_truoc.lnstcuacongtyme,
            self.output_warn_msg.format(
                symbol, timestamp, "luykeloinhuansauthuecuachusohuutapdoan_svck"
            ),
        )

        out_dict[
            "luykeloinhuansauthuecuachusohuutapdoantangtruongsvck"
        ] = MathUtils.calculate_ratio(
            out_dict["luykeloinhuansauthuecuachusohuutapdoan"],
            luykeloinhuansauthuecuachusohuutapdoan_svck,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuansauthuecuachusohuutapdoantangtruongsvck"
            ),
        )

        out_dict[
            "loinhuanthuantuhoatdongtaichinh"
        ] = current_ketquakinhdoanh_record.lnhdtaichinh

        out_dict["loinhuantuhoatdongtaichinhtangtruongqoq"] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.lnhdtaichinh,
            ketquakinhdoanh_quytruoc_record.lnhdtaichinh,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuantuhoatdongtaichinhtangtruongqoq"
            ),
        )

        out_dict[
            "loinhuantuhoatdongtaichinhtangtruongsvck"
        ] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.lnhdtaichinh,
            ketquakinhdoanh_svck_record.lnhdtaichinh,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuantuhoatdongtaichinhtangtruongsvck"
            ),
        )

        # loinhuanthuantuhoatdongtaichinhtangtruong

        luykeloinhuantuhoatdongtaichinh = MathUtils.calculate_sum(
            ketquakinhdoanh_trongnam.lnhdtaichinh,
            self.output_warn_msg.format(
                symbol, timestamp, "luykeloinhuantuhoatdongtaichinh"
            ),
        )

        out_dict["luykeloinhuantuhoatdongtaichinh"] = luykeloinhuantuhoatdongtaichinh

        luykeloinhuanhoatdongtaichinh_svck = MathUtils.calculate_sum(
            ketquakinhdoanh_ky_truoc.lnhdtaichinh,
            self.output_warn_msg.format(
                symbol, timestamp, "luykeloinhuanhoatdongtaichinh_svck"
            ),
        )

        out_dict[
            "luykeloinhuantuhoatdongtaichinhtangtruongsvck"
        ] = MathUtils.calculate_ratio(
            luykeloinhuantuhoatdongtaichinh,
            luykeloinhuanhoatdongtaichinh_svck,
            self.output_warn_msg.format(
                symbol, timestamp, "luykeloinhuantuhoatdongtaichinhtangtruongsvck"
            ),
        )

        out_dict[
            "loinhuangophoatdongkinhdoanhbaohiem"
        ] = current_ketquakinhdoanh_record.lngop

        out_dict[
            "loinhuangophoatdongkinhdoanhbaohiemtangtruongqoq"
        ] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.lngop,
            ketquakinhdoanh_quytruoc_record.lngop,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuangophoatdongkinhdoanhbaohiemtangtruongqoq"
            ),
        )

        out_dict[
            "loinhuangophoatdongkinhdoanhbaohiemtangtruongsvck"
        ] = MathUtils.calculate_ratio(
            current_ketquakinhdoanh_record.lngop,
            ketquakinhdoanh_svck_record.lngop,
            self.output_warn_msg.format(
                symbol, timestamp, "loinhuangophoatdongkinhdoanhbaohiemtangtruongsvck"
            ),
        )

        out_dict["luykeloinhuangophoatdongkinhdoanhbaohiem"] = MathUtils.calculate_sum(
            ketquakinhdoanh_trongnam.lngop,
            self.output_warn_msg.format(
                symbol, timestamp, "luykeloinhuangophoatdongkinhdoanhbaohiem"
            ),
        )

        luykeloinhuangophoatdongkinhdoanhbaohiem_svck = MathUtils.calculate_sum(
            ketquakinhdoanh_ky_truoc.lngop,
            self.output_warn_msg.format(
                symbol, timestamp, "luykeloinhuangophoatdongkinhdoanhbaohiem_svck"
            ),
        )

        out_dict[
            "luykeloinhuangophoatdongkinhdoanhbaohiemtangtruongsvck"
        ] = MathUtils.calculate_ratio(
            out_dict["luykeloinhuangophoatdongkinhdoanhbaohiem"],
            luykeloinhuangophoatdongkinhdoanhbaohiem_svck,
            self.output_warn_msg.format(
                symbol,
                timestamp,
                "luykeloinhuangophoatdongkinhdoanhbaohiemtangtruongsvck",
            ),
        )
        petrungbinh3nam = (chisotaichinhquy4_3y_ago["pe_ttm"].sum()) / 3
        pbtrungbinh4nam = (chisotaichinhquy4_3y_ago["pb_ttm"].sum()) / 3

        out_dict["eps_ttm"] = chisotaichinh_quynay_ttm_record.eps_ttm
        out_dict["pe_ttm"] = chisotaichinh_quynay_ttm_record.pe_ttm
        out_dict["pb_ttm"] = chisotaichinh_quynay_ttm_record.pb_ttm
        out_dict["pbtb3nam"] = pbtrungbinh4nam
        out_dict["petb3nam"] = petrungbinh3nam

        return pd.DataFrame([out_dict])
