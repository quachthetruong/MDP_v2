from datetime import timedelta
import logging

import pandas as pd

KAFKA_TOPIC = {
    "news": "dsai-raw-news",
    "stock_si": "si",
    "stock_tp": "tp",
    "stock_tick": "tick",
    "stock_trading_session": "trading_session",
    "derivative": "infogate_price",
}

CATEGORY_WIFEED = {
    'doanh_nghiep_san_xuat': 'doanh_nghiep_san_xuat',
    'chung_khoan': 'chung_khoan',
    'bao_hiem': 'bao_hiem',
    'ngan_hang': 'ngan_hang',
}


def format_timedelta(t_delta, fmt):
    d = {"days": t_delta.days}
    d["hours"], rem = divmod(t_delta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


def generated_identified_name(signal_name, timestep, version):
    return "{}_{}_{}".format(signal_name, format_timedelta(timestep, '{days}d{hours}h{minutes}m'), version)


def convert_date_to_first_quarter_month(df_year_col, df_month_code_col):
    dict_month_code = {
        0: '01-01',
        1: '01-01',
        2: '04-01',
        3: '07-01',
        4: '10-01'
    }
    df_month_code_col = dict_month_code[df_month_code_col]
    df_year_col = str(df_year_col)
    date_str = df_year_col + '-' + df_month_code_col
    date_str = pd.to_datetime(date_str, format='%Y-%m-%d')
    return date_str


def no_accent_vietnamese_col(df, col):
    s = df[col]
    s = s.replace(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', regex=True)
    s = s.replace(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', regex=True)
    s = s.replace(r'[èéẹẻẽêềếệểễ]', 'e', regex=True)
    s = s.replace(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', regex=True)
    s = s.replace(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', regex=True)
    s = s.replace(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', regex=True)
    s = s.replace(r'[ìíịỉĩ]', 'i', regex=True)
    s = s.replace(r'[ÌÍỊỈĨ]', 'I', regex=True)
    s = s.replace(r'[ùúụủũưừứựửữ]', 'u', regex=True)
    s = s.replace(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', regex=True)
    s = s.replace(r'[ỳýỵỷỹ]', 'y', regex=True)
    s = s.replace(r'[ỲÝỴỶỸ]', 'Y', regex=True)
    s = s.replace(r'[Đ]', 'D', regex=True)
    s = s.replace(r'[đ]', 'd', regex=True)
    df[col] = s
    return df


def convert_df_data_to_snake_case(df, col):
    s = df[col]
    s = s.str.lower()
    s = s.str.replace(' ', '_')
    df[col] = s
    return df


def convert_dict_to_timedelta(dict_time) -> timedelta:
    timestep_dict = {}
    timedelta_args = {'days', 'seconds', 'microseconds',
                      'milliseconds', 'minutes', 'hours', 'weeks'}

    if not isinstance(dict_time, dict):
        raise TypeError(
            f'timestep must be in dict type with keys: {format(timedelta_args)}')

    for key, val in dict_time.items():
        if key not in timedelta_args:
            raise TypeError(f'{key} is not right argument for timedelta')
        timestep_dict[key] = val
    logging.info(f"LOG timestep_dict: {timestep_dict}")
    return timedelta(**timestep_dict)
