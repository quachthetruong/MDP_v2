import pandas as pd
import logging
from common import config


class ResponseUtils():
    def __init__(self) -> None:
        pass

    @classmethod
    def one_record_res(cls, input_df: pd.DataFrame, warn_msg):
        if input_df is None or len(input_df) != 1:
            logging.warning(warn_msg)
            return pd.Series(None, input_df.columns)
        else:
            return input_df.iloc[0]

    @classmethod
    def filter_one_record_res(cls, input_df: pd.DataFrame, timestamp, warn_msg):
        tmp_df = input_df[input_df[config.SYSTEM_TIMESTAMP_COL] == timestamp]
        if len(tmp_df) != 1:
            logging.warning(warn_msg)
            return pd.Series(None, input_df.columns), False
        return tmp_df.iloc[0], True

    @classmethod
    def encap_filter_one_record_res(cls, input_df: pd.DataFrame, timestamp, warn_msg):
        tmp_df = input_df[input_df['time'] == timestamp]
        if len(tmp_df) != 1:
            logging.warning(warn_msg)
            return pd.Series(None, input_df.columns), False
        return tmp_df.iloc[0], True

    @classmethod
    def encap_filter_one_record_by_date(cls, input_df: pd.DataFrame, timestamp, warn_msg):
        # print("timestamp_Fill: ", pd.to_datetime(timestamp).date())

        tmp_df = input_df[input_df['time'].dt.date == pd.to_datetime(timestamp).date()]
        if len(tmp_df) != 1:
            logging.warning(warn_msg)
            return pd.Series(None, input_df.columns), False
        return tmp_df.iloc[0], True
