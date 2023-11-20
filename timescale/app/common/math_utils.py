import pandas as pd
import numpy as np
import logging


class MathUtils:
    def __init__(self) -> None:
        pass
    
    @classmethod
    def calculate_ratio(cls, current, prev, warn_msg):
        if current is None or prev is None or np.isnan(prev) or np.isnan(current):
            logging.warning(warn_msg)
            return None
        if prev == 0:
            logging.warning(warn_msg)
            return None 
        if prev > 0:
            return current/prev - 1
        else:
            return 1 - current/prev

    @classmethod
    def calculate_absolute_ratio(cls, current, prev, warn_msg):
        if current is None or prev is None or np.isnan(prev) or np.isnan(current):
            logging.warning(warn_msg)
            return None
        if prev == 0:
            logging.warning(warn_msg)
            return None
        return current/prev

    @classmethod
    def calculate_minus(cls, current, prev, warn_msg):
        if current is None or prev is None or np.isnan(prev) or np.isnan(current):
            logging.warning(warn_msg)
            return None
        return current - prev     
    
    @classmethod
    def calculate_sum(cls, series : pd.Series, warn_msg):
        if np.any(np.isnan(series)):
            logging.warning(warn_msg)
            return None
        return np.sum(series)

    @classmethod
    def greater_equal_to(cls, current, prev, warn_msg=None):
        if current is None or prev is None or np.isnan(prev) or np.isnan(current):
            if warn_msg is not None:
                logging.warning(warn_msg)
            else:
                logging.warning("Greater or equal: {} >= {}".format(current, prev))
            return False
        return current >= prev

    @classmethod
    def calculate_minus_(cls, current, prev, warn_msg):
        if current is None or prev is None or np.isnan(prev) or np.isnan(current):
            logging.warning(warn_msg)
            return None
        if current < 0:
            return current + prev
        return current - prev
    
    @classmethod
    def calculate_sum_(cls, current, prev, warn_msg):
        if current is None or prev is None or np.isnan(prev) or np.isnan(current):
            logging.warning(warn_msg)
            return None
        return current + prev


