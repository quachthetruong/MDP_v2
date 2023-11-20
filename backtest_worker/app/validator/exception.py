import os
import sys
from typing import List
import pandas as pd
import traceback
from schemas.exception import InvalidNode, InvalidCode, InvalidStage, MinerException
import logging


def miner_exception_handler(func: callable):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except (InvalidStage, InvalidNode) as e:
            errors: List[str] = [e.__class__.__name__, *e.errors]
            logging.error(errors)
            raise MinerException(errors)
        except InvalidCode as e:
            # exc_type, exc_obj, exc_tb = sys.exc_info()
            # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errors: List[str] = [e.__class__.__name__, *e.errors]
            logging.error(errors)
            raise MinerException(errors)
        except Exception as e:
            errors: List[str] = [e.__class__.__name__,
                                 traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)]
            logging.error(errors)
            raise MinerException(errors)

    return inner_function
