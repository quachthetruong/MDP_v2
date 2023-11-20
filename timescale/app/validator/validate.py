import logging
from typing import List
import pandas as pd
from cron_converter import Cron
from datetime import datetime

from pydantic import ValidationError
from schemas.miner_unit import Node, Stage
from schemas.exception import InvalidCode, InvalidNode, InvalidStage
from validator.StageValidator import StageValidator
from validator.NodeValidator import NodeValidator


def get_cron_instance(cron_str: str, start_date: datetime, end_date: datetime) -> Cron:
    assert isinstance(start_date, datetime)
    assert isinstance(end_date, datetime)
    try:
        cron_instance = Cron()
        cron_instance.from_string(cron_str)
        return cron_instance
    except Exception as e:
        logging.warning('The cron_str expression is wrong. ' + str(e))
        raise


def validate_nodes(func: callable):
    def inner_function(*args, **kwargs):
        result = func(*args, **kwargs)
        try:
            NodeValidator.validate_dict(result)
            return result
        except AssertionError as e:
            raise InvalidNode(msg=str(e)) from e
    return inner_function


def validate_stages(func: callable):
    def inner_function(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            StageValidator.validate(result)
            return result
        except (AssertionError, ValidationError) as e:
            raise InvalidStage(msg=str(e)) from e
    return inner_function


def validate_code(func: callable):
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise InvalidCode(msg=str(e)) from e
    return inner_function


def validate_process_per_symbol(func: callable):
    def inner_function(*args, **kwargs):
        result = func(*args, **kwargs)
        try:
            NodeValidator.validate_symbol(result, symbol=kwargs['symbol'])
            return result
        except AssertionError as e:
            raise InvalidNode(msg=str(e)) from e
    return inner_function
