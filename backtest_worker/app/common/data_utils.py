from typing import Dict

import pandas as pd

from schemas.miner_unit import Node


class DataUtils():
    @classmethod
    def add_prefix_suffix(cls, src, prefix="", suffix=""):
        tmp = {}
        for key, value in src.items():
            tmp["{}{}{}".format(prefix,key,suffix)] = value
        return tmp

    @classmethod
    def symbol_split(cls,symbol: str, inputs: Dict[str, Node]) -> pd.DataFrame:
        current_symbol_inputs: Dict[str, pd.DataFrame] = {}
        other_symbol_inputs: Dict[str, pd.DataFrame] = {}
        for input_name, input_node in inputs.items():
            input_data = input_node.dataframe
            if len(input_data) != 0:
                current_symbol_inputs[input_name] = input_data[input_data['symbol_'] == symbol]
                other_symbol_inputs[input_name] = input_data[input_data['symbol_'] != symbol]
            else:
                current_symbol_inputs[input_name] = pd.DataFrame(
                    columns=input_data.columns)
                other_symbol_inputs[input_name] = pd.DataFrame(
                    columns=input_data.columns)
        return current_symbol_inputs, other_symbol_inputs