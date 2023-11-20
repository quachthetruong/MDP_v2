from typing import List
import pandas as pd
dtype_mapping = {
    "str": "object",
    "int": "int64",
    "float": "float64",
    "Decimal": "float64",
    "bool": "bool_"
}


def normalize_data_type(dataframe: pd.DataFrame) -> pd.DataFrame:
    def convert_type(series: pd.Series) -> str:
        count = {"float": 0, "Decimal": 0, "int": 0, "bool": 0}

        if series.dtype.name != 'object':
            return series.dtype.name
        for value in series:
            if value is None:
                continue
            if type(value).__name__ == 'str':
                return dtype_mapping['str']
            count[type(value).__name__] += 1
        for key in count.keys():
            if count[key] > 0:
                return dtype_mapping[key]
        return 'object'

    for col in dataframe.columns:
        dtype = convert_type(dataframe[col])
        dataframe[col] = dataframe[col].astype(dtype)
    return dataframe
