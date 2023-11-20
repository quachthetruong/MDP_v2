from datetime import datetime
from typing import Dict, List
from schemas.miner_unit import Node
from streams.data_stream_base import DataStreamBase
from miners.processor.minerProcessor import MinerProcessor

from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from common.data_utils import DataUtils

from datetime import timedelta,datetime
from streams.data_stream_base import DataStreamBase

from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
import numpy as np
from pandas import DataFrame
from schemas.miner_unit import Node
import talib as ta
from common import config
from validator.validate import validate_nodes,validate_code,validate_process_per_symbol

class TransformProcessor(MinerProcessor):
    def __init__(self, name="transform",code:str=""):
        super().__init__(name)
        self.code=code

    @validate_process_per_symbol
    @validate_code
    def process_per_symbol(self,code:str, inputs:Dict[str, Node], symbol:str, timestamp:datetime)->Node:
        result: Dict[str, Node]={}
        local_params={'result':result,'timestamp':timestamp, 'inputs':inputs, 'symbol':symbol}
        user_code=code
        final_code=f"{user_code}\nresult=process_per_symbol(inputs=inputs, symbol=symbol, timestamp=timestamp)"
        exec(final_code,globals(),local_params)
        return local_params['result']
        
    @validate_nodes
    def process_all_symbols(self, inputNodes: Dict[str, Node], target_symbols: List[str], timestamp: datetime,code:str) -> Dict[str, Node]:
        outputs: List[Node] = []
        for symbol in target_symbols:
            output = self.process_per_symbol(
                inputs=inputNodes, symbol=symbol, timestamp=timestamp,code=code)
            outputs.append(output)
        return {"output": Node(name="output", source=list(inputNodes.keys()), dataframe=pd.concat(map(lambda x: x.dataframe, outputs)))}

    def execute(self,timestamp:datetime,data:Dict[str,Node])-> Dict[str, Node]:
        return self.process_all_symbols(inputNodes=data,timestamp=timestamp,code=self.code,
                               target_symbols=self.miner_config.metadata.target_symbols)
        