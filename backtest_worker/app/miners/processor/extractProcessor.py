from datetime import datetime
from typing import Dict, List
from schemas.miner_unit import Node
from streams.data_stream_base import DataStreamBase
from miners.processor.minerProcessor import MinerProcessor

from common.math_utils import MathUtils
from common.response_utils import ResponseUtils

from datetime import timedelta,datetime
from streams.data_stream_base import DataStreamBase

from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
import numpy as np
from schemas.miner_unit import Node
import talib as ta
from validator.validate import validate_nodes,validate_code


class ExtractProcessor(MinerProcessor):
    def __init__(self, name="extract",code:str=""):
        super().__init__(name)
        self.code=code

    @validate_nodes
    @validate_code
    def get_inputs(self,code:str,input_streams:Dict[str,DataStreamBase],timestamp:datetime,target_symbols:List[str]) -> Dict[str, Node]:
        result: Dict[str, Node]={}
        local_params={'result':result,'timestamp':timestamp, 'input_streams':input_streams, 'target_symbols':target_symbols}
        user_code=code
        final_code=f"{user_code}\nnodes = get_inputs(timestamp=timestamp, input_streams=input_streams, target_symbols=target_symbols)\nresult: Dict[str, Node] = {{node.name: node for node in nodes}}"
        exec(final_code,globals(),local_params)
        return local_params['result']

    def execute(self,timestamp:datetime,data:Dict[str,DataStreamBase])-> Dict[str, Node]:
        return self.get_inputs(input_streams=data,timestamp=timestamp,code=self.code,
                               target_symbols=self.miner_config.metadata.target_symbols)
        