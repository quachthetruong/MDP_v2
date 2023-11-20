from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from miners.miner_back_test_base import MinerBackTestBase
from datetime import timedelta,datetime
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from storage.redshift_storage import RedshiftStorage
from storage.encap_database_storage import EncapDatabaseStorage
from common import config
from streams.stream_cfg import StreamCfg
from dateutil.relativedelta import relativedelta
from typing import Dict,List
import logging
import pandas as pd
import numpy as np
from pandas import DataFrame
from schemas.miner_unit import Node
from validator.validate import validate_nodes, validate_code,validate_process_per_symbol
import logging
import talib as ta

class {{camel_case(metadata.name)}}(MinerBackTestBase):
    """
    {{metadata.description}}
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg = [{% for input_stream in spec.input_streams %}
        StreamCfg(signal_name='{{input_stream.metadata.signal_name}}',
                  same_table_name={{input_stream.metadata.same_table_name}},
                  timestep=timedelta(days={{input_stream.metadata.timestep.days}},hours={{input_stream.metadata.timestep.hours}},minutes={{input_stream.metadata.timestep.minutes}}),
                  version='{{input_stream.metadata.version}}',
                  timestamp_field='{{input_stream.metadata.timestamp_field}}',
                  symbol_field='{{input_stream.metadata.symbol_field}}',
                  to_create={{input_stream.metadata.to_create}},
                  stream_fields = [{% for stream_field in input_stream.spec.stream_fields %}
                      ('{{stream_field.name}}', '{{stream_field.type}}'),{% endfor %}
                  ],
                  storage_backend={{input_stream.metadata.storage_backend}}),{% endfor %}
    ]


    def __init__(self, target_symbols={{metadata.target_symbols}}):
        input_streams = self.init_input_streams(self.input_cfg)
        ############################
        # get_target_symbols 
        ############################
        first_input_stream=input_streams[self.input_cfg[0].signal_name]
        if target_symbols is None or len(target_symbols) == 0:
            list_symbol = list(
                first_input_stream.backend.get_distinct_symbol(first_input_stream.symbol_field)[
                    first_input_stream.symbol_field
                ]
            )
            target_symbols = list_symbol

        super().__init__(target_symbols, input_streams)

    @validate_nodes
    @validate_code
    def get_inputs(self, timestamp)->Dict[str,Node]: 
        {%- if get_inputs_str is not none %}
        {{get_inputs_str|indent(width=8, first=False)}}
        nodes = get_inputs(timestamp=timestamp, input_streams=self.input_streams, target_symbols=self.target_symbols)
        result: Dict[str, Node] = {node.name: node for node in nodes}
        return result
        {% else %}
        pass{% endif %}
    
    @validate_process_per_symbol
    @validate_code
    def process_per_symbol(self, inputs:Dict[str, Node], symbol:str, timestamp)->Node:
        {%- if process_per_symbol_str is not none %}
        {{process_per_symbol_str|indent(width=8, first=False)}}

        return process_per_symbol(inputs=inputs, symbol=symbol, timestamp=timestamp)
        {% else %}
        pass{% endif %}
