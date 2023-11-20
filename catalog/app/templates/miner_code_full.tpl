from common.math_utils import MathUtils
from common.response_utils import ResponseUtils
from miners.miner_base_v2 import MinerBaseV2
from datetime import timedelta,datetime
from streams.data_stream_base import DataStreamBase
from storage.database_storage import DatabaseStorage
from storage.mock_storage import MockStorage
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
from schemas.other import Node

class {{camel_case(metadata.name)}}(MinerBaseV2):
    """
    {{metadata.description}}
    """

    input_warn_msg = "While access the input. There's a problem with input {} for the symbol {} at timestamp {} "
    output_warn_msg = "While producing output. There's a problem with symbol {} at timestamp {} for the field: {}"
    # list input stream cfg
    input_cfg:List[StreamCfg] = [{% for input_stream in spec.input_streams %}
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

    output_cfg:StreamCfg = StreamCfg(
        signal_name='{{spec.output_stream.metadata.signal_name}}',
        timestep=timedelta(days={{spec.output_stream.metadata.timestep.days}},hours={{spec.output_stream.metadata.timestep.hours}},minutes={{spec.output_stream.metadata.timestep.minutes}}),
        same_table_name=True,
        version='{{spec.output_stream.metadata.version}}',
        timestamp_field='{{spec.output_stream.metadata.timestamp_field}}',
        symbol_field='{{spec.output_stream.metadata.symbol_field}}',
        storage_backend=MockStorage,
        stream_fields=[{% for stream_field in spec.output_stream.spec.stream_fields %}
                      ('{{stream_field.name}}', '{{stream_field.type}}'),{% endfor %}
        ]
    )

    def __init__(self, target_symbols={{metadata.target_symbols}}):
        output_stream=DataStreamBase.from_config(self.output_cfg)

        input_streams = self.init_input_streams(self.input_cfg)
        ############################
        # get_target_symbols 
        ############################
        if target_symbols is None:
            list_symbol = list(
                input_streams[
                    "{{spec.input_streams[0].metadata.signal_name}}"
                ].backend.get_distinct_symbol(config.SYSTEM_SYMBOL_COL)[
                    config.SYSTEM_SYMBOL_COL
                ]
            )
            target_symbols = list_symbol

        super().__init__(target_symbols=target_symbols, input_streams=input_streams,output_stream=output_stream)

    def get_inputs(self, timestamp)->List[Node]: 
        {%- if get_inputs_str is not none %}
        {{get_inputs_str|indent(width=8, first=False)}}
        return get_inputs(timestamp=timestamp,input_streams=self.input_streams,target_symbols=self.target_symbols)
        {% else %}
        pass{% endif %}
        

    def process_per_symbol(self, inputs:List[pd.DataFrame], symbol:str, timestamp):
        {%- if process_per_symbol_str is not none %}
        {{process_per_symbol_str|indent(width=8, first=False)}}
        return process_per_symbol(inputs=inputs, symbol=symbol, timestamp=timestamp)
        {% else %}
        pass{% endif %}
