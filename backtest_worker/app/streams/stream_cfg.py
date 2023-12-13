from datetime import datetime, timedelta

from common import config
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Tuple
from typing import Type
from common.utils import convert_dict_to_timedelta
from storage.storage_base import StorageBase


valid_types = set(['text', 'timestamp', 'numeric', 'float'])


def valid_prefix_and_type(v, prefix='i_', suffix='_'):
    for item in v:
        assert len(item) == 2
        if not item[0].startswith(prefix):
            raise ValueError('field name must start with {}'.format(prefix))
        if not item[1] in valid_types:
            raise ValueError('field type must in {}'.format(valid_types))
    return v


class StreamCfg(BaseModel):
    signal_name: str
    same_table_name: bool = False
    timestep: timedelta
    timestamp_field: str = config.SYSTEM_TIMESTAMP_COL
    version: str
    to_create: bool = True
    symbol_field: str = config.SYSTEM_SYMBOL_COL
    stream_fields: List[Tuple[str,...]] = []
    storage_backend: Type[StorageBase]


class InsightCfg(StreamCfg):
    # signal_name: str # = "insight name specifically"
    i_signals: List[tuple] = []
    o_desc_signals: List[tuple] = []
    o_diag_signals: List[tuple] = []
    o_pred_signals: List[tuple] = []
    o_pres_signals: List[tuple] = []

    @field_validator('signal_name')
    def valid_insight_signal_name(cls, v):
        if not v.startswith('insight_'):
            raise ValueError("signal_name should start with insight_")
        return v

    @field_validator('i_signals')
    def valid_prefix_and_type_i_signals(cls, v):
        return valid_prefix_and_type(v, prefix='i_')

    @field_validator('o_desc_signals')
    def valid_prefix_and_type_o_desc_signals(cls, v):
        return valid_prefix_and_type(v, prefix='o_desc_')

    @field_validator('o_diag_signals')
    def valid_prefix_and_type_o_diag_signals(cls, v):
        return valid_prefix_and_type(v, prefix='o_diag_')

    @field_validator('o_pred_signals')
    def valid_prefix_and_type_o_pred_signals(cls, v):
        return valid_prefix_and_type(v, prefix='o_pred_')

    @field_validator('o_pres_signals')
    def valid_prefix_and_type_o_pres_signals(cls, v):
        return valid_prefix_and_type(v, prefix='o_pres_')

    @field_validator('stream_fields')
    def contain_needed_fields_stream_fields(cls, v):
        v_set = set(v)
        names = set([item[0] for item in v])
        assert len(names) == len(v), "There is repeated fields"
        if ('rule_exp', 'text') not in v_set:
            raise ValueError(
                "('rule_exp', 'text') should be in the stream_fields")

        # if ('in_json', 'str') not in v_set:
        #    raise ValueError("('in_json', 'str') should be in the stream_fields")
        # if ('out_json', 'str') not in v_set:
        #    raise ValueError("('out_json', 'str') should be in the stream_fields")
        if ('title', 'text') not in v_set:
            raise ValueError("('title', 'str') should be in the stream_fields")
        if ('summary', 'text') not in v_set:
            raise ValueError(
                "('summary', 'str') should be in the stream_fields")
        if ('expired_date', 'timestamp') not in v_set:
            raise ValueError(
                "('expired_date', 'timestamp') should be in the stream_fields")
        if ('last_update', 'timestamp') not in v_set:
            raise ValueError(
                "('last_update', 'timestamp') should be in the stream_fields")
        return v

    def __init__(self, **data) -> None:
        super().__init__(**data)

        default_fields = [('title', 'text'),
                          ('summary', 'text'),
                          ('expired_date', 'timestamp'),
                          ('last_update', 'timestamp')]

        if 'stream_fields' in data:
            raise ValueError("You can not define stream_fields explicitly")

        self.stream_fields = default_fields

        if 'i_signals' in data:
            self.stream_fields += data['i_signals']
        if 'o_desc_signals' in data:
            self.stream_fields += data['o_desc_signals']
        if 'o_diag_signals' in data:
            self.stream_fields += data['o_diag_signals']
        if 'o_pred_signals' in data:
            self.stream_fields += data['o_pred_signals']
        if 'o_pres_signals' in data:
            self.stream_fields += data['o_pres_signals']


class MinerCfg(BaseModel):
    name: str
    description: str
    target_symbols: List[str] = []
    input_streams: List[StreamCfg] = []
    output_stream: StreamCfg
    schedule: Optional[str] = Field(None, description="cron expression")
    timestep: timedelta
    start_date: Optional[datetime] = Field(
        datetime(2013, 1, 1, 0, 0, 0), description="start date of miner")
    version: str
