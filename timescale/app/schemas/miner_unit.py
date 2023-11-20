from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pandas import DataFrame
from pydantic import BaseModel, Field, model_validator
from schemas.other import StartDate
from schemas.stream_field import StreamField

class MiniNode(BaseModel):
    symbol: str
    data: List[Dict]


class DetailNode(BaseModel):
    name: str
    source: List[str]
    columns: Optional[List[StreamField]] = Field(
        [], description="columns of node")
    mini_nodes: List[MiniNode]


class Node(BaseModel):
    name: str
    source: Optional[List[str]] = Field([], description="source of node")
    dataframe: DataFrame

    @model_validator(mode='before')
    def identified_name(cls, v):
        if 'source' not in v and 'dataframe' in v:
            v['source'] = [v['dataframe'].__doc__] if v['dataframe'].__doc__ else []
        return v

    class Config:
        arbitrary_types_allowed = True


class Stage(BaseModel):
    timestamp: datetime
    start_date: StartDate
    schedule: str
    nodes: Dict[str, Node] = {}


class DetailStage(BaseModel):
    timestamp: datetime
    start_date: StartDate
    schedule: str
    nodes: List[DetailNode] = []