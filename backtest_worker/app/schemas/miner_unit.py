from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pandas import DataFrame
from pydantic import BaseModel, Field, model_validator
from schemas.other import ScheduleDate
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

class StageMetadata(BaseModel):
    schedule: Optional[str] = Field(None, description="schedule time to run miner")
    start_date: Optional[ScheduleDate] = Field({"day": 1, "month": 1, "year": 2023, "hour": 0})
    end_date: Optional[ScheduleDate] = Field({"day": 1, "month": 1, "year": 2024, "hour": 0})

class Stage(BaseModel):
    timestamp: datetime
    nodes: Dict[str, Node] = {}


class DetailStage(BaseModel):
    timestamp: datetime
    nodes: List[DetailNode] = []

class BacktestResult(BaseModel):
    metadata: StageMetadata
    stages: List[DetailStage]
    log: str = Field("", description="logs of miner")