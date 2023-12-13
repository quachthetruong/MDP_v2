
from typing import Any, List, Optional, Union
from pydantic import BaseModel, Field, field_validator

from cron_converter import Cron
from schemas.stream import Stream, StreamBase, StreamCatalog, StreamMetadata
from schemas.other import TimeStep, StartDate


class MinerMetadata(BaseModel):
    id: Optional[int] = Field(None, description="auto increment id")
    name: str
    description: str
    target_symbols: List[str]
    timestep: TimeStep = {"days": 1, "hours": 0, "minutes": 0}
    start_date: StartDate = {"day": 1, "month": 1, "year": 2023, "hour": 0}
    schedule: Optional[str] = Field(
        None, description="schedule time to run miner")
    file_path: Optional[str] = Field(None, description="miner path")

    @field_validator('target_symbols', mode='before')
    @classmethod
    def check_target_symbols(cls, target_symbols):
        if target_symbols is None:
            target_symbols = []
        if isinstance(target_symbols, str):
            target_symbols = [item for item in filter(
                None, target_symbols.split(','))]
        return target_symbols

    @field_validator('schedule', mode='before')
    @classmethod
    def check_schedule_format(cls, schedule):
        if schedule is None or schedule == "":
            return None
        if not isinstance(schedule, str):
            raise ValueError("schedule must be string or None")
        cron_instance = Cron()
        cron_instance.from_string(schedule)
        return schedule
    
    @field_validator('start_date', mode='after')
    @classmethod
    def convert_month_js_py(cls, start_date):
        if start_date is None:
            return None
        if not isinstance(start_date, dict):
            raise ValueError("start_date must be dict or None")
        if "month" in start_date:
            start_date["month"]+=1#javascript month start from 0 to 11, python month start from 1 to 12
        return start_date
        


class MinerSpec(BaseModel):
    input_streams: List[Union[StreamCatalog, StreamMetadata, StreamBase]] = []
    output_stream: Optional[Union[StreamCatalog,
                                  StreamMetadata, StreamBase]] = None

class MinerSetupSpec(BaseModel):
    input_streams: List[str] = []
    
class MinerSetupCatalog(BaseModel):
    kind:str="miner"
    metadata:MinerMetadata
    spec:MinerSetupSpec

class MinerCatalog(BaseModel):
    kind: str = "miner"
    metadata: MinerMetadata
    spec: MinerSpec


class MinerStreamRelationship(BaseModel):
    stream_id: int
    miner_id: int
    type: str


class Miner(BaseModel):
    catalog: MinerCatalog
    streams:List[Stream]


class Code(BaseModel):
    get_input: str
    process_per_symbol: str

class HashData(BaseModel):
    route:str
    miner_config:MinerCatalog
    body:Optional[Any]=None