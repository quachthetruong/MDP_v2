import logging
from typing_extensions import TypedDict
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Any, List, Optional
from schemas.other import TimeStep
import config
from commons.utils import generated_identified_name
from schemas.storage_base import StorageType
from pydantic import validator
import logging
from schemas.stream_field import StreamField


valid_types = set(['text', 'timestamp', 'numeric', 'float'])


class StreamBase(BaseModel):
    name: str
    id: Optional[int] = Field(None, description="auto increment id")

    class Config:
        arbitrary_types_allowed = True


class StreamMetadata(StreamBase):
    signal_name: str
    same_table_name: Optional[bool] = Field(
        False, description="if False, table name will be generated from signal_name, timestep and version")
    description: Optional[str] = Field(
        "No description", description="not required")
    timestep: TimeStep = {"days": 1, "hours": 0, "minutes": 0}
    timestamp_field: str = config.SYSTEM_TIMESTAMP_COL
    version: str
    to_create: bool = False
    symbol_field: str = config.SYSTEM_SYMBOL_COL
    storage_backend: str

    @field_validator('storage_backend', mode='before')
    @classmethod
    def check_in_storage_type(cls, storage_backend):
        if storage_backend not in StorageType.__members__:
            raise ValueError(
                f"storage_backend must be in {StorageType.__members__}")
        return storage_backend

    @field_validator('version', mode='before')
    @classmethod
    def check_version_type_str(cls, version):
        if isinstance(version, str):
            return version
        if isinstance(version, int):
            version = str(version)
        return version

    @model_validator(mode='before')
    def identified_name(cls, data):
        if 'name' in data and data['name']:
            return data
        if 'same_table_name' in data and data['same_table_name']:
            data['name'] = data['signal_name']
        else:
            # logging.info(
            #     f"signal_name: {data['signal_name']} timestep: {data['timestep']} version: {data['version']}")
            data['name'] = generated_identified_name(
                data['signal_name'], data['timestep'], data['version'])
        return data


class StreamSpec(BaseModel):
    stream_fields: List[StreamField] = []


class StreamCatalog(BaseModel):
    kind: str = "stream"
    metadata: StreamMetadata
    spec: StreamSpec


class Stream(BaseModel):
    catalog: StreamCatalog
    data: List[Any] = []
