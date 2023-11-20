from typing import Dict, Optional
from pydantic import BaseModel, Field, validator

dtype_mapping = {
    'object': 'text',
    'int64': 'bigint',
    'float64': 'numeric',
    'bool': 'boolean',
    'datetime64[ns]': 'timestamp'
}

reverse_dtype_mapping = {
    'text': 'object',
    'bigint': 'int64',
    'numeric': 'float64',
    'boolean': 'bool',
    'timestamp': 'datetime64[ns]'
}

def map_type(type: str,mapping:Dict[str,str]):
    if type in mapping:
        return mapping[type]
    return type

class StreamField(BaseModel):
    stream_id: Optional[int]=Field(None, description="not required")
    name: Optional[str]
    type: Optional[str]
    is_nullable: Optional[bool]=Field(True, description="not required")
    is_primary_key: Optional[bool]=Field(False, description="not required")
    @validator('type',pre=True,always=False)
    @classmethod
    def check_postgres_type(cls, type):
        return map_type(type=type,mapping=dtype_mapping)
    