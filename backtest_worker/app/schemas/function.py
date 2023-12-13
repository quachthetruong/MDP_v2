from typing import List
# from schemas.other import Function,Param
from typing import Any, Generic, List, TypeVar

from pydantic import BaseModel

from schemas.miner import MinerCatalog

# T = TypeVar("T")
# class Param(Generic[T]):
#     name:str
#     type:str
#     value:T
#     def __init__(self,name:str,value:T):
#         self.name=name
#         self.value=value
#         self.type=T.__name__
# class Function:
#     name:str
#     params:List[Param]
#     def __init__(self,name:str,params:List[Param]):
#         self.name=name
#         self.params=params
    
# params=[Param[str](name="symbol_column",value="symbol_"),
#         Param[str](name="timestamp_column",value="indexed_timestamp_"),
#         Param[List[str]](name="target_symbols",value="[VND,VUA]"),
#         Param[str](name="indexed_timestamp",value="2020-01-01 00:00:00")]
# get_record=Function(name="get_record",params=params)

class Param(BaseModel):
    name:str
    type:str
    value:str

        
class Function(BaseModel):
    name:str
    params:List[Param]


class StreamExtract(BaseModel):
    name: str
    source: str
    function: Function

class MinerExtract(BaseModel):
    minerCatalog:MinerCatalog
    StreamExtracts:List[StreamExtract]
