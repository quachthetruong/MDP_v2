from datetime import datetime, timedelta
from typing import Dict, List
from miners.processor.minerProcessor import MinerProcessor
from schemas.stream import StreamCatalog
from streams.stream_cfg import StreamCfg
from streams.data_stream_base import DataStreamBase
from schemas.miner import MinerCatalog
from storage.database_storage import DatabaseStorage
from storage.encap_database_storage import EncapDatabaseStorage
from storage.redshift_storage import RedshiftStorage
from enum import Enum





class StorageType(Enum):
    # storage = {"DatabaseStorage":DatabaseStorage,"EncapDatabaseStorage":EncapDatabaseStorage,"RedshiftStorage":RedshiftStorage}
    DatabaseStorage = DatabaseStorage
    EncapDatabaseStorage = EncapDatabaseStorage
    RedshiftStorage = RedshiftStorage

class SetUpProcessor(MinerProcessor):
    def __init__(self, name="setup"):
        super().__init__(name)

    def convert_catalog_to_stream_cfg(self, input_streams: List[StreamCatalog]) -> List[StreamCfg]:
        return [StreamCfg(signal_name=input_stream.metadata.signal_name,
                  same_table_name=input_stream.metadata.same_table_name,
                  timestep=timedelta(**input_stream.metadata.timestep),
                  version=input_stream.metadata.version,
                  timestamp_field=input_stream.metadata.timestamp_field,
                  symbol_field=input_stream.metadata.symbol_field,
                  to_create=input_stream.metadata.to_create,
                  stream_fields = [(stream_field.name,stream_field.type) for stream_field in input_stream.spec.stream_fields],
                  storage_backend=StorageType[input_stream.metadata.storage_backend].value)
               for input_stream in input_streams]
    
    def init_input_streams(self,input_streams:List[StreamCatalog]) -> Dict[str, DataStreamBase]:
        input_cfg = self.convert_catalog_to_stream_cfg(input_streams)
        input_streams = {}
        for cfg in input_cfg:
            input_stream = DataStreamBase.from_config(stream_cfg=cfg)
            input_streams[input_stream.signal_name] = input_stream
        return input_streams

    def execute(self,timestamp:datetime,data:List[StreamCatalog]=[])->Dict[str,DataStreamBase]:
        if not data:  data = self.miner_config.spec.input_streams
        return self.init_input_streams(input_streams=data)
        