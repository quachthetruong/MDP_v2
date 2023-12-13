import requests
from sqlalchemy.orm import Session
from commons.logger import logger
from typing import  List, Union
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, select

from models.stream import StreamCfgModel, StreamFieldsModel
from schemas.stream import Stream, StreamBase, StreamField, StreamFormat, StreamMetadata, StreamSpec
from services.base import (
    BaseDataManager,
    BaseService,
)
import config
class StreamService(BaseService):
    def __init__(self,session: Session):
        super().__init__(session)
        self.streamDataManager= StreamDataManager(session)

    def extract_stream_spec(self,stream_cfg:StreamCfgModel,format:StreamFormat)->Union[Stream,StreamMetadata,StreamBase]:
        if format==StreamFormat.Metadata:
                formated_stream=StreamMetadata(**stream_cfg.to_dict())
        elif format==StreamFormat.Base:
            formated_stream=StreamBase(**stream_cfg.to_dict())
        elif format==StreamFormat.Full:
            stream_fields=[StreamField(**stream_field.to_dict()) for stream_field in stream_cfg.stream_fields]
            metadata=StreamMetadata(**stream_cfg.to_dict())
            spec=StreamSpec(stream_fields=stream_fields)
            formated_stream=Stream(metadata=metadata,spec=spec)
        return formated_stream


    def get_streams(self, stream_ids: List[int]=[],stream_names: List[str]=[]) -> List[Stream]:
        streams=self.streamDataManager.get_stream_full(stream_ids,stream_names)
        return [self.extract_stream_spec(stream,format=StreamFormat.Full) for stream in streams]

    def get_all_stream(self,limit:int=10,offset:int=0) -> List[StreamMetadata]:
        listStreamMetadata=self.streamDataManager.get_all_spec(limit=limit,offset=offset)
        return [self.extract_stream_spec(stream,format=StreamFormat.Full) for stream in listStreamMetadata]
    
    def save_stream(self,stream:Stream)->Stream:
        metadata,spec=stream.metadata,stream.spec
        stream_metadata=self.streamDataManager.save_stream_cfg(metadata)
        for stream_field in spec.stream_fields:
            stream_field.stream_id=stream_metadata.id
        # logger.info(f"stream_metadata:{len(spec.stream_fields)}")
        stream_fields=self.streamDataManager.save_stream_fields(spec.stream_fields)
        return Stream(metadata=StreamMetadata(**stream_metadata.to_dict()),
                      spec=StreamSpec(stream_fields=[StreamField(**stream_field.to_dict()) for stream_field in stream_fields]))

    def save_metadata(self,metadata:StreamMetadata)->StreamMetadata:
        stream_metadata=self.streamDataManager.save_stream_cfg(metadata)
        return StreamMetadata(**stream_metadata.to_dict())
    
    def save_stream_fields(self,stream_fields:List[StreamField]=[])->List[StreamField]:
        stream_field_models=self.streamDataManager.save_stream_fields(stream_fields)
        return [StreamField(**stream_field.to_dict()) for stream_field in stream_field_models]
    
    def verify(self,stream:Stream)->Stream:
        metadata,spec=stream.metadata,stream.spec
        return Stream(metadata=metadata,spec=spec)
    
    def create_table(self,stream:Stream):
        res=requests.post(config.TIMESCALE_SERVICE_URL+"stream/create_table",json=stream.dict())
        logger.info(f"create_table {res.json()}")
    
class StreamDataManager(BaseDataManager):
        
    def get_stream_full(self,stream_ids:List[int]=[],stream_names:List[str]=[])->List[StreamCfgModel]:
        conditions=[]
        if len(stream_ids)>0:
            conditions.append(StreamCfgModel.id.in_(stream_ids))
        if len(stream_names)>0:
            conditions.append(StreamCfgModel.name.in_(stream_names))
        logger.info(f"conditions:{and_(*conditions)}")
        query = select(StreamCfgModel).where(and_(*conditions))
        return self.get_all(query)

    def get_stream_cfg(self,stream_id:int)->StreamCfgModel:
        query = select(StreamCfgModel).where(StreamCfgModel.id == stream_id)
        return self.get_one(query)
    
    def get_stream_fields(self,stream_id:int)->List[StreamFieldsModel]:
        query = select(StreamFieldsModel).where(StreamFieldsModel.stream_id == stream_id)
        return self.get_all(query)
    
    def get_all_spec(self,limit:int=10,offset:int=0) -> List[StreamCfgModel]:
        query = select(StreamCfgModel).limit(limit).offset(offset).order_by(StreamCfgModel.id)
        logger.info(f"query:{type(query)}{query}")
        return self.get_all(query)
    
    def save_stream_cfg(self,stream_cfg:StreamMetadata)->StreamCfgModel:
        stream_cfg_dict=stream_cfg.model_dump()
        stream_cfg_dict['timestep_hours']=stream_cfg_dict['timestep']['hours']
        stream_cfg_dict['timestep_days']=stream_cfg_dict['timestep']['days']
        stream_cfg_dict['timestep_minutes']=stream_cfg_dict['timestep']['minutes']
        del stream_cfg_dict['timestep'],stream_cfg_dict['to_create'],stream_cfg_dict['id']
        insert_stmt = insert(StreamCfgModel.__table__).values(stream_cfg_dict)
        insert_stmt=insert_stmt.on_conflict_do_update(  index_elements=[StreamCfgModel.name],
                                                        set_={col: getattr(insert_stmt.excluded, col) for col in stream_cfg_dict})
        insert_stmt=insert_stmt.returning(StreamCfgModel)
        results=self.session.execute(insert_stmt).fetchone()
        return StreamCfgModel(**dict(results._mapping))
    
    def save_stream_fields(self,stream_fields:List[StreamField])->List[StreamFieldsModel]:
        if len(stream_fields)==0:
            return []
        insert_stmt = insert(StreamFieldsModel.__table__).values([{**stream_field.model_dump()} for stream_field in stream_fields])
        insert_stmt=insert_stmt.on_conflict_do_update(  index_elements=[StreamFieldsModel.stream_id,StreamFieldsModel.name],
                                                        set_={col: getattr(insert_stmt.excluded, col) for col in StreamField.__fields__})
        insert_stmt=insert_stmt.returning(StreamFieldsModel)
        results=self.session.execute(insert_stmt).fetchall()
        return list(StreamFieldsModel(**dict(row._mapping)) for row in results)