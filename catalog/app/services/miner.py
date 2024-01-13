from fastapi import HTTPException
from sqlalchemy.orm import Session
from commons.miner_adapter import convert_miner_catalog_to_miner_cfg
from generators.generator import generate_miner
from commons.logger import logger
from typing import Any, List, Optional, TypedDict

from sqlalchemy import  and_, select
from sqlalchemy.dialects.postgresql import insert
from commons.file import  get_files,get_all_files


from models.miner import MinerCfgModel, MinerCode, MinerStreamRelationshipModel
from schemas.miner import BackTestRequest, Miner, MinerMetadata, MinerSetup, MinerSpec, MinerStreamRelationship, Code
from services.base import (
    BaseDataManager,
    BaseService,
)
from models.stream import StreamCfgModel
from schemas.stream import Stream, StreamBase, StreamField, StreamFormat, StreamMetadata, StreamSpec
from services.stream import StreamService




class MinerService(BaseService):
    def __init__(self,session: Session):
        super().__init__(session)
        self.MinerDataManager= MinerDataManager(session)
        self.streamService=StreamService(self.session)


    def extract_miner_spec(self,miner:MinerCfgModel,format:StreamFormat)->MinerSpec:
        input_streams,output_stream=[],None
        for stream in miner.streams:
            stream_cfg:StreamCfgModel=stream.stream_cfg
            formated_stream=self.streamService.extract_stream_spec(stream_cfg,format)
            if stream.type=="input":
                input_streams.append(formated_stream)
            else:
                output_stream=formated_stream 
        return MinerSpec(input_streams=input_streams,output_stream=output_stream)


    def get_miner(self, miner_ids: List[int]=[],miner_names: List[str]=[]) -> List[Miner]:
        miners=self.MinerDataManager.get_miner_full(miner_ids,miner_names)
        specs=[self.extract_miner_spec(miner,format=StreamFormat.Full) for miner in miners]
        return [Miner(metadata=MinerMetadata(**miner.to_dict()),spec=spec) for miner,spec in zip(miners,specs)]

    def get_all_miner(self,limit:int=10,offset:int=10) -> List[Miner]:
        miners=self.MinerDataManager.get_all_miner(limit=limit,offset=offset)
        # logger.info(f'check miners {miners.to}')
        specs=[self.extract_miner_spec(miner,format=StreamFormat.Base) for miner in miners]
        return [Miner(metadata=MinerMetadata(**miner.to_dict()),spec=spec) for miner,spec in zip(miners,specs)]
    
    def save_miner_metadata(self,minerMetadata:MinerMetadata)->Miner:
        # logger.info(f"miner:{miner.model_dump()}")
        save_miner_cfg=self.MinerDataManager.save_miner_cfg(minerMetadata)
        return self.get_miner(miner_names=[save_miner_cfg.name])[0]
    
    def save_stream_relation(self,miner_spec:MinerSpec,miner_id:int)->List[MinerStreamRelationship]:
        input_streams=self.streamService.get_streams(stream_names=[stream.metadata.name for stream in miner_spec.input_streams]) 
        output_stream=self.streamService.get_streams(stream_names=[miner_spec.output_stream.metadata.name])[0]
        stream_ids=[stream.metadata.id for stream in input_streams]+[output_stream.metadata.id]
        stream_types=["input"]*len(input_streams)+["output"]
        save_stream_spec=self.MinerDataManager.save_stream_relation(stream_ids,stream_types,miner_id)
        return [MinerStreamRelationship(**stream.to_dict()) for stream in save_stream_spec]
    
    def save_code(self,code:Code,miner_id:int)->Code:
        save_code=self.MinerDataManager.save_code(code,miner_id)
        return Code(get_input=save_code.extract_code,process_per_symbol=save_code.transform_code)
    
    def save_miner_full(self,backtestRequest:BackTestRequest)->Miner:
        minerCatalog,code=backtestRequest.minerCatalog,backtestRequest.code
      
        ###step1: create table for output stream
        output_stream=self.streamService.create_table(minerCatalog.spec.output_stream)
        ###step2: save output stream_cfg
        output_stream=self.streamService.save_stream(minerCatalog.spec.output_stream)
        # raise HTTPException(status_code=400, detail="test transaction")
        if output_stream is None:
            raise HTTPException(status_code=400, detail="output stream not found")
        # miner.metadata.file_path=file_path
        ###step3: save miner config
        miner=self.save_miner_metadata(minerCatalog.metadata)
        ###step4: save miner stream relation
        self.save_stream_relation(minerCatalog.spec,miner.metadata.id)
        ###step5: save code
        code=self.save_code(miner_id=miner.metadata.id,code=code)
        return miner

    # def verify_stream(self,stream_name:str)->Stream:
    #     streams=streamService.get_streams(stream_names=[stream_name])
    #     logger.info(f"streams {stream_name}:{streams.model_dump()}")
    #     if len(streams)==0:
    #         raise Exception(f"stream {stream_name} not found")
    #     return streams[0]
    def verify(self,miner:MinerSetup)->Miner:
        # logger.info(f"start verify")
        metadata,spec=miner.metadata,miner.spec
        input_streams=self.streamService.get_streams(stream_names=[stream for stream in spec.input_streams]) 
        # logger.info(f"return get_streams {spec.input_streams} input_streams {[stream.metadata.name for stream in input_streams]}")
        return Miner(metadata=metadata,spec={"input_streams":input_streams})
    
    def save_file(self,backtestRequest:BackTestRequest)->str:
        file_path =generate_miner(minerCatalog=backtestRequest.minerCatalog,code=backtestRequest.code)
        return file_path
    
    def get_miner_files(self,miner_names: List[str]=[])->dict:
        return get_files(folder_name='miners/',filenames=miner_names)
        
    def get_all_miner_files(self)->dict:
        return get_all_files(folder_name='miners/')
    
class MinerDataManager(BaseDataManager):
        

    def get_miner_full(self,miner_ids:List[int]=[],miner_names:List[str]=[])->List[MinerCfgModel]:
        conditions=[]
        logger.info(f"show miner_ids:{miner_ids},miner_names:{miner_names}")
        if len(miner_ids)>0:
            conditions.append(MinerCfgModel.id.in_(miner_ids))
        if len(miner_names)>0:
            conditions.append(MinerCfgModel.name.in_(miner_names))
        query=self.session.query(MinerCfgModel).where(and_(*conditions))
        return self.get_all(query)
    
    
    def get_all_miner(self,limit:int=10,offset:int=0) -> List[MinerCfgModel]:
        query = select(MinerCfgModel).limit(limit).offset(offset).order_by(MinerCfgModel.id)
        return self.get_all(query)
        
    def save_miner_cfg(self,minerCatalog:MinerMetadata)->MinerCfgModel:
        miner_cfg_dict=convert_miner_catalog_to_miner_cfg(minerCatalog)
        insert_stmt = insert(MinerCfgModel.__table__).values(miner_cfg_dict)
        insert_stmt=insert_stmt.on_conflict_do_update(  index_elements=[MinerCfgModel.name],
                                                        set_={col: getattr(insert_stmt.excluded, col) for col in miner_cfg_dict})
        insert_stmt=insert_stmt.returning(MinerCfgModel)
        results=self.session.execute(insert_stmt).fetchone()
        return MinerCfgModel(**dict(results._mapping))
    
    def save_stream_relation(self,stream_ids:List[int],stream_types:List[str],miner_id:int)->List[MinerStreamRelationshipModel]:
        insert_stmt = insert(MinerStreamRelationshipModel.__table__).values(
            [{'miner_id':miner_id,'stream_id':stream_id,'type':stream_type} for (stream_id,stream_type) in zip(stream_ids,stream_types)])
        insert_stmt=insert_stmt.on_conflict_do_update(  
            index_elements=[MinerStreamRelationshipModel.stream_id,MinerStreamRelationshipModel.type,MinerStreamRelationshipModel.miner_id],
            set_={col: getattr(insert_stmt.excluded, col) for col in MinerStreamRelationship.__fields__})
        insert_stmt=insert_stmt.returning(MinerStreamRelationshipModel)
        results=self.session.execute(insert_stmt).fetchall()
        return list(MinerStreamRelationshipModel(**dict(row._mapping)) for row in results)
    
    def save_code(self,code:Code,miner_id:int)->MinerCode:
        insert_stmt = insert(MinerCode.__table__).values(
            {'miner_id':miner_id,'extract_code':code.get_input,'transform_code':code.process_per_symbol})
        insert_stmt=insert_stmt.on_conflict_do_update(  
            index_elements=[MinerCode.id],
            set_={'extract_code':insert_stmt.excluded.extract_code,'transform_code':insert_stmt.excluded.transform_code})
        insert_stmt=insert_stmt.returning(MinerCode)
        results=self.session.execute(insert_stmt).fetchone()
        return MinerCode(**dict(results._mapping))