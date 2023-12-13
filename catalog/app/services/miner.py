from fastapi import HTTPException
from sqlalchemy.orm import Session
from generators.generator import generate_miner
from commons.logger import logger
from typing import Any, List, Optional, TypedDict

from sqlalchemy import  and_, select
from sqlalchemy.dialects.postgresql import insert
from commons.file import  get_files,get_all_files


from models.miner import MinerCfgModel, MinerStreamRelationshipModel
from schemas.miner import Miner, MinerMetadata, MinerSetup, MinerSpec, MinerStreamRelationship, Code
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
    
    def save_miner(self,miner:Miner)->Miner:
        # logger.info(f"miner:{miner.model_dump()}")
        metadata,spec=miner.metadata,miner.spec
        save_miner_cfg=self.MinerDataManager.save_miner_cfg(metadata)
        save_stream_spec=self.save_stream_relation(spec,save_miner_cfg.id)

        return self.get_miner(miner_names=[save_miner_cfg.name])[0]
    
    def save_stream_relation(self,miner_spec:MinerSpec,miner_id:int)->List[MinerStreamRelationship]:
        input_streams=self.streamService.get_streams(stream_names=[stream.metadata.name for stream in miner_spec.input_streams]) 
        output_stream=self.streamService.get_streams(stream_names=[miner_spec.output_stream.metadata.name])[0]
        stream_ids=[stream.metadata.id for stream in input_streams]+[output_stream.metadata.id]
        stream_types=["input"]*len(input_streams)+["output"]
        save_stream_spec=self.MinerDataManager.save_stream_relation(stream_ids,stream_types,miner_id)
        return [MinerStreamRelationship(**stream.to_dict()) for stream in save_stream_spec]
    
    def save_miner_full(self,miner:Miner,code:Code)->Miner:
        ###step1: save miner file code
        file_path=self.save_file(miner_config=miner,code=code)
        ###step2: create table for output stream
        output_stream=self.streamService.create_table(miner.spec.output_stream)
        ###step3: save output stream_cfg
        output_stream=self.streamService.save_stream(miner.spec.output_stream)
        # raise HTTPException(status_code=400, detail="test transaction")
        if output_stream is None:
            raise HTTPException(status_code=400, detail="output stream not found")
        miner.metadata.file_path=file_path
        ###step4: save miner config
        miner=self.save_miner(miner)
        return miner

    # def verify_stream(self,stream_name:str)->Stream:
    #     streams=streamService.get_streams(stream_names=[stream_name])
    #     logger.info(f"streams {stream_name}:{streams.model_dump()}")
    #     if len(streams)==0:
    #         raise Exception(f"stream {stream_name} not found")
    #     return streams[0]
    def verify(self,miner:MinerSetup)->Miner:
        logger.info(f"start verify")
        metadata,spec=miner.metadata,miner.spec
        input_streams=self.streamService.get_streams(stream_names=[stream for stream in spec.input_streams]) 
        logger.info(f"return get_streams")
        return Miner(metadata=metadata,spec={"input_streams":input_streams})
    
    def save_file(self,miner_config:Miner,code:Code)->str:
        file_path =generate_miner(minerCatalog=miner_config,get_inputs_str=code.get_input,
                                       process_per_symbol_str=code.process_per_symbol)
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
        
    def save_miner_cfg(self,miner_cfg:MinerMetadata)->MinerCfgModel:
        miner_cfg_dict=miner_cfg.model_dump()
        miner_cfg_dict['timestep_hours']=miner_cfg_dict['timestep']['hours']
        miner_cfg_dict['timestep_days']=miner_cfg_dict['timestep']['days']
        miner_cfg_dict['timestep_minutes']=miner_cfg_dict['timestep']['minutes']
        miner_cfg_dict['start_date_month']=miner_cfg_dict['start_date']['month']
        miner_cfg_dict['start_date_day']=miner_cfg_dict['start_date']['day']
        miner_cfg_dict['start_date_year']=miner_cfg_dict['start_date']['year']
        miner_cfg_dict['start_date_hour']=miner_cfg_dict['start_date']['hour']
        del miner_cfg_dict['timestep'],miner_cfg_dict['id'],miner_cfg_dict['start_date']
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