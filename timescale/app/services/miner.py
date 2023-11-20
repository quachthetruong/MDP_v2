import gzip
import json
import logging
import pprint
from fastapi import HTTPException
import requests
from sqlalchemy.orm import Session
from schemas.miner import HashData
from commons.logger import logger
from typing import Any, Dict, List, Union
from sqlalchemy.sql import text
from services.base import BaseService, BaseDataManager
from schemas.miner import MinerCatalog, MinerSpec
from schemas.miner import Miner
from sqlalchemy import select
from schemas.stream import StreamCatalog
from services.stream import StreamService
from schemas.miner_unit import DetailStage
from schemas.other import ValidateForm
import config
from schemas.miner import Code
from commons.celery_utils import get_celery_cached_result, hash_celery_task_id
from validator.exception import miner_exception_handler
from worker import celery
from celery.utils.serialization import UnpickleableExceptionWrapper


class MinerService(BaseService):
    def __init__(self, session: Session):
        super().__init__(session)
        self.minerDataManager = MinerDataManager(session)
        self.streamService = StreamService(session)

    def reraise_celery_exception(info):
        exec("raise {class_name}('{message}')".format(class_name=info.__class__.__name__, message=info.__str__()))

    def setUp(self, miner_config: MinerCatalog) -> Miner:
        minerCatalog = self.getCatalog(miner_config)
        reOrderStreamCatalog = []
        for stream_cfg in miner_config.spec.input_streams:
            for stream_catalog in minerCatalog.spec.input_streams:
                if stream_cfg.name == stream_catalog.metadata.name:
                    reOrderStreamCatalog.append(stream_catalog)
                    break
        minerCatalog.spec.input_streams = reOrderStreamCatalog
        data = [self.streamService.getData(streamCatalog=streamCatalog, minerCatalog=minerCatalog)
                for streamCatalog in minerCatalog.spec.input_streams]
        miner = Miner(catalog=minerCatalog, data=data)

        return miner

    def getCatalog(self, miner_config: MinerCatalog) -> MinerCatalog:
        res = requests.post(config.CATALOG_SERVICE_URL +
                            "miner/verify", json=miner_config.model_dump())
        return MinerCatalog(**res.json())
    

    def test_get_input(self, miner_config: MinerCatalog, code: Code) -> List[DetailStage]:
        try:
            hashData=HashData(miner_config=miner_config,code=code,route="get_input")
            task_id=hash_celery_task_id(hashData)
            # if value:=get_celery_cached_result(celery,task_id):
            #     result=value["result"] #celery return task_id, result, status, traceback, children, date_done
            # else:
            task = celery.send_task('tasks.mimic_get_input',task_id=task_id, kwargs={"miner_config":miner_config.model_dump(),"code":code.model_dump()})
            result=task.get()
            # detail_stages_json_str=gzip.decompress(result).decode()
            # detail_stages_json=eval(detail_stages_json_str)
            return [DetailStage.model_validate_json(item) for item in result]
        except Exception as e:
            errors=eval(e.__str__())
            logger.error(f"error catch get_input {type(errors)} {errors}")
            raise HTTPException(status_code=501, detail=errors)

    
    def test_process(self, miner_config: MinerCatalog, code: Code) -> List[DetailStage]:
        try:
            hashData=HashData(miner_config=miner_config,code=code,route="process")
            task_id=hash_celery_task_id(hashData)
            # logger.info(f"task_id value {task_id}")

            # if value:=get_celery_cached_result(celery,task_id):
            #     result=value["result"] #celery return task_id, result, status, traceback, children, date_done
            # else:
            task = celery.send_task('tasks.mimic_process',task_id=task_id, kwargs={"miner_config":miner_config.model_dump(),"code":code.model_dump()})
            result=task.get()
            # detail_stages_json_str=gzip.decompress(result).decode()
            # detail_stages_json=eval(detail_stages_json_str)
            return [DetailStage.model_validate_json(item) for item in result]
        except Exception as e:
            errors=eval(e.__str__())
            raise HTTPException(status_code=501, detail=errors)

    def validate(self, validateForm: ValidateForm):
        logging.info(f"validateForm {validateForm}")


class MinerDataManager(BaseDataManager):

    def getData(self, streamCatalog: StreamCatalog, minerCatalog: MinerCatalog) -> List[Any]:
        users_table = self.metadata_obj.tables[streamCatalog.metadata.name.split(
            '.', 1)[-1]]
        symbol_list = tuple(minerCatalog.metadata.target_symbols)
        query = select(users_table).where(text(
            f'{streamCatalog.metadata.symbol_field} in {symbol_list}')).limit(10).offset(0)
        results = self.session.execute(query).all()
        return [row._asdict() for row in results]
        # logger.info(f'checknay {query}')
        # return []
