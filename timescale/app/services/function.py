import celery
from fastapi import HTTPException
import pandas as pd
import psycopg2
from sqlalchemy.orm import Session
from commons.logger import logger
from typing import Any, Dict, Iterator, List
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, select
from sqlalchemy.sql import text
from sqlalchemy import Column
from services.base import BaseService, BaseDataManager
from schemas.miner import HashData, MinerCatalog
from schemas.stream import StreamCatalog
from common.template_loader import TemplateLoader
import config
import redshift_connector

from schemas.stream_field import StreamField
from commons.celery_utils import hash_celery_task_id
from schemas.miner_unit import DetailStage
from schemas.function import Function

# from backend.redshift_session import create_redshift_session


class FunctionService():
    def get_record(self, miner_config: MinerCatalog,**kwargs) -> List[DetailStage]:
        try:
            hashData=HashData(miner_config=miner_config,body=kwargs,route="get_input")
            task_id=hash_celery_task_id(hashData)
            logger.info(f"task_id {task_id}")
            # if value:=get_celery_cached_result(celery,task_id):
            #     result=value["result"] #celery return task_id, result, status, traceback, children, date_done
            # else:
            task = celery.send_task('tasks.extract_input',task_id=task_id, kwargs={"miner_config":miner_config.model_dump(),"body":kwargs})
            result=task.get()
            # detail_stages_json_str=gzip.decompress(result).decode()
            # detail_stages_json=eval(detail_stages_json_str)
            return [DetailStage.model_validate_json(item) for item in result]
        except Exception as e:
            errors=eval(e.__str__())
            logger.error(f"error catch get_input {type(errors)} {errors}")
            raise HTTPException(status_code=501, detail=errors)