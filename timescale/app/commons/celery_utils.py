import hashlib
from schemas.miner import HashData
from celery import Celery
from celery.backends.redis import RedisBackend
from commons.logger import logger
import json

def hash_celery_task_id(hashData:HashData)->str:
    task_id= hashlib.md5(f'{hashData.model_dump()}'.encode()).hexdigest()
    logger.info(f"task_id {task_id}")
    return task_id

def get_celery_cached_result(celery:Celery,task_id:str):
    redis_backend:RedisBackend=celery.backend
    key=f"celery-task-meta-{task_id}"
    result= redis_backend.get(key=key)
    return result