import hashlib
from schemas.miner import HashData
from celery import Celery
from celery.backends.redis import RedisBackend
import logging
import json
from celery_instance import celery

def hash_celery_task_id(hashData:HashData)->str:
    task_id= hashlib.md5(f'{hashData.model_dump()}'.encode()).hexdigest()
    logging.info(f"task_id {task_id}")
    return task_id

def get_celery_cached_result(task_id:str):
    redis_backend:RedisBackend=celery.backend
    key=f"celery-task-meta-{task_id}"
    result= redis_backend.get(key=key)
    return result