import os
import time
from celery import Celery

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://redis:6379'),
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://redis:6379')

class CeleryConfig:
    # task_serializer = "pickle"
    # result_serializer = "pickle"
    # event_serializer = "json"
    # accept_content = ["application/json", "application/x-python-serialize"]
    # result_accept_content = ["application/json", "application/x-python-serialize"]
    task_compression = 'gzip'

celery = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)# celery.config_from_object(CeleryConfig)
celery.conf.update(task_compression="gzip")
celery.conf.update(result_compression="gzip")