from datetime import datetime
import os
import time
from celery import Celery
import logging

import requests
from miners.processor.transformProcessor import TransformProcessor
from miners.simulator.backtest_simulator import BacktestSimulator
from miners.processor.extractProcessor import ExtractProcessor
from miners.processor.setUpProcessor import SetUpProcessor
from miners.pipeline.minerPipeline import MinerPipeline
from schemas.exception import InvalidNode

from generators.generator import generate_miner, get_class
from schemas.miner import Code, MinerCatalog
from miners.miner_back_test_base import MinerBackTestBase
from commons.detail_transformer import DetailTransformer

import gzip
logger=logging.getLogger(__name__)

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

@celery.task(name='tasks.hello')
def hello():
    try: 
        logger.info(f"conf: {celery.conf}")
        print("hello world dc r print")
        value= gzip.compress(str(celery.conf).encode())

        return value
    except Exception as e:
        print(f"kiem tra hello {e}")
        raise e
        return str(e)

@celery.task(name='tasks.mimic_get_input')
def mimic_get_input(miner_config, code):
    try:
        # logging.info(f"miner_config {miner_config}")
        miner_config=MinerCatalog(**miner_config)
        code=Code(**code)
        ### miner base
        pipeline=MinerPipeline(miner_config=miner_config)
        ### __init__
        pipeline.add_processer(SetUpProcessor())
        ### get_input
        pipeline.add_processer(ExtractProcessor(code=code.get_input))
        ### mimic_backrun
        backtest_simulator=BacktestSimulator(
            schedule=miner_config.metadata.schedule, 
            start_date=datetime(**miner_config.metadata.start_date),
            end_date=datetime.now())
        
        stages = backtest_simulator.mimic_backrun(pipeline=pipeline)
        
        ### split result into multiple symbol
        detail_stages= DetailTransformer(stages=stages, target_symbols=miner_config.metadata.target_symbols).transform()
        detail_stages_json=[detail_stage.model_dump_json() for detail_stage in detail_stages]
        # logger.info(f"detail_stages_json {type(detail_stages_json)} {str(detail_stages_json)[:100]}")
        # detail_stages_gzip=gzip.compress(str(detail_stages_json).encode())
        return detail_stages_json
    except Exception as e:
        logger.error(f"error catch {str(e)}")
        raise e
    
@celery.task(name='tasks.mimic_process')
def mimic_process(miner_config, code):
    try:
        miner_config=MinerCatalog(**miner_config)
        code=Code(**code)
        ### miner base
        pipeline=MinerPipeline(miner_config=miner_config)
        ### __init__
        pipeline.add_processer(SetUpProcessor())
        ### get_input
        pipeline.add_processer(ExtractProcessor(code=code.get_input))
        ### process
        pipeline.add_processer(TransformProcessor(code=code.process_per_symbol))
        ### mimic_backrun
        backtest_simulator=BacktestSimulator(
            schedule=miner_config.metadata.schedule, 
            start_date=datetime(**miner_config.metadata.start_date),
            end_date=datetime.now())
        
        stages = backtest_simulator.mimic_backrun(pipeline=pipeline)
        
        ### split result into multiple symbol
        detail_stages= DetailTransformer(stages=stages, target_symbols=miner_config.metadata.target_symbols).transform()
        detail_stages_json=[detail_stage.model_dump_json() for detail_stage in detail_stages]
        # detail_stages_gzip=gzip.compress(str(detail_stages_json).encode())
        # return detail_stages_gzip
        return detail_stages_json
    except Exception as e:
        logger.error(f"error catch {str(e)}")
        raise e