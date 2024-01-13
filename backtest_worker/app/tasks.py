from datetime import datetime

import logging
from celery_instance import celery

from schemas.function import StreamExtract
from miners.processor.transformProcessor import TransformProcessor
from miners.simulator.backtest_simulator import BacktestSimulator
from miners.processor.extractProcessor import ExtractProcessor
from miners.processor.setUpProcessor import SetUpProcessor
from miners.pipeline.minerPipeline import MinerPipeline
from schemas.exception import InvalidNode
from schemas.miner import Code, MinerCatalog
from commons.detail_transformer import DetailTransformer

import gzip
logger=logging.getLogger(__name__)



@celery.task(name='tasks.health_check')
def health_check():
    try: 
        print("health_check")
        return "health_check"
    except Exception as e:
        print(f"error catch {str(e)}")
        raise e
    
@celery.task(name='tasks.extract')
def extract(miner_config,extract_streams):
    try:
        miner_config=MinerCatalog(**miner_config)
        extract_streams=[StreamExtract(**extract_stream) for extract_stream in extract_streams]
        # ### miner base
        pipeline=MinerPipeline(miner_config=miner_config)
        print("pipeline")
        # ### __init__
        pipeline.add_processer(SetUpProcessor())
        print("SetUpProcessor")
        # ### get_input
        pipeline.add_processer(ExtractProcessor(extract_streams=extract_streams))
        print("ExtractProcessor")
        # logging.info(f"mimic_backrun {miner_config.metadata.start_date}")

        # ### run
        backtest_simulator=BacktestSimulator(
            schedule=miner_config.metadata.schedule, 
            start_date=datetime(**miner_config.metadata.start_date),
            end_date=datetime.now())
        
        # print(f"BacktestSimulator {backtest_simulator}")
        
        stages = backtest_simulator.mimic_backrun(pipeline=pipeline)
        # ### split result into multiple symbol
        # detail_stages= DetailTransformer(stages=pipeline.stages, target_symbols=miner_config.metadata.target_symbols).transform()
        # detail_stages_json=[detail_stage.model_dump_json() for detail_stage in detail_stages]
        # # detail_stages_gzip=gzip.compress(str(detail_stages_json).encode())
        # return detail_stages_json
    except Exception as e:
        logger.error(f"error catch {str(e)}")
        raise e

@celery.task(name='tasks.mimic_get_input')
def mimic_get_input(miner_config, code):
    try:
        # logging.info(f"miner_config {miner_config}")
        miner_config=MinerCatalog(**miner_config)
        code=Code(**code)
        # print(f"code {code}")
        ### miner base
        pipeline=MinerPipeline(miner_config=miner_config)
        ### __init__
        pipeline.add_processer(SetUpProcessor())
        ### get_input
        pipeline.add_processer(ExtractProcessor(code=code.get_input))
        ### mimic_backrun
        # logging.info(f"mimic_backrun {miner_config.metadata.start_date}")
        backtest_simulator=BacktestSimulator(
            schedule=miner_config.metadata.schedule, 
            start_date=datetime(**miner_config.metadata.start_date),
            end_date=datetime(**miner_config.metadata.end_date))
        
        stages,sys_out = backtest_simulator.mimic_backrun(pipeline=pipeline)#resu
        # logging.info(f"relog log{log}")
        ### split result into multiple symbol
        backtestResult= DetailTransformer(stages=stages, miner_config=miner_config).transform()

        backtestResult.log=sys_out
        # logging.info(f"backtestResult {backtestResult.log}")
        # logger.info(f"backtestResult {backtestResult}")
        # detail_stages_json=[detail_stage.model_dump_json() for detail_stage in detail_stages]
        # logger.info(f"detail_stages_json {type(detail_stages_json)} {str(detail_stages_json)[:100]}")
        # detail_stages_gzip=gzip.compress(str(detail_stages_json).encode())
        return backtestResult.model_dump_json()
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
        
        stages,sys_out = backtest_simulator.mimic_backrun(pipeline=pipeline)
        # print(f"mimic_process log{log}")
        
        ### split result into multiple symbol
        backtestResult= DetailTransformer(stages=stages, miner_config=miner_config).transform()
        backtestResult.log=sys_out

        # detail_stages_gzip=gzip.compress(str(detail_stages_json).encode())
        # return detail_stages_gzip
        return backtestResult.model_dump_json()
    except Exception as e:
        logger.error(f"error catch {str(e)}")
        raise e
    

