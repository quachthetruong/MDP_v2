from datetime import datetime
import io
from typing import Callable, List,Dict, Tuple
from validator.exception import miner_exception_handler
from miners.pipeline.minerPipeline import MinerPipeline
from commons.utils import convert_datetime_to_dict
from schemas.miner_unit import Stage, ScheduleDate,Node
from cron_converter import Cron
import logging
from contextlib import redirect_stdout


class BacktestSimulator:
    def __init__(self,schedule:str, start_date:datetime,end_date:datetime):
        self.schedule=schedule
        self.start_date=start_date
        self.end_date=end_date

    def get_cron_instance(self,cron_str: str, start_date: datetime, end_date: datetime) -> Cron:
        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)
        try:
            cron_instance = Cron()
            cron_instance.from_string(cron_str)
            return cron_instance
        except Exception as e:
            logging.warning('The cron_str expression is wrong. ' + str(e))
            raise

    def build_stage(self, timestamp: datetime,pipeline:MinerPipeline) -> Stage:
        nodes = pipeline.trigger(timestamp=timestamp)
        return Stage(timestamp=timestamp, nodes=nodes)

    @miner_exception_handler
    def mimic_backrun(self, pipeline:MinerPipeline) -> Tuple[List[Stage],str]:
        cron_instance = self.get_cron_instance(cron_str=self.schedule, start_date=self.start_date, end_date=self.end_date)
        schedule = cron_instance.schedule(self.start_date)

        stages: List[Stage] = []
        f = io.StringIO() # capture print() in user code
        with redirect_stdout(f):
            while timestamp := schedule.next():
                print("*" * 50)
                print(f"Execution time: {timestamp}")
                print("*" * 50)
                if timestamp < self.end_date:
                    stage = self.build_stage(timestamp=timestamp,pipeline=pipeline)
                    stages.append(stage)
                else:
                    break
        logs = f.getvalue()
        return stages,logs
