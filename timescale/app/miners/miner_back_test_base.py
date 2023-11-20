from abc import ABC, abstractmethod

from typing import Callable, Dict, List

import pandas as pd
from datetime import datetime
from streams.data_stream_base import DataStreamBase
import logging
from commons.logger import logger
from schemas.miner_unit import Node, Stage, StartDate
from commons.utils import convert_datetime_to_dict
from commons.decorator_utils import timeit
from validator.exception import miner_exception_handler
from validator.validate import get_cron_instance, get_cron_instance, validate_nodes, validate_stages


class MinerBackTestBase:

    def __init__(self, target_symbols: List[str], input_streams):
        self.output_stream = []
        self.input_streams = input_streams
        self.target_symbols = target_symbols
        self.inputs: Dict[datetime, Dict[str, Node]] = {}

    @classmethod
    def from_config(cls, target_symbols: List, output_cfg: dict, input_cfg: List[dict]):
        input_streams = cls.init_input_streams(input_cfg)
        # output_stream = DataStreamBase.from_config(output_cfg)
        return cls(target_symbols,  input_streams)

    @classmethod
    def init_input_streams(cls, input_cfg: List[dict]) -> Dict[str, DataStreamBase]:
        input_streams = {}
        for cfg in input_cfg:
            input_stream = DataStreamBase.from_config(cfg)
            input_streams[input_stream.signal_name] = input_stream
        return input_streams

    @abstractmethod
    def get_inputs(self, timestamp) -> Dict[str, Node]:
        raise NotImplementedError("Should implement this.")

    def get_input_per_timestamp(self, timestamp: datetime) -> Dict[str, Node]:
        if timestamp in self.inputs:
            logger.info(f"cached {timestamp} in self.inputs")
            return self.inputs[timestamp]
        tmp = self.get_inputs(timestamp)
        self.inputs[timestamp] = tmp
        return self.inputs[timestamp]

    def validate_primary_key(self, output_df: pd.DataFrame, symbol: str) -> bool:
        if output_df is None or output_df.empty:
            return False
        if 'symbol_' not in output_df or 'indexed_timestamp_' not in output_df:
            logging.info("symbol_ or indexed_timestamp_ not in dataframe")
            return False
        if output_df['symbol_'].iloc[0] != symbol:
            return False
        return True

    @validate_nodes
    @timeit
    def process_all_symbols(self, inputNodes: Dict[str, Node], target_symbols: List[str], timestamp: datetime) -> Dict[str, Node]:
        outputs: List[Node] = []
        for symbol in target_symbols:
            output = self.process_per_symbol(
                inputs=inputNodes, symbol=symbol, timestamp=timestamp)
            outputs.append(output)
        return {"output": Node(name="output", source=list(inputNodes.keys()), dataframe=pd.concat(map(lambda x: x.dataframe, outputs)))}

    def process(self, timestamp: datetime) -> Dict[str, Node]:
        assert (isinstance(timestamp, datetime))
        inputNodes = self.get_input_per_timestamp(timestamp)
        return self.process_all_symbols(inputNodes=inputNodes, target_symbols=self.target_symbols, timestamp=timestamp)

    def process_and_save(self, timestamp: datetime) -> Dict[str, Node]:
        output = self.process(timestamp)
        return output

    @abstractmethod
    def process_per_symbol(self, inputs: Dict[str, Node], symbol: str, timestamp) -> Node:
        raise NotImplementedError("Should implement this.")

    @validate_stages
    def build_stage(self, schedule: str, start_date: datetime, timestamp: datetime, func: Callable[[datetime], Dict[str, Node]]) -> Stage:
        nodes = func(timestamp)
        return Stage(timestamp=timestamp, schedule=schedule, start_date=StartDate(**convert_datetime_to_dict(start_date)), nodes=nodes)

    @miner_exception_handler
    def mimic_backrun(self, cron_str: str, start_date: datetime, end_date: datetime, step: int) -> List[Stage]:
        cron_instance = get_cron_instance(cron_str, start_date, end_date)
        schedule = cron_instance.schedule(start_date)

        stages: List[Stage] = []
        while timestamp := schedule.next():
            if timestamp < end_date:
                stage = self.build_stage(schedule=cron_str, start_date=start_date, timestamp=timestamp,
                                         func=self.process_and_save if step == 1 else self.get_input_per_timestamp)
                stages.append(stage)
            else:
                break
        return stages

    # def find(cls, identified_name):
    #    raise NotImplementedError("Should implement this.")
