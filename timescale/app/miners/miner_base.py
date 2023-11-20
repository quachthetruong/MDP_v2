from abc import ABC, abstractmethod
from typing import List
from common import config
import pandas as pd 
from datetime import datetime
from cron_converter import Cron
from streams.data_stream_base import DataStreamBase
from streams.stream_cfg import StreamCfg
import logging


class MinerBase:
        
    def __init__(self, target_symbols: List, output_stream, input_streams):
        self.output_stream = output_stream
        self.input_streams = input_streams
        self.target_symbols = target_symbols

    @classmethod
    def from_config(cls, target_symbols: List, output_cfg: dict, input_cfg: List[dict]):
        input_streams = cls.init_input_streams(input_cfg)
        output_stream = DataStreamBase.from_config(output_cfg)
        
        return cls(target_symbols, output_stream, input_streams)
    
    @classmethod
    def init_input_streams(cls, input_cfg: List[dict]):
        input_streams = {}
        for cfg in input_cfg:
            input_stream = DataStreamBase.from_config(cfg)
            input_streams[input_stream.signal_name] = input_stream
        return input_streams    
    
    @abstractmethod
    def get_inputs(self, timestamp):
        raise NotImplementedError("Should implement this.")

    def process(self, timestamp):
        inputs = self.get_inputs(timestamp)
        outputs = []

        if isinstance(inputs, dict):
            for symbol in self.target_symbols:
                outputs.append(self.process_per_symbol(inputs[symbol],symbol, timestamp))
        else:
            assert(isinstance(inputs, tuple)) # Make sure we are working with a tuple
            for symbol in self.target_symbols:
                input_per_symbol = []
                for input in inputs:
                    if len(input) != 0:
                        input_per_symbol.append(input[input[config.SYSTEM_SYMBOL_COL] == symbol])
                    else:
                        input_per_symbol.append(pd.DataFrame(columns=input.columns))
                output = self.process_per_symbol(input_per_symbol,symbol, timestamp)
                if output is not None:
                    outputs.append(output)
        if len(outputs) != 0:
            return pd.concat(outputs)
        else:
            logging.warning("No output for {}".format(timestamp))
            return None
    
    def process_and_save(self, timestamp):
        output = self.process(timestamp)
        # if output is not None:
        #     return self.output_stream.append(output)

    @abstractmethod
    def process_per_symbol(self, inputs, symbol, timestamp):
        raise NotImplementedError("Should implement this.")

    def mimic_backrun(self, cron_str, start_date, end_date):
        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)
        cron_instance = Cron()
        try:
            cron_instance.from_string(cron_str)
        except Exception as e:
            logging.warning('The cron_str expression is wrong. ' + str(e))
            return False
        schedule = cron_instance.schedule(start_date)
        while True:
            timestamp = schedule.next()
            if timestamp < end_date:
                self.process_and_save(timestamp)
            else:
                break
        return True

    def __str__(self) -> str:
        msg = "Target symbols: {}, Output stream: {}"
        return msg.format(self.target_symbols, self.output_stream)

    # def find(cls, identified_name):
    #    raise NotImplementedError("Should implement this.")

class EncapMinnerBase(MinerBase):
    
    def process(self, timestamp):
        inputs = self.get_inputs(timestamp)
        outputs = []

        if isinstance(inputs, dict):
            for symbol in self.target_symbols:
                outputs.append(self.process_per_symbol(inputs['symbol'],symbol, timestamp))
        else:
            assert(isinstance(inputs, tuple)) # Make sure we are working with a tuple
            for symbol in self.target_symbols:
                input_per_symbol = []
                for input in inputs:
                    if len(input) != 0:
                        input_per_symbol.append(input[input['symbol'] == symbol])
                    else:
                        input_per_symbol.append(pd.DataFrame(columns=input.columns))
                output = self.process_per_symbol(input_per_symbol, symbol, timestamp)
                if output is not None:
                    outputs.append(output)
        if len(outputs) != 0:
            return pd.concat(outputs)
        else:
            logging.warning("No output for {}".format(timestamp))
            return None
