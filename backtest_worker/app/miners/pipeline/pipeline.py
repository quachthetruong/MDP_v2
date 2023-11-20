from abc import ABC, abstractmethod
from typing import List
from schemas.miner import MinerCatalog
from typing import Dict, Generic, TypeVar

from miners.processor.processer import Processor

class Pipeline():
    
    def __init__(self):
        self.pipes:List[Processor]=[]

    @abstractmethod
    def add_processer(self,pipe:Processor):
        raise NotImplementedError("Should implement this!")
      
    @abstractmethod
    def trigger(self,input):
        raise NotImplementedError("Should implement this!")

