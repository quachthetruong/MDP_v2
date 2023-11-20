from abc import abstractmethod
from datetime import datetime
from schemas.miner import MinerCatalog
from miners.processor.processer import Processor

class MinerProcessor(Processor):
      def __init__(self, name: str):
          self.name = name

      def setConfig(self,miner_config:MinerCatalog):
          self.miner_config=miner_config

      @abstractmethod
      def execute(self,timestamp:datetime, data):
          raise NotImplementedError("Should implement this!")
  
      def __str__(self):
          return self.name