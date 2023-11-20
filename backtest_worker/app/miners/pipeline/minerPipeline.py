from datetime import datetime
from schemas.miner import MinerCatalog
from miners.processor.minerProcessor import MinerProcessor
from miners.pipeline.pipeline import Pipeline


class MinerPipeline(Pipeline):
    def __init__(self,miner_config:MinerCatalog):
        super().__init__()
        self.miner_config:MinerCatalog=miner_config
    def add_processer(self,pipe:MinerProcessor):
        pipe.setConfig(miner_config=self.miner_config)
        self.pipes.append(pipe)
    
   
    def trigger(self,timestamp:datetime, data=None):
      for pipe in self.pipes:
        data = pipe.execute(timestamp=timestamp,data=data)
      return data
