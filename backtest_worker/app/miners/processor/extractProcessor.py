# from datetime import datetime
# from typing import Dict, List
# # from services.function import DynamicFunction
# from schemas.function import StreamExtract
# from schemas.miner_unit import Node
# from streams.data_stream_base import DataStreamBase
# from miners.processor.minerProcessor import MinerProcessor


# from datetime import timedelta,datetime
# from streams.data_stream_base import DataStreamBase

# from dateutil.relativedelta import relativedelta
# import logging
# import pandas as pd
# import numpy as np
# from schemas.miner_unit import Node
# import talib as ta
# from validator.validate import validate_nodes,validate_code


# class ExtractProcessor(MinerProcessor):
#     def __init__(self, name="extract",extract_streams:List[StreamExtract]=[]):
#         super().__init__(name)
#         self.extract_streams=extract_streams

#     def extract_stream(self,extract_stream:StreamExtract,input_stream:DataStreamBase,timestamp:datetime) -> Node:
   
#         dataframe:pd.DataFrame
#         if extract_stream.function.name=="get_record":
#             dataframe= input_stream.get_record(**{param.name:param.value for param in extract_stream.function.params},
#                                            target_symbols=self.miner_config.metadata.target_symbols)
#         elif extract_stream.function.name=="get_record_range":
#             dataframe= input_stream.get_record_range(**{param.name:param.value for param in extract_stream.function.params},
#                                            target_symbols=self.miner_config.metadata.target_symbols)
#         return Node(name=extract_stream.name,dataframe=dataframe)
        

#     @validate_nodes
#     def extract_miner(self,extract_streams:List[StreamExtract],input_streams:Dict[str,DataStreamBase],timestamp:datetime) -> Dict[str, Node]:
#         # result: Dict[str, Node]={}
#         # local_params={'result':result,'timestamp':timestamp, 'input_streams':input_streams, 'target_symbols':target_symbols}
#         print("miner_config",self.miner_config)
#         print("extract_streams",extract_streams)

#         # return local_params['result']

#     def execute(self,timestamp:datetime,data:Dict[str,DataStreamBase])-> Dict[str, Node]:
#         return self.extract(input_streams=data,timestamp=timestamp,extract_streams=self.extract_streams)
        
from datetime import datetime
from typing import Dict, List
from commons.celery_utils import get_celery_cached_result, hash_celery_task_id
from schemas.miner import HashData
from schemas.miner_unit import Node
from streams.data_stream_base import DataStreamBase
from miners.processor.minerProcessor import MinerProcessor

from common.math_utils import MathUtils
from common.response_utils import ResponseUtils

from datetime import timedelta,datetime
from streams.data_stream_base import DataStreamBase

from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
import numpy as np
from schemas.miner_unit import Node
import talib as ta
from validator.validate import validate_nodes,validate_code

class ExtractProcessor(MinerProcessor):
    def __init__(self, name="extract",code:str=""):
        super().__init__(name)
        self.code=code

    @validate_nodes
    @validate_code
    def get_inputs(self,code:str,input_streams:Dict[str,DataStreamBase],timestamp:datetime,target_symbols:List[str]) -> Dict[str, Node]:
        result: Dict[str, Node]={}
        local_params={'result':result,'timestamp':timestamp, 'input_streams':input_streams, 'target_symbols':target_symbols}
        user_code=code
        final_code=f"{user_code}\nnodes = get_inputs(timestamp=timestamp, input_streams=input_streams, target_symbols=target_symbols)\nresult: Dict[str, Node] = {{node.name: node for node in nodes}}"
        exec(final_code,globals(),local_params)
        return local_params['result']

    def execute(self,timestamp:datetime,data:Dict[str,DataStreamBase])-> Dict[str, Node]:
        # hashData=HashData(miner_config=self.miner_config,body=self.code,route="get_input")
        # task_id=hash_celery_task_id(hashData)
        # logging.info(f"task_id {task_id}")
        # if value:=get_celery_cached_result(task_id):
        #     logging.info("get_inputs from cache")
        #     result=value["result"] #celery return task_id, result, status, traceback, children, date_done
        # else:
        # cache celery only apply for pipeline not single processor
        print("key",data.keys())
        result=self.get_inputs(input_streams=data,timestamp=timestamp,code=self.code,
                            target_symbols=self.miner_config.metadata.target_symbols)
        return result