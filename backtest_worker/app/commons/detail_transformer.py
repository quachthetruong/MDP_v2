import copy
import logging
from typing import Dict, List
import numpy as np

import pandas as pd
from schemas.miner import MinerCatalog

from schemas.miner_unit import BacktestResult, MiniNode, Node, Stage, DetailNode,DetailStage, StageMetadata
from schemas.stream_field import StreamField
from commons.dataframe import normalize_data_type

primary_key_columns = ['symbol_', 'indexed_timestamp_']


class DetailTransformer:
    def __init__(self, stages: List[Stage], miner_config: MinerCatalog) -> None:
        self.stages = stages
        self.miner_config = miner_config
        self.columns: Dict[str, List[StreamField]] = {}
        self.node_names: List[str] = stages[0].nodes.keys() if len(
            stages) > 0 else []

    def transform(self) -> BacktestResult:
        self.columns = self.get_pandas_metadata(stages=self.stages)
        detailStages= self.transform_detail_stages(stages=self.stages)
        stageMetadata= StageMetadata(schedule=self.miner_config.metadata.schedule,
                                     start_date=self.miner_config.metadata.start_date,
                                     end_date=self.miner_config.metadata.end_date)
        return BacktestResult(metadata=stageMetadata, stages=detailStages)

    def get_pandas_metadata(self, stages: List[Stage]) -> Dict[str, List[StreamField]]:
        dataframe_per_node = self.group_dataframes_per_node(stages=stages)
        columns_per_node = self.get_columns_per_node(
            dataframe_per_node=dataframe_per_node)
        return columns_per_node

    def group_dataframes_per_node(self, stages: List[Stage]) -> Dict[str, List[pd.DataFrame]]:
        dataframe_per_node = {node_name: [] for node_name in self.node_names}
        for stage in stages:
            # print(f"stage {stage.nodes.values():}")
            for node in stage.nodes.values():
                if node.dataframe.empty:
                    continue
                dataframe_per_node[node.name].append(node.dataframe)
        return dataframe_per_node

    def get_columns_per_node(self, dataframe_per_node: Dict[str, List[pd.DataFrame]]) -> Dict[str, List[StreamField]]:
        columns_per_node = {node_name: [] for node_name in self.node_names}
        for node_name, dataframe_list in dataframe_per_node.items():
            if not dataframe_list:
                continue

            union_df = normalize_data_type(pd.concat(dataframe_list))
            columns_per_node[node_name] = self.get_stream_fields(union_df)

        return columns_per_node

    def get_stream_fields(self, union_df: pd.DataFrame) -> List[StreamField]:
        columns_per_node = []
        for column_name, data_type in union_df.dtypes.items():
            stream_field = StreamField(name=column_name, type=str(data_type))
            if column_name in primary_key_columns:
                stream_field.is_primary_key = True
                stream_field.is_nullable = False
            columns_per_node.append(stream_field)
        return columns_per_node

    def extract_node_detail(self, node: Node) -> DetailNode:
        mini_nodes: List[MiniNode] = []
        mini_node_indexes: Dict[str, int] = {}
        for index, target_symbol in enumerate(self.miner_config.metadata.target_symbols):
            mini_nodes.append(MiniNode(symbol=target_symbol, data=[]))
            mini_node_indexes[target_symbol] = index
        
        # if not return, node.dataframe.groupby("symbol_") will throw error keyerror 'symbol_'
        if node.dataframe.empty:
            return DetailNode(name=node.name, source=node.source, mini_nodes=mini_nodes)

        for symbol, group_df in node.dataframe.groupby("symbol_"):
            group = self.convert_to_records_format(dataframe=group_df)
            mini_nodes[mini_node_indexes[symbol]] = MiniNode(
                symbol=symbol, data=group)
        return DetailNode(name=node.name, source=node.source, mini_nodes=mini_nodes)

    def convert_to_records_format(self, dataframe: pd.DataFrame) -> List[Dict]:
        dataframe = dataframe.replace({np.nan: None})
        data: List[Dict] = dataframe.to_dict(orient='records')
        return data

    def transform_detail_nodes(self, nodes: Dict[str, Node]) -> List[DetailNode]:
        detailNodes: List[DetailNode] = []
        for node_name, node in nodes.items():
            logging.info(f"node_name {node_name}")
            detailNode = self.extract_node_detail(node=node)
            detailNode.columns = self.columns[node_name]
            detailNodes.append(detailNode)

        return detailNodes

    def extract_stage_detail(self, stage: Stage) -> DetailStage:
        detailNodes = self.transform_detail_nodes(stage.nodes)
        return DetailStage(timestamp=stage.timestamp, nodes=detailNodes)

    def transform_detail_stages(self, stages: List[Stage]) -> List[DetailStage]:

        detailStages: List[DetailStage] = []
        for index, stage in enumerate(stages):
            detailStage = self.extract_stage_detail(stage=stage)
            detailStages.append(detailStage)

        return detailStages
