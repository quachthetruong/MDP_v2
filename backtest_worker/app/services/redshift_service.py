import pandas as pd
import redshift_connector
from sqlalchemy.orm import Session
from typing import Any, Dict, List
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, select
from sqlalchemy.sql import text
from sqlalchemy import Column
from schemas.stream_field import StreamField
from services.base import BaseService, BaseDataManager
from schemas.miner import MinerCatalog
from schemas.stream import StreamCatalog
from common.template_loader import TemplateLoader
import config
from redshift_connector import Connection

class RedshiftService:
    def __init__(self):
        conn=redshift_connector.connect(database="dev", host="poc-redshift-free-trial.cir6kkgprzvy.ap-southeast-1.redshift.amazonaws.com",
                                         port=5439, user="awsuser", password="RGYcH3Yoyqo5fKrk")
        self.redshiftDataManager = RedshiftDataManager(conn)
    def get_record(self, table_name: str, indexed_timestamp: str, symbol_column: str, timestamp_column: str,
                   target_symbols: List = None, filter_query: str = None, columns: List[str]=[]):
        return self.redshiftDataManager.get_record(
            table_name=table_name,
            indexed_timestamp=indexed_timestamp,
            symbol_column=symbol_column,
            timestamp_column=timestamp_column,
            target_symbols=target_symbols,
            filter_query=filter_query,
            columns=columns
        )

    def get_record_range(
        self,
        table_name: str,
        included_min_timestamp: str,
        included_max_timestamp: str,
        symbol_column: str,
        timestamp_column: str,
        target_symbols: List = None,
        filter_query: str = None,
        columns: List[str]=[]
    ) -> pd.DataFrame:
        return self.redshiftDataManager.get_record_range(
            table_name=table_name,
            included_min_timestamp=included_min_timestamp,
            included_max_timestamp=included_max_timestamp,
            symbol_column=symbol_column,
            timestamp_column=timestamp_column,
            target_symbols=target_symbols,
            filter_query=filter_query,
            columns=columns
        )

    def get_distinct_symbol(
        self,
        table_name: str,
        symbol_column: str,
    ) -> pd.DataFrame:
        return self.redshiftDataManager.get_distinct_symbol(
            table_name=table_name,
            symbol_column=symbol_column,
        )


class RedshiftDataManager:
    def __init__(self, conn:Connection):
        self.conn = conn
    def get_record_range(self, table_name: str, included_min_timestamp: str, included_max_timestamp: str,
                         timestamp_column: str, symbol_column: str, target_symbols: List[str] = [],
                         filter_query: str = None, limit: int = config.RECORD_LIMIT,columns:List[str]=[]) -> pd.DataFrame:
        loader = TemplateLoader()
        args = {
            "table_name": table_name,
            "included_min_timestamp": included_min_timestamp,
            "included_max_timestamp": included_max_timestamp,
            "symbol_column": symbol_column,
            "timestamp_column": timestamp_column,
            "target_symbols": target_symbols,
            "filter_query": filter_query,
            "limit": limit
        }
        sql = loader.render("get_record_range.tpl", **args)
        cursor=self.conn.cursor()
        # cursor.execute(sql)
        # # logger.info(f"sql:=========== {sql}")
        # results = []
        # for row in cursor.fetchall():
        #     results.append(dict(zip(columns, row)))
        results: pd.DataFrame = cursor.fetch_dataframe()

        self.conn.commit() 
        self.conn.close() 
        return results


    def get_record(self, table_name: str, indexed_timestamp: str, timestamp_column: str, symbol_column: str,
                   target_symbols: List[str] = [], filter_query: str = None, limit: int = config.RECORD_LIMIT,
                   columns:List[str]=[]) -> pd.DataFrame:
        loader = TemplateLoader()
        args = {
            "table_name": table_name,
            "indexed_timestamp": indexed_timestamp,
            "symbol_column": symbol_column,
            "timestamp_column": timestamp_column,
            "target_symbols": target_symbols,
            "filter_query": filter_query,
            "limit": limit
        }
        sql = loader.render("get_record.tpl", **args)
        results = self.session.execute(text(sql)).all()
        return pd.DataFrame([row._asdict() for row in results])

    def get_distinct_symbol(self, table_name: str, symbol_column: str, limit: int = config.RECORD_LIMIT):
        loader = TemplateLoader()
        args = {
            "table_name": table_name,
            "symbol_column": symbol_column,
            "limit": limit
        }
        sql = loader.render("get_distinct_symbol.tpl", **args)
        results = self.session.execute(text(sql)).all()
        return [row._asdict() for row in results]
