import pandas as pd
import psycopg2
from sqlalchemy.orm import Session
from commons.logger import logger
from typing import Any, Dict, Iterator, List
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, select
from sqlalchemy.sql import text
from sqlalchemy import Column
from services.base import BaseService, BaseDataManager
from schemas.miner import MinerCatalog
from schemas.stream import StreamCatalog
from common.template_loader import TemplateLoader
import config
import redshift_connector

from schemas.stream_field import StreamField

# from backend.redshift_session import create_redshift_session


class StreamService(BaseService):
    def __init__(self, session: Session):
        super().__init__(session)
        self.streamDataManager = StreamDataManager(session)

    def get_streams(self, stream_names: List[str], target_symbols: List[str] = []):
        return [self.streamDataManager.get_stream(stream_name, target_symbols) for stream_name in stream_names]
    
    def get_stream(self,stream_name:str):
        return self.streamDataManager.get_stream(stream_name)

    def getStreamByCatalog(self, streamCatalog: StreamCatalog, minerCatalog: MinerCatalog=None)->List[Any]:
        if streamCatalog.metadata.id is None:
            return []
        stream_name = streamCatalog.metadata.name
        symbol_field = streamCatalog.metadata.symbol_field
        timestamp_field = streamCatalog.metadata.timestamp_field
        storage_backend = streamCatalog.metadata.storage_backend
        target_symbols = minerCatalog.metadata.target_symbols if minerCatalog else []
        ###manual by redshift_connector, not sqlalchemy
        if storage_backend == "RedshiftStorage":
            return self.get_redshift_streams(stream_name=stream_name, target_symbols=target_symbols,
                                            symbol_field=symbol_field, timestamp_field=timestamp_field,
                                            stream_fields=streamCatalog.spec.stream_fields)
        ##########
        return self.streamDataManager.get_stream(stream_name=stream_name, target_symbols=target_symbols,
                                                  symbol_field=symbol_field, timestamp_field=timestamp_field)

    def create_table(self, streamCatalog: StreamCatalog):
        self.streamDataManager.create_table(streamCatalog=streamCatalog)


    def get_redshift_streams(self,stream_name: str, target_symbols: List[str], limit: int = 100,
                    symbol_field: str = 'symbol', timestamp_field: str = 'time',stream_fields:List[StreamField]=[]):
        # session: Session=next(create_redshift_session())
        symbol_list = ",".join(f"'{symbol}'" for symbol in target_symbols)
        query = f'SELECT * FROM {stream_name} where {symbol_field} in ({symbol_list}) order by {timestamp_field}  limit {limit}'
        logger.info(f"Redshift query: {query}")
        
        conn=redshift_connector.connect(database="dev", host="poc-redshift-free-trial.cir6kkgprzvy.ap-southeast-1.redshift.amazonaws.com",
                                         port=5439, user="awsuser", password="RGYcH3Yoyqo5fKrk")
        cursor=conn.cursor()
        cursor.execute(query)
        results = []
        columns=[stream_field.name for stream_field in stream_fields]
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))

        logger.info(f"redshift result {len(results)}")
        conn.commit() 
        conn.close() 
        return results

class StreamDataManager(BaseDataManager):
    def get_stream(self, stream_name: str, target_symbols: List[str]=[], limit: int = 100,
                    symbol_field: str = 'symbol_', timestamp_field: str = 'indexed_timestamp_'
                    ):
        if len(target_symbols) == 0:
            query = f'SELECT * FROM {stream_name} order by {timestamp_field}  limit {limit}'
        else:
            symbol_list = ",".join(f"'{symbol}'" for symbol in target_symbols)
            query = f'SELECT * FROM {stream_name} where {symbol_field} in ({symbol_list}) order by {timestamp_field}  limit {limit}'
        logger.info(f"query database storage: {query}")
        logger.info(f"session: {self.session}")
        results = self.session.execute(text(query)).all()
        logger.info(f"database storage result {len(results)}")
        return [row._asdict() for row in results]

    def getData(self, streamCatalog: StreamCatalog, minerCatalog: MinerCatalog) -> List[Any]:
        users_table = self.metadata.tables[streamCatalog.metadata.name.split(
            '.', 1)[-1]]
        symbol_list = tuple(minerCatalog.metadata.target_symbols)
        query = select(users_table).where(text(
            f'{streamCatalog.metadata.symbol_field} in {symbol_list}')).limit(100).offset(0)
        results = self.session.execute(query).all()
        return [row._asdict() for row in results]

    def get_record_range(self, table_name: str, included_min_timestamp: str, included_max_timestamp: str,
                         timestamp_column: str, symbol_column: str, target_symbols: List[str] = [],
                         filter_query: str = None, limit: int = config.RECORD_LIMIT) -> pd.DataFrame:
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
        # logger.info(f"sql:=========== {sql}")
        results = self.session.execute(text(sql)).all()
        return pd.DataFrame([row._asdict() for row in results])

    def get_record(self, table_name: str, indexed_timestamp: str, timestamp_column: str, symbol_column: str,
                   target_symbols: List[str] = [], filter_query: str = None, limit: int = config.RECORD_LIMIT) -> pd.DataFrame:
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
        results = self.session.execute(sql).all()
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
