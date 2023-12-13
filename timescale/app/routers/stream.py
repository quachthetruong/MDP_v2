import logging
from typing import Any, List, Optional, Type, Union

from fastapi import (
    APIRouter,
    Depends,
    Response,
)
from sqlalchemy.orm import Session

from backend.session import  create_session
import pandas as pd
from schemas.stream import StreamCatalog
from services.stream import StreamService
from sqlalchemy import create_engine
import config
from schemas.stream_field import map_type,reverse_dtype_mapping
router = APIRouter()
 

@router.get("/")
async def get_all_stream(name:StreamCatalog,symbol:str="",session: Session = Depends(create_session)):
    """Get all stream metadata in catalog."""
    stream_names=list([item for item in filter(None,name.split(','))])
    target_symbols=list([item for item in filter(None,symbol.split(','))])
    return StreamService(session).get_streams(stream_names=stream_names,target_symbols=target_symbols)

@router.post("/detail")
async def get_stream_by_catalog(streamCatalog:StreamCatalog,session: Session = Depends(create_session)):
    return StreamService(session).getStreamByCatalog(streamCatalog=streamCatalog)

@router.post("/create_table")
async def create_table(streamCatalog:StreamCatalog):
    engine=create_engine(config.MOCK_TIMESCALE_DB_URL, pool_pre_ping=True)

    init_stream_columns={stream_field.name :pd.Series(dtype=map_type(stream_field.type,reverse_dtype_mapping)) 
                         for stream_field in streamCatalog.spec.stream_fields}
    data_df=pd.DataFrame(init_stream_columns)
    data_df.set_index(['symbol_','indexed_timestamp_'],inplace=True)

    logging.info(f'create table {data_df.info()}')
    data_df.to_sql(streamCatalog.metadata.name.split('.', 1)[-1],engine,if_exists='replace',index=True)
    return data_df
    # pass