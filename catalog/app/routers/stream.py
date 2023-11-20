from typing import Any, List, Optional, Type, Union

from fastapi import (
    APIRouter,
    Depends,
    Response,
)
from sqlalchemy.orm import Session

from backend.session import create_session

from schemas.stream import Stream, StreamField, StreamMetadata
from services.stream import StreamService

from commons.logger import logger

router = APIRouter()


# @router.get("/all")
# async def get_all_stream(session: Session = Depends(create_session)) -> List[StreamMetadata]:
#     """Get all stream metadata in catalog."""
#     return StreamService(session).get_all_stream()


@router.get("/")
async def get_streams(  
    id: str="",
    name: str="",
    page_size: int=10,
    page_number: int=1,
    session: Session = Depends(create_session),
) -> List[Union[Stream,StreamMetadata]]:
    """Get stream by ID."""
    logger.info(f"stream input id:{id},name:{name}")
    stream_ids=list([int(item) for item in filter(None,id.split(','))])
    stream_names=list([item for item in filter(None,name.split(','))])
    if len(stream_ids)==0 and len(stream_names)==0:
        return StreamService(session).get_all_stream(limit=page_size,offset=(page_number-1)*page_size)
    return StreamService(session).get_streams(stream_ids=stream_ids,stream_names=stream_names)

@router.post("/save")
async def save_stream(
    stream: Stream,
    session: Session = Depends(create_session),
) -> Stream:
    """Create a new stream."""
    return StreamService(session).save_stream(stream)

@router.post("/verify")
async def verify(
    stream: Stream,
    session: Session = Depends(create_session),
) -> Stream:
    """Create a new stream."""
    return StreamService(session).verify(stream)

@router.post("/metadata/save")
async def save_metadata(
    metadata: StreamMetadata,
    session: Session = Depends(create_session),
) -> Stream:
    """Create a new stream."""
    # logger.info(f"check array router {metadata}")
    return StreamService(session).save_metadata(metadata)

@router.post("/stream_fields/save")
async def save_stream_fields(
    stream_fields: List[StreamField],
    session: Session = Depends(create_session),
) -> List[StreamField]:
    """Create a new stream."""
    return StreamService(session).save_stream_fields(stream_fields=stream_fields)



