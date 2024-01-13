from typing import List

from fastapi import (
    APIRouter,
    Depends,
)
from pydantic import BaseModel

from sqlalchemy.orm import Session

from backend.session import create_session

from schemas.miner import BackTestRequest, Miner, Code, MinerSetup
from services.miner import MinerService
from commons.logger import logger


router = APIRouter(responses={404: {"description": "Not found bal bla"}})


@router.get("/")
async def get_miner(
    id: str = "",
    name: str = "",
    page_size: int = 10,
    page_number: int = 1,
    session: Session = Depends(create_session),
) -> List[Miner]:
    logger.info(f"miner input id:{id},name:{name}")
    miner_ids = list([int(item) for item in filter(None, id.split(','))])
    miner_names = list([item for item in filter(None, name.split(','))])
    if len(miner_ids) == 0 and len(miner_names) == 0:
        return MinerService(session).get_all_miner(limit=page_size, offset=(page_number-1)*page_size)
    return MinerService(session).get_miner(miner_ids, miner_names)


@router.post("/save_cfg")
async def save_miner(
    miner: Miner,
    session: Session = Depends(create_session),
) -> Miner:
    """Create a new stream."""
    # logger.info(f"miner:{miner.model_dump()}")
    return MinerService(session).save_miner(miner)


@router.post("/save_full_cfg")
async def save_miner_full(
    saveRequest: BackTestRequest,
    session: Session = Depends(create_session),
) -> Miner:
    """Create a new stream."""
    # logger.info(f"miner:{miner.model_dump()}")
    logger.info("saveRequest***********************************")
    return MinerService(session).save_miner_full(backtestRequest=saveRequest)


@router.post("/verify")
async def verify_miner(
    miner: MinerSetup,
    session: Session = Depends(create_session),
) -> Miner:
    """verify stream yaml"""
    return MinerService(session).verify(miner)

@router.post("/save_file")
async def save_file(testProcess: BackTestRequest, session: Session = Depends(create_session)):
    return MinerService(session).save_file(miner_config=testProcess.minerCatalog, code=testProcess.code)


@router.get("/file")
async def get_miner_file(name: str = "", session: Session = Depends(create_session)):
    miner_names = list([item for item in filter(None, name.split(','))])
    return MinerService(session).get_miner_files(miner_names=miner_names)


@router.get("/file_all")
async def get_miner_file_all(session: Session = Depends(create_session)):
    return MinerService(session).get_all_miner_files()
