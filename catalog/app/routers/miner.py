from typing import List

from fastapi import (
    APIRouter,
    Depends,
)
from pydantic import BaseModel

from sqlalchemy.orm import Session

from backend.session import create_session

from schemas.miner import Miner, Code, MinerSetup
from services.miner import MinerService
from commons.logger import logger


router = APIRouter(responses={404: {"description": "Not found bal bla"}})


class SaveRequest(BaseModel):
    minerCatalog: Miner
    code: Code


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
    saveRequest: SaveRequest,
    session: Session = Depends(create_session),
) -> Miner:
    """Create a new stream."""
    # logger.info(f"miner:{miner.model_dump()}")
    return MinerService(session).save_miner_full(miner=saveRequest.minerCatalog, code=saveRequest.code)


@router.post("/verify")
async def verify_miner(
    miner: MinerSetup,
    session: Session = Depends(create_session),
) -> Miner:
    """verify stream yaml"""
    a = MinerService(session).verify(miner)
    logger.info("done catalog verify")
    return a


@router.post("/save_file")
async def save_file(testProcess: SaveRequest, session: Session = Depends(create_session)):
    return MinerService(session).save_file(miner_config=testProcess.minerCatalog, code=testProcess.code)


@router.get("/file")
async def get_miner_file(name: str = "", session: Session = Depends(create_session)):
    miner_names = list([item for item in filter(None, name.split(','))])
    return MinerService(session).get_miner_files(miner_names=miner_names)


@router.get("/file_all")
async def get_miner_file_all(session: Session = Depends(create_session)):
    return MinerService(session).get_all_miner_files()
