import gzip
import logging
from typing import List
from fastapi import (
    APIRouter,
    Depends,
    Request
)
from fastapi.responses import HTMLResponse

from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.session import create_session

from schemas.miner import Miner, MinerCatalog, MinerSetupCatalog
from services.miner import MinerService
from schemas.other import ValidateForm
from schemas.miner import Code
from schemas.function import MinerExtract
from validator.exception import miner_exception_handler
from worker import celery

router = APIRouter()


class BackTestRequest(BaseModel):
    minerCatalog: MinerCatalog
    code: Code



@router.post("/setup")
async def setup_miner(
        miner_config: MinerSetupCatalog,
        session: Session = Depends(create_session)) -> Miner:
    return MinerService(session).setUp(miner_config=miner_config)


@router.post("/extract")
async def extract(MinerExtract: MinerExtract, session: Session = Depends(create_session)):
    return MinerService(session).extract(miner_config=MinerExtract.minerCatalog, extract_streams=MinerExtract.extractStreams)

@router.post("/test_get_input")
async def test_get_inputs(backTestRequest: BackTestRequest, session: Session = Depends(create_session)):
    return MinerService(session).test_get_input(miner_config=backTestRequest.minerCatalog, code=backTestRequest.code)


@router.post("/test_process")
async def test_process(backTestRequest: BackTestRequest, session: Session = Depends(create_session)):
    return MinerService(session).test_process(miner_config=backTestRequest.minerCatalog, code=backTestRequest.code)


@router.post("/validate")
async def validate_output(validateForm: ValidateForm, session: Session = Depends(create_session)):
    return MinerService(session).validate(validateForm=validateForm)

@router.get("/test")
async def test():
    @miner_exception_handler
    def test():    
        task = celery.send_task('tasks.hello', kwargs={})
        result=task.get()
        # response = f"<div>check status of {result} </div>"
        res=gzip.decompress(result).decode()

        return res
    return test()

@router.get("/status/{task_id}")
async def status(task_id: str):
    task = celery.AsyncResult(task_id)
    response = f"<div>status of {task_id} is {task.status} {task.result}</div>"
    return response