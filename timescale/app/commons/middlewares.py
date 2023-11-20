import traceback
from fastapi import Request, Response
import time
import logging


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        traceback.print_exc()
        return Response(str(e), status_code=500)


async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    logging.info(f"start request {request.url}")
    response = await call_next(request)
    process_time = str(time.time() - start_time)
    response.headers["X-Process-Time"] = process_time
    logging.info(f"Process time for {request.url}: {process_time}")
    return response
