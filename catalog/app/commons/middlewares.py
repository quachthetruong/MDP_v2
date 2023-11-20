import traceback
from fastapi import Request, Response
from commons.logger import logger
import time


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        traceback.print_exc()
        return Response(str(e), status_code=500)


async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = str(time.time() - start_time)
    response.headers["X-Process-Time"] = process_time
    logger.info(f"Process time: {process_time}")
    return response
