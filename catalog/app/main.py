from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import config
from routers import (
    stream, miner
)
from version import __version__
from commons.middlewares import catch_exceptions_middleware, add_process_time_header
# ... other imports

from traceback import print_exception

app = FastAPI(
    title=config.OPEN_API_TITLE,
    description=config.OPEN_API_DESCRIPTION,
    version=__version__,
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},
    debug=True,
)


# add_exception_handlers(app)
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.middleware('http')(catch_exceptions_middleware)
app.middleware('http')(add_process_time_header)
app.include_router(stream.router, prefix='/stream', tags=['stream'])
app.include_router(miner.router, prefix='/miner', tags=['miner'])
