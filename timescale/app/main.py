from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse

import config
from routers import (miner, stream,health)
from version import __version__
from commons.middlewares import catch_exceptions_middleware, add_process_time_header
# ... other imports


app = FastAPI(
    title=config.OPEN_API_TITLE,
    description=config.OPEN_API_DESCRIPTION,
    version=__version__,
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},
    debug=True,
    default_response_class=ORJSONResponse,#ValueError: Out of range float values are not JSON compliant
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
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.include_router(health.router,prefix='/health',tags=['health'],)
app.include_router(stream.router, prefix='/stream', tags=['stream'])
app.include_router(miner.router, prefix='/miner', tags=['miner'])

