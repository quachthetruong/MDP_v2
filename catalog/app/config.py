from typing import (
    Final,
)


OPEN_API_TITLE: Final = "API_CATALOG"
OPEN_API_DESCRIPTION: Final = "Demo API over catalog database built with FastAPI."



SYSTEM_TIMESTAMP_COL = 'indexed_timestamp_'
SYSTEM_SYMBOL_COL = 'symbol_'

CATALOG_DB_URL='postgresql://postgres:postgres@103.151.242.52:5432/dsai'
TIMESCALE_SERVICE_URL='http://10.68.1.158:8003/'