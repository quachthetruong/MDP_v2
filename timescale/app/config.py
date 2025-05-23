from typing import (
    Final,
)


OPEN_API_TITLE: Final = "API_Timescale"
OPEN_API_DESCRIPTION: Final = "Demo API over UAT database built with FastAPI."


SYSTEM_TIMESTAMP_COL = 'indexed_timestamp_'
SYSTEM_SYMBOL_COL = 'symbol_'

TIMESCALE_DB_URL = 'postgresql://postgres:nIV0JOWbNLpZuFk0lHkq1uF02AKvIryLAV9USAvkvLT9AAcoIbFK0ydZGabETOKK@103.151.242.52:5432/dsai'
MOCK_TIMESCALE_DB_URL = 'postgresql://postgres:postgres@catalog-local-db:5432/timescale_mock'
CATALOG_SERVICE_URL = 'http://catalog-local:8002/'

CATALOG_DB_URL='postgresql://postgres:postgres@catalog-local-db:5432/dsai'
REDSHIFT_DB_URL='redshift://awsuser:RGYcH3Yoyqo5fKrk@poc-redshift-free-trial.cir6kkgprzvy.ap-southeast-1.redshift.amazonaws.com:5439/dev'

RECORD_LIMIT = 100
