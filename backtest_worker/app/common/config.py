SYSTEM_TIMESTAMP_COL = 'indexed_timestamp_'
SYSTEM_SYMBOL_COL = 'symbol_'
SYSTEM_COLS = [(SYSTEM_TIMESTAMP_COL, 'timestamp'), (SYSTEM_SYMBOL_COL, 'varchar(20)')]
STORAGE_FOLDER = '/tmp/enmdp/'


KAFKA_TOPIC = {
    "news": "dsai-raw-news",
    "stock_si": "si",
    "stock_tp": "tp",
    "stock_tick": "tick",
    "stock_trading_session": "trading_session",
    "derivative": "infogate_price",
}

