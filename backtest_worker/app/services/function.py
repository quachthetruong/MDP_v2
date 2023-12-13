# from typing import Callable, List

# import pandas as pd
# from schemas.stream import StreamCatalog
# from common.template_loader import TemplateLoader
# from sqlalchemy.sql import text
# from sqlalchemy.orm import Session

# import config

# def get_record(session:Session, stream_catalog:StreamCatalog,
#                    target_symbols: List[str] = [], limit: int = config.RECORD_LIMIT) -> pd.DataFrame:
#         loader = TemplateLoader()
#         args = {
#             "table_name": table_name,
#             "indexed_timestamp": indexed_timestamp,
#             "symbol_column": symbol_column,
#             "timestamp_column": timestamp_column,
#             "target_symbols": target_symbols,
#             "limit": limit
#         }
#         sql = loader.render("get_record.tpl", **args)
#         results = session.execute(text(sql)).all()
#         return pd.DataFrame([row._asdict() for row in results])


# def get_record_range(session:Session, table_name: str, included_min_timestamp: str, included_max_timestamp: str,
#                       timestamp_column: str, symbol_column: str, target_symbols: List[str] = [],
#                       filter_query: str = None, limit: int = config.RECORD_LIMIT) -> pd.DataFrame:
#     loader = TemplateLoader()
#     args = {
#         "table_name": table_name,
#         "included_min_timestamp": included_min_timestamp,
#         "included_max_timestamp": included_max_timestamp,
#         "symbol_column": symbol_column,
#         "timestamp_column": timestamp_column,
#         "target_symbols": target_symbols,
#         "filter_query": filter_query,
#         "limit": limit
#     }
#     sql = loader.render("get_record_range.tpl", **args)
#     # logger.info(f"sql:=========== {sql}")
#     results = session.execute(text(sql)).all()
#     return pd.DataFrame([row._asdict() for row in results])

# class DynamicFunction:
#     def __init__(self,name:str,params:List):
#         self.func:Callable=locals()[name]
#         self.params=params
#     def run(self):
#         return self.func(**self.params)
        