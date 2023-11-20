import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from commons.logger import logger
from main import app
from services.stream import StreamService,StreamDataManager
from services.base import BaseDataManager
from sqlalchemy.sql.expression import Executable
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import Select
client = TestClient(app)

EXPECTED_GET_ALL_SPEC="SELECT stream_cfg.id, stream_cfg.name, stream_cfg.description, stream_cfg.signal_name, stream_cfg.timestep_seconds, stream_cfg.timestep_days, stream_cfg.version, stream_cfg.timestamp_field, stream_cfg.symbol_field, stream_cfg.storage_backend, stream_cfg.same_table_name FROM stream_cfg ORDER BY stream_cfg.id LIMIT {param_1} OFFSET {param_2}"
EXPECTED_GET_ALL_FULL="SELECT stream_cfg.id, stream_cfg.name, stream_cfg.description, stream_cfg.signal_name, stream_cfg.timestep_seconds, stream_cfg.timestep_days, stream_cfg.version, stream_cfg.timestamp_field, stream_cfg.symbol_field, stream_cfg.storage_backend, stream_cfg.same_table_name FROM stream_cfg WHERE stream_cfg.id IN ({param_1}) AND stream_cfg.name IN ({param_2})"
verified_json={'kind': 'stream', 'metadata': {'description': 'No description', 'name': 'stream_test', 'signal_name': 'test_stream', 'storage_backend': 'DatabaseStorage', 'symbol_field': 'symbol_', 'timestamp_field': 'indexed_timestamp_', 'timestep': {'days': 5, 'seconds': 0}, 'to_create': True, 'version': 2}, 'spec': {'stream_fields': [{'is_nullable': True, 'is_primary_key': False, 'name': 'type', 'type': 'varchar(20)'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'quy', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'nam', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhaplaithuantangtruongqoq', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhaplaithuantangtruongsvck', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'tongthunhaphoatdongtangtruongqoq', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'tongthunhaphoatdongtangtruongsvck', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhapngoailai', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhaplaithuanluyke', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhaplaithuanluyketangtruongsvck', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'tongthunhaphoatdongluyke', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'tongthunhaphoatdongtangtruong', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhapngoailailuyke', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhapngoailaitangtruongqoq', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhapngoailaitangtruongsvck', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'loinhuansaothueqoq', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'loinhuansauthuesvck', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'loinhuansauthueluyke', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'loinhuansauthuetangtruong', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'nim_q', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'nimtangtruongsvck', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'cir_ttm', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'casa_q', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'tylenoxau_q', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'tyle_baonoxau_q', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'ldr_q', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'car_ttm', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'eps_ttm', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'pe_ttm', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'pb_ttm', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'last_updated', 'type': 'timestamp'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'thunhaplaithuan', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'chovay', 'type': 'float'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'chovaytangtruongsvck', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'tongthunhaphoatdong', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'loinhuansauthue', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'giacophieu', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'chiphihoatdong', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'cir', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'chiphiduphong', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'chiphiduphongtangtruongsvck', 'type': 'numeric'}, {'is_nullable': True, 'is_primary_key': False, 'name': 'giacophieu_last_updated', 'type': 'text'}]}}
class TestStream(unittest.TestCase):
  def get_raw_query(self,query:Executable):
    if isinstance(query, Select):
      query= query.compile(compile_kwargs={"literal_binds": True})
    if isinstance(query, Query):
      query=query.statement.compile(compile_kwargs={"literal_binds": True})
    return str(query).replace('\n',"")

  @patch.object(BaseDataManager, 'get_all')
  def test_get_all_stream(self,mock_get_all):
      res=client.get("/stream/")
      mock_get_all.assert_called_once()
      query = mock_get_all.call_args.args[0]
      raw_query=self.get_raw_query(query)
      self.assertEqual(raw_query,EXPECTED_GET_ALL_SPEC.format(param_1=10,param_2=0))

  @patch.object(BaseDataManager, 'get_all')
  def test_get_streams(self,mock_get_all):
      res=client.get("/stream?id=1,2&name=abc,def")
      mock_get_all.assert_called_once()
      query = mock_get_all.call_args.args[0]
      raw_query=self.get_raw_query(query)
      logger.info(f"{raw_query}")
      self.assertEqual(raw_query,EXPECTED_GET_ALL_FULL.format(param_1="1, 2",param_2="'abc', 'def'"))
      
  def test_verify_stream(self):
      res=client.post("/stream/verify",json=verified_json)
