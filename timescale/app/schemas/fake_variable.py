### mock varible 
from schemas.stream import StreamCatalog,StreamMetadata, StreamSpec

initStreamMetadata=StreamMetadata(id=None,name="fake_stream",signal_name="fake_stream",same_table_name=False,
                                  description="initStreamMetadata",timestep={"days": 1, "hours": 0, "minutes": 0},
                                  timestamp_field="indexed_timestamp_",version="1",to_create=False,symbol_field="symbol_",
                                  storage_backend="DatabaseStorage")
initStreamSpec=StreamSpec(stream_fields=[])
initStreamCatalog=StreamCatalog(kind="stream",metadata=initStreamMetadata,spec=initStreamSpec)
class FakeVariable:

  @staticmethod
  def fake_stream_catalog(name)->StreamCatalog:
    new_stream_catalog=initStreamCatalog.model_copy()
    new_stream_catalog.metadata.name=name
    new_stream_catalog.metadata.signal_name=name
    return new_stream_catalog