from typing import List, Optional
from sqlalchemy import ForeignKey, func
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)

from models.base import SQLModel



class StreamCfgModel(SQLModel):
    __tablename__ = "stream_cfg"

    id: Mapped[int] = mapped_column("id", primary_key=True, autoincrement=True,server_default=func.nextval('stream_cfg_id_seq'))
    name: Mapped[str] = mapped_column("name")
    description: Mapped[str] = mapped_column("description")
    signal_name: Mapped[str] = mapped_column("signal_name")
    timestep_hours: Mapped[int] = mapped_column("timestep_hours")
    timestep_days: Mapped[int] = mapped_column("timestep_days")
    timestep_minutes: Mapped[int] = mapped_column("timestep_minutes")
    version: Mapped[str] = mapped_column("version")
    timestamp_field: Mapped[str] = mapped_column("timestamp_field")
    symbol_field: Mapped[str] = mapped_column("symbol_field")
    storage_backend: Mapped[str] = mapped_column("storage_backend")
    same_table_name: Mapped[bool] = mapped_column("same_table_name")
    type: Mapped[str] = mapped_column("type")

    miners = relationship("MinerStreamRelationshipModel", back_populates='stream_cfg')
    stream_fields: Mapped[List["StreamFieldsModel"]] = relationship(back_populates="stream")

class StreamFieldsModel(SQLModel):
    __tablename__ = "stream_fields"

    id: Mapped[int] = mapped_column("id", primary_key=True)
    stream_id: Mapped[int] = mapped_column("stream_id",ForeignKey("stream_cfg.id"))
    name: Mapped[str] = mapped_column("name")
    type: Mapped[str] = mapped_column("type")
    is_nullable: Mapped[bool] = mapped_column("is_nullable")
    is_primary_key: Mapped[bool] = mapped_column("is_primary_key")
    
    stream: Mapped["StreamCfgModel"] = relationship(back_populates="stream_fields")