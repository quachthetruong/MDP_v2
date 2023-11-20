from sqlalchemy import ForeignKey, Table,Column
from sqlalchemy.orm import (
    Mapped,
    mapped_column,relationship,registry
)

from models.base import SQLModel
from models.stream import StreamCfgModel


class MinerCfgModel(SQLModel):
    __tablename__ = "miner_cfg"

    id: Mapped[int] = mapped_column("id", primary_key=True)
    name: Mapped[str] = mapped_column("name")
    description: Mapped[str] = mapped_column("description")
    target_symbols: Mapped[str] = mapped_column("target_symbols")
    timestep_days: Mapped[int] = mapped_column("timestep_days")
    timestep_hours: Mapped[int] = mapped_column("timestep_hours")
    timestep_minutes: Mapped[int] = mapped_column("timestep_minutes")
    schedule: Mapped[str] = mapped_column("schedule")
    start_date_day: Mapped[int] = mapped_column("start_date_day")
    start_date_month: Mapped[int] = mapped_column("start_date_month")
    start_date_year: Mapped[int] = mapped_column("start_date_year")
    start_date_hour: Mapped[int] = mapped_column("start_date_hour")
    file_path: Mapped[str] = mapped_column("file_path")

    streams = relationship("MinerStreamRelationshipModel", back_populates='miner_cfg')


class MinerStreamRelationshipModel(SQLModel):
    __tablename__ = "miner_stream_relationship"

    # id: Mapped[int] = mapped_column("id", primary_key=True)
    stream_id: Mapped[int] = mapped_column("stream_id",ForeignKey("stream_cfg.id"), primary_key=True)
    miner_id: Mapped[int] = mapped_column("miner_id",ForeignKey("miner_cfg.id"), primary_key=True)
    type: Mapped[str] = mapped_column("type", primary_key=True)
    miner_cfg  = relationship("MinerCfgModel", back_populates="streams")
    stream_cfg  = relationship("StreamCfgModel", back_populates="miners")

# class MinerStreamRelationship(object):
#     pass


    
# miner_stream_relationship = Table('miner_stream_relationship', SQLModel.metadata,
#     Column('stream_id', ForeignKey('stream_cfg.id'), primary_key=True),
#     Column('miner_id', ForeignKey('miner_cfg.id'), primary_key=True),
#     Column('type', primary_key=True),
# )

# mapper_registry = registry()
# mapper_registry.map_imperatively(MinerStreamRelationship, miner_stream_relationship)  # this will modify the User class!