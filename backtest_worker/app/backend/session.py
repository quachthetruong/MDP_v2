from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)
from sqlalchemy import MetaData

from backend.config import config


# create session factory to generate new database sessions
SessionFactory = sessionmaker(
    bind=create_engine(config.database.dsn,pool_pre_ping=True),
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def create_session() -> Iterator[Session]:
    """Create new database session.

    Yields:
        Database session.
    """

    session = SessionFactory()

    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
meta = MetaData()
meta.reflect(bind=create_engine(config.database.dsn,pool_pre_ping=True))