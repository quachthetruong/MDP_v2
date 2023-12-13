from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)
from backend.config import config
from sqlalchemy.engine.url import URL
from redshift_connector import Connection as RedshiftConnection
import redshift_connector



def get_conn(conn_params:dict) -> RedshiftConnection:
    return redshift_connector.connect(**conn_params)

# engine=create_engine(config.database.redshift, pool_pre_ping=True)
# SessionFactory = sessionmaker(
#     bind= create_engine(config.database.redshift, pool_pre_ping=True),
#     autocommit=False,
#     autoflush=False,
#     expire_on_commit=False,
# )


# def create_redshift_session() -> Iterator[Session]:
#     """Create new database session.

#     Yields:
#         Database session.
#     """

#     session = SessionFactory()

#     try:
#         yield session
#         session.commit()
#     except Exception:
#         session.rollback()
#         raise
#     finally:
#         session.close()
