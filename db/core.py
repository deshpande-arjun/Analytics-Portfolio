from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///av_data.db",
)


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine."""
    return create_engine(DATABASE_URL, future=True)


@contextmanager
def get_session() -> Session:
    """Provide a transactional SQLAlchemy session."""
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    finally:
        session.close()
