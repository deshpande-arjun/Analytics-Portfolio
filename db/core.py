from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import create_engine, text
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


def ensure_tables() -> None:
    """Create required tables if they do not exist."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS update_log (
                    run_time TEXT,
                    ticker TEXT,
                    table_name TEXT,
                    PRIMARY KEY (run_time, ticker, table_name)
                )
                """
            )
        )


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
