from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:Secret@localhost:5432/trading_data",
)

_tables_initialized = False


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine and ensure tables exist."""
    engine = create_engine(DATABASE_URL, future=True)
    global _tables_initialized
    if not _tables_initialized:
        ensure_tables(engine)
        _tables_initialized = True
    return engine


def ensure_tables(engine: Engine | None = None) -> None:
    """Create required tables if they do not exist."""
    if engine is None:
        engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS update_log (
                    run_time TIMESTAMPTZ NOT NULL,
                    ticker TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    PRIMARY KEY (run_time, ticker, table_name)
                )
                """
            )
        )
