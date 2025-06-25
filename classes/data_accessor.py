#!/usr/bin/env python3
"""Thin wrapper around SQLAlchemy engine for Postgres access."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class DataAccessor:
    """Helper to read/write tables from a Postgres database."""

    def __init__(self, db_url: str) -> None:
        self.engine = create_engine(db_url, pool_pre_ping=True)

    def read_table(self, table: str) -> pd.DataFrame:
        """Load entire ``table`` into a DataFrame."""
        return pd.read_sql_table(table, self.engine)

    def write_frame(self, df: pd.DataFrame, table: str, if_exists: str = "append") -> None:
        """Write ``df`` to ``table``."""
        df.to_sql(table, self.engine, if_exists=if_exists, index=False)

    def raw_engine(self) -> Engine:
        """Return underlying SQLAlchemy engine."""
        return self.engine
