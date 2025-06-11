#!/usr/bin/env python3
"""Unified data retrieval utilities."""

from __future__ import annotations

from typing import List

from .alpha_vantage_data import AlphaVantageData

import pandas as pd


class DataFetcher(AlphaVantageData):
    """Pull raw data from internet sources and store in a database.

    This class currently relies on Alpha Vantage for data retrieval. It
    provides convenience wrappers around :class:`AlphaVantageData` and adds
    helpers for price data. Data is stored in tables like ``raw_price_data`` and
    the fundamental tables defined in :class:`AlphaVantageData`.
    """

    # ------------------------------------------------------------------
    # price data
    # ------------------------------------------------------------------
    def store_daily_prices(
        self,
        tickers: List[str],
        outputsize: str = "compact",
        db_name: str | None = None,
    ) -> None:
        """Fetch daily prices for ``tickers`` and store them.

        This uses the ``TIME_SERIES_DAILY_ADJUSTED`` endpoint and writes the
        data to the ``raw_price_data`` table.
        """
        for ticker in tickers:
            self.update_daily_prices(ticker, outputsize=outputsize, db_name=db_name)

    def update_daily_prices(
        self,
        ticker: str,
        outputsize: str = "compact",
        db_name: str | None = None,
    ) -> None:
        """Update price data for ``ticker`` inserting only new rows."""
        data = self._av_request(
            "TIME_SERIES_DAILY_ADJUSTED", symbol=ticker, outputsize=outputsize
        )
        if not data or "Time Series (Daily)" not in data:
            return

        conn = self._connect(db_name)
        table = "raw_price_data"
        with conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                date TEXT,
                ticker TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adjusted_close REAL,
                volume REAL,
                PRIMARY KEY (date, ticker)
            )"""
            )
            cur = conn.execute(
                f"SELECT date FROM {table} WHERE ticker=?", (ticker,)
            )
            existing = {row[0] for row in cur.fetchall()}

            for dt, row in data["Time Series (Daily)"].items():
                if dt in existing:
                    continue
                try:
                    record = (
                        dt,
                        ticker,
                        float(row["1. open"]),
                        float(row["2. high"]),
                        float(row["3. low"]),
                        float(row["4. close"]),
                        float(row["5. adjusted close"]),
                        float(row["6. volume"]),
                    )
                except (KeyError, ValueError):
                    continue
                conn.execute(
                    f"INSERT OR REPLACE INTO {table} (date, ticker, open, high, low, close, adjusted_close, volume)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    record,
                )
        conn.close()

