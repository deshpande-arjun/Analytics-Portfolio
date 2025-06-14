#!/usr/bin/env python3
"""Utilities for reading Alpha Vantage data from SQLite."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List

import pandas as pd


class DatabaseAccessor:
    """Read and normalize raw Alpha Vantage data from the database."""

    def __init__(self, db_name: str = "av_data.db") -> None:
        self.db_name = db_name

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        """Return a connection to :pyattr:`self.db_name`."""
        return sqlite3.connect(self.db_name)

    def _to_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert non-index columns of ``df`` to numeric when possible."""
        for col in df.columns:
            # if col.lower() == "ticker":
            #     continue #skips ticker (string) from convert to numeric
            if pd.api.types.is_object_dtype(df[col]):
                sample = df[col].dropna().astype(str).head(10)  # sample up to 10 non-null entries
                
                if sample.str.match(r'^-?\d+(\.\d+)?$').all():    
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except (ValueError, TypeError) as e: 
                        print(f" Warning: Failed to convert column '{col}' to numeric. Reason: {e}")
                
                else:
                    continue
        return df

    # ------------------------------------------------------------------
    # price data
    # ------------------------------------------------------------------
    def get_prices(
        self,
        tickers: List[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Return price data for ``tickers`` between ``start_date`` and ``end_date``."""
        query = (
            "SELECT date, ticker, open, high, low, close, adjusted_close, volume "
            "FROM raw_price_data"
        )
        params: List[Any] = []
        conditions: List[str] = []
        if tickers:
            placeholders = ",".join("?" for _ in tickers)
            conditions.append(f"ticker IN ({placeholders})")
            params.extend(tickers)
        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        conn = self._connect()
        df = pd.read_sql_query(query, conn, params=params, parse_dates=["date"])
        conn.close()
        df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
        df = self._to_numeric(df).fillna(pd.NA)
        return df

    # ------------------------------------------------------------------
    # fundamental data helpers
    # ------------------------------------------------------------------
    def _load_fundamental_table(
        self,
        table: str,
        tickers: List[str] | None,
        period: str | None,
    ) -> pd.DataFrame:
        query = f"SELECT ticker, fiscal_date_ending, period, data FROM {table}"
        params: List[Any] = []
        conditions: List[str] = []
        if tickers:
            placeholders = ",".join("?" for _ in tickers)
            conditions.append(f"ticker IN ({placeholders})")
            params.extend(tickers)
        if period:
            conditions.append("period = ?")
            params.append(period)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        conn = self._connect()
        raw_df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        if raw_df.empty:
            return pd.DataFrame()
        records: List[Dict[str, Any]] = []
        for _, row in raw_df.iterrows():
            data = json.loads(row["data"]) if row["data"] else {}
            data["ticker"] = row["ticker"]
            data["fiscal_date_ending"] = row["fiscal_date_ending"]
            data["period"] = row["period"]
            records.append(data)
        df = pd.DataFrame(records)
        df = df.sort_values(["ticker", "fiscal_date_ending"]).reset_index(drop=True)
        df = self._to_numeric(df).fillna(pd.NA)
        return df

    def _load_overview(self, tickers: List[str] | None) -> pd.DataFrame:
        query = "SELECT ticker, data FROM fundamental_overview"
        params: List[Any] = []
        if tickers:
            placeholders = ",".join("?" for _ in tickers)
            query += f" WHERE ticker IN ({placeholders})"
            params.extend(tickers)
        conn = self._connect()
        raw_df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        if raw_df.empty:
            return pd.DataFrame()
        records: List[Dict[str, Any]] = []
        for _, row in raw_df.iterrows():
            data = json.loads(row["data"]) if row["data"] else {}
            data["ticker"] = row["ticker"]
            records.append(data)
        df = pd.DataFrame(records)
        df = df.sort_values("ticker").reset_index(drop=True)
        df = self._to_numeric(df).fillna(pd.NA)
        return df

    def get_fundamentals(
        self,
        tickers: List[str] | None = None,
        period: str = "annual",
    ) -> Dict[str, pd.DataFrame]:
        """Return fundamental dataframes for ``tickers``."""
        data = {
            "overview": self._load_overview(tickers),
            "income_statement": self._load_fundamental_table(
                "fundamental_income_statement", tickers, period
            ),
            "balance_sheet": self._load_fundamental_table(
                "fundamental_balance_sheet", tickers, period
            ),
            "cash_flow": self._load_fundamental_table(
                "fundamental_cash_flow", tickers, period
            ),
        }
        return data

    # ------------------------------------------------------------------
    # combined data
    # ------------------------------------------------------------------
    def get_prices_with_overview(
        self,
        tickers: List[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Return price data joined with overview fundamentals."""
        prices = self.get_prices(tickers, start_date, end_date)
        overview = self._load_overview(tickers)
        if overview.empty or prices.empty:
            return prices
        df = prices.merge(overview, on="ticker", how="left")
        return df

    # ------------------------------------------------------------------
    # ETF holdings
    # ------------------------------------------------------------------
    def store_etf_holdings(self, symbol: str, data: Dict[str, Any]) -> None:
        """Store ETF holdings data in the ``etf_holdings`` table."""

        df = pd.DataFrame(data.get("holdings", []))
        if df.empty:
            return

        df = df.rename(
            columns={
                "symbol": "stock_ticker",
                "ticker": "stock_ticker",
                "description": "name",
                "name": "name",
                "assetType": "asset_type",
                "sharesHeld": "shares_held",
                "marketValue": "market_value",
            }
        )

        df["etf_symbol"] = symbol
        df["date_fetched"] = pd.Timestamp("today").strftime("%Y-%m-%d")

        columns = [
            "etf_symbol",
            "stock_ticker",
            "name",
            "asset_type",
            "cusip",
            "isin",
            "weight",
            "shares_held",
            "market_value",
            "sector",
            "date_fetched",
        ]
        for col in columns:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[columns]

        conn = self._connect()
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS etf_holdings (
                    etf_symbol TEXT,
                    stock_ticker TEXT,
                    name TEXT,
                    asset_type TEXT,
                    cusip TEXT,
                    isin TEXT,
                    weight REAL,
                    shares_held REAL,
                    market_value REAL,
                    sector TEXT,
                    date_fetched TEXT,
                    PRIMARY KEY (etf_symbol, stock_ticker, date_fetched)
                )"""
            )
            df.to_sql("etf_holdings", conn, if_exists="append", index=False)
        conn.close()

    def get_etf_holdings(self, symbol: str) -> pd.DataFrame:
        """Return holdings for ``symbol`` from the ``etf_holdings`` table."""

        conn = self._connect()
        df = pd.read_sql_query(
            "SELECT * FROM etf_holdings WHERE etf_symbol = ?",
            conn,
            params=(symbol,),
        )
        conn.close()
        df = self._to_numeric(df).fillna(pd.NA)
        return df
