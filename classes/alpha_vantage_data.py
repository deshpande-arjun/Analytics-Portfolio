#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility class for retrieving various data from Alpha Vantage and storing
it in an SQLite database. This includes fundamental, technical, economic,
and news sentiment endpoints. It mirrors the style of the existing
``MarketData`` class but focuses exclusively on the Alpha Vantage API.
"""

from __future__ import annotations

import os
import sqlite3
import requests
from typing import Dict, Any, List
import pandas as pd


class AlphaVantageData:
    """Fetch and store data from Alpha Vantage."""

    def __init__(self, db_name: str = "av_data.db", api_key: str = "demo") -> None:
        self.db_name = db_name
        self.api_key = api_key

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _connect(self, db_name: str | None = None) -> sqlite3.Connection:
        """Return a connection to ``db_name`` (or :pyattr:`self.db_name`)."""
        return sqlite3.connect(db_name or self.db_name)

    def create_database(self, db_name: str | None = None) -> None:
        """Create a new SQLite database with all required tables."""
        conn = self._connect(db_name)
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS fundamental_overview (
                ticker TEXT PRIMARY KEY,
                data   TEXT
            )"""
            )
            for tbl in (
                "fundamental_income_statement",
                "fundamental_balance_sheet",
                "fundamental_cash_flow",
            ):
                conn.execute(
                    f"""CREATE TABLE IF NOT EXISTS {tbl} (
                    ticker TEXT,
                    fiscal_date_ending TEXT,
                    period TEXT,
                    data TEXT,
                    PRIMARY KEY (ticker, fiscal_date_ending, period)
                )"""
                )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS technical_indicators (
                date TEXT,
                ticker TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, ticker, indicator)
            )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS economic_indicators (
                date TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, indicator)
            )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS news_sentiment (
                time_published TEXT,
                ticker TEXT,
                headline TEXT,
                summary TEXT,
                sentiment TEXT,
                score REAL,
                PRIMARY KEY (time_published, ticker)
            )"""
            )
        conn.close()

    def record_exists(
        self,
        table: str,
        ticker: str,
        date: str,
        date_column: str = "date",
        db_name: str | None = None,
    ) -> bool:
        """Return ``True`` if ``table`` has a row for ``ticker`` and ``date``."""
        conn = self._connect(db_name)
        cur = conn.execute(
            f"SELECT 1 FROM {table} WHERE ticker=? AND {date_column}=? LIMIT 1",
            (ticker, date),
        )
        exists = cur.fetchone() is not None
        conn.close()
        return exists

    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # internal helper to call Alpha Vantage
    # ------------------------------------------------------------------
    def _av_request(self, function: str, **params: Any) -> Dict[str, Any] | None:
        """Send a request to the Alpha Vantage API and return the JSON data."""
        base_url = "https://www.alphavantage.co/query"
        payload = {"function": function, "apikey": self.api_key}
        payload.update(params)

        try:
            response = requests.get(base_url, params=payload, timeout=30)
            if response.status_code == 200:
                return response.json()
            print(f"⚠️ API request failed: {response.status_code}")
            return None
        except Exception as exc:  # pragma: no cover - network
            print(f"⚠️ API request error: {exc}")
            return None

    # ------------------------------------------------------------------
    # fundamental data
    # ------------------------------------------------------------------
    def store_company_overview(self, tickers: List[str], db_name: str | None = None) -> None:
        """Fetch ``OVERVIEW`` data for tickers and store in the database."""
        conn = self._connect(db_name)
        table = "fundamental_overview"
        with conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT PRIMARY KEY,
                data   TEXT
            )"""
            )

            for ticker in tickers:
                data = self._av_request("OVERVIEW", symbol=ticker)
                if data:
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table} (ticker, data) VALUES (?, ?)",
                        (ticker, pd.Series(data).to_json()),
                    )
        conn.close()

    # ------------------------------------------------------------------
    # fundamental fetch helpers
    # ------------------------------------------------------------------
    def get_income_statement(self, ticker: str, period: str = "annual") -> pd.DataFrame | None:
        """Return the income statement for ``ticker`` as a DataFrame."""
        data = self._av_request("INCOME_STATEMENT", symbol=ticker)
        if not data:
            return None
        key = "annualReports" if period == "annual" else "quarterlyReports"
        return pd.DataFrame(data.get(key, []))

    def get_balance_sheet(self, ticker: str, period: str = "annual") -> pd.DataFrame | None:
        """Return the balance sheet for ``ticker`` as a DataFrame."""
        data = self._av_request("BALANCE_SHEET", symbol=ticker)
        if not data:
            return None
        key = "annualReports" if period == "annual" else "quarterlyReports"
        return pd.DataFrame(data.get(key, []))

    def get_cash_flow(self, ticker: str, period: str = "annual") -> pd.DataFrame | None:
        """Return the cash flow statement for ``ticker`` as a DataFrame."""
        data = self._av_request("CASH_FLOW", symbol=ticker)
        if not data:
            return None
        key = "annualReports" if period == "annual" else "quarterlyReports"
        return pd.DataFrame(data.get(key, []))

    def get_income_statements(self, tickers: List[str], period: str = "annual") -> Dict[str, pd.DataFrame]:
        """Return income statements for multiple tickers."""
        result = {}
        for t in tickers:
            df = self.get_income_statement(t, period)
            if df is not None:
                result[t] = df
        return result

    def get_balance_sheets(self, tickers: List[str], period: str = "annual") -> Dict[str, pd.DataFrame]:
        """Return balance sheets for multiple tickers."""
        result = {}
        for t in tickers:
            df = self.get_balance_sheet(t, period)
            if df is not None:
                result[t] = df
        return result

    def get_cash_flows(self, tickers: List[str], period: str = "annual") -> Dict[str, pd.DataFrame]:
        """Return cash flow statements for multiple tickers."""
        result = {}
        for t in tickers:
            df = self.get_cash_flow(t, period)
            if df is not None:
                result[t] = df
        return result

    def _store_fundamental_report(
        self,
        tickers: List[str],
        function: str,
        table: str,
        period: str,
        db_name: str | None = None,
    ) -> None:
        """Internal helper to store a fundamental report."""
        conn = self._connect(db_name)
        with conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT,
                fiscal_date_ending TEXT,
                period TEXT,
                data TEXT,
                PRIMARY KEY (ticker, fiscal_date_ending, period)
            )"""
            )

            key = "annualReports" if period == "annual" else "quarterlyReports"
            for ticker in tickers:
                data = self._av_request(function, symbol=ticker)
                if not data:
                    continue
                for rep in data.get(key, []):
                    fdate = rep.get("fiscalDateEnding")
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table} (ticker, fiscal_date_ending, period, data)"
                        " VALUES (?, ?, ?, ?)",
                        (ticker, fdate, period, pd.Series(rep).to_json()),
                    )
        conn.close()

    def _update_fundamental_report(
        self,
        ticker: str,
        function: str,
        table: str,
        period: str,
        db_name: str | None = None,
    ) -> None:
        """Update ``table`` with new reports for ``ticker``."""
        conn = self._connect(db_name)
        with conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT,
                fiscal_date_ending TEXT,
                period TEXT,
                data TEXT,
                PRIMARY KEY (ticker, fiscal_date_ending, period)
            )"""
            )

            cur = conn.execute(
                f"SELECT fiscal_date_ending FROM {table} WHERE ticker=? AND period=?",
                (ticker, period),
            )
            existing_dates = {row[0] for row in cur.fetchall()}

            data = self._av_request(function, symbol=ticker)
            if not data:
                return

            key = "annualReports" if period == "annual" else "quarterlyReports"
            for rep in data.get(key, []):
                fdate = rep.get("fiscalDateEnding")
                if fdate in existing_dates:
                    continue
                conn.execute(
                    f"INSERT OR REPLACE INTO {table} (ticker, fiscal_date_ending, period, data)"
                    " VALUES (?, ?, ?, ?)",
                    (ticker, fdate, period, pd.Series(rep).to_json()),
                )
        conn.close()


    def store_income_statement(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        """Fetch and store income statements for the tickers provided."""
        self._store_fundamental_report(
            tickers,
            "INCOME_STATEMENT",
            "fundamental_income_statement",
            period,
            db_name,
        )

    def store_balance_sheet(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        """Fetch and store balance sheets for the tickers provided."""
        self._store_fundamental_report(
            tickers,
            "BALANCE_SHEET",
            "fundamental_balance_sheet",
            period,
            db_name,
        )

    def store_cash_flow(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        """Fetch and store cash flow statements for the tickers provided."""
        self._store_fundamental_report(
            tickers,
            "CASH_FLOW",
            "fundamental_cash_flow",
            period,
            db_name,
        )

    def update_income_statement(self, ticker: str, period: str = "annual", db_name: str | None = None) -> None:
        """Fetch and insert only new income statement data for ``ticker``."""
        self._update_fundamental_report(
            ticker,
            "INCOME_STATEMENT",
            "fundamental_income_statement",
            period,
            db_name,
        )

    def update_balance_sheet(self, ticker: str, period: str = "annual", db_name: str | None = None) -> None:
        """Fetch and insert only new balance sheet data for ``ticker``."""
        self._update_fundamental_report(
            ticker,
            "BALANCE_SHEET",
            "fundamental_balance_sheet",
            period,
            db_name,
        )

    def update_cash_flow(self, ticker: str, period: str = "annual", db_name: str | None = None) -> None:
        """Fetch and insert only new cash flow data for ``ticker``."""
        self._update_fundamental_report(
            ticker,
            "CASH_FLOW",
            "fundamental_cash_flow",
            period,
            db_name,
        )

    def store_all_fundamentals(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        """Store overview, income statement, balance sheet and cash flow."""
        self.store_company_overview(tickers, db_name)
        self.store_income_statement(tickers, period, db_name)
        self.store_balance_sheet(tickers, period, db_name)
        self.store_cash_flow(tickers, period, db_name)

    # ------------------------------------------------------------------
    # technical indicators
    # ------------------------------------------------------------------
    def store_technical_indicator(
        self,
        ticker: str,
        indicator: str,
        interval: str = "daily",
        time_period: int | None = None,
        series_type: str = "close",
        db_name: str | None = None,
    ) -> None:
        """Fetch a technical indicator (e.g. RSI, SMA) and store to database."""
        params = {
            "symbol": ticker,
            "interval": interval,
            "series_type": series_type,
        }
        if time_period is not None:
            params["time_period"] = time_period

        data = self._av_request(indicator, **params)
        if not data:
            return

        conn = self._connect(db_name)
        table = "technical_indicators"
        with conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                date TEXT,
                ticker TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, ticker, indicator)
            )"""
            )

            # The API returns a dict of date->value pairs under a key like
            # 'Technical Analysis: RSI'. We locate this key dynamically.
            key = next((k for k in data.keys() if k.startswith("Technical")), None)
            if key and data.get(key):
                for dt, val in data[key].items():
                    # For indicators with multiple fields (e.g., MACD) pick the
                    # first numeric value.
                    try:
                        if isinstance(val, dict):
                            val = float(next(iter(val.values())))
                        else:
                            val = float(val)
                    except (ValueError, StopIteration):
                        continue
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table} (date, ticker, indicator, value)"
                        " VALUES (?, ?, ?, ?)",
                        (dt, ticker, indicator, val),
                    )
        conn.close()

    # ------------------------------------------------------------------
    # economic indicators
    # ------------------------------------------------------------------
    def store_economic_indicator(self, indicator: str, db_name: str | None = None) -> None:
        """Fetch and store a global/economic indicator series."""
        data = self._av_request(indicator)
        if not data:
            return

        conn = self._connect(db_name)
        table = "economic_indicators"
        with conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                date TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, indicator)
            )"""
            )

            # Data returned is typically under a key like 'data' or 'Realtime State'
            series = next((v for k, v in data.items() if isinstance(v, list)), None)
            if series:
                for row in series:
                    dt = row.get("date") or row.get("timestamp")
                    val = row.get("value") or row.get("v")
                    if dt is None or val is None:
                        continue
                    try:
                        val = float(val)
                    except ValueError:
                        continue
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table} (date, indicator, value)"
                        " VALUES (?, ?, ?)",
                        (dt, indicator, val),
                    )
        conn.close()

    # ------------------------------------------------------------------
    # news sentiment
    # ------------------------------------------------------------------
    def store_news_sentiment(self, tickers: List[str], db_name: str | None = None) -> None:
        """Fetch and store Alpha Vantage ``NEWS_SENTIMENT`` data."""
        conn = self._connect(db_name)
        table = "news_sentiment"
        with conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                time_published TEXT,
                ticker TEXT,
                headline TEXT,
                summary TEXT,
                sentiment TEXT,
                score REAL,
                PRIMARY KEY (time_published, ticker)
            )"""
            )

            for ticker in tickers:
                data = self._av_request(
                    "NEWS_SENTIMENT", tickers=ticker, sort="LATEST", limit=50
                )
                if not data or "feed" not in data:
                    continue
                for item in data["feed"]:
                    label = item.get("ticker_sentiment", [{}])[0].get("ticker_sentiment_label")
                    score = item.get("ticker_sentiment", [{}])[0].get("ticker_sentiment_score")
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table} (time_published, ticker, headline, summary, sentiment, score)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            item.get("time_published"),
                            ticker,
                            item.get("title"),
                            item.get("summary"),
                            label,
                            float(score) if score is not None else None,
                        ),
                    )
        conn.close()

