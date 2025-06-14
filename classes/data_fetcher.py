#!/usr/bin/env python3
"""Unified data retrieval utilities for Alpha Vantage data."""

from __future__ import annotations

import sqlite3
import requests
import time
import json
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


class DataFetcher:
    """Fetch raw data from the Alpha Vantage API and store it in SQLite."""

    def __init__(self, db_name: str = "av_data.db", api_key: str = "demo") -> None:
        self.db_name = db_name
        self.api_key = api_key
        self.call_interval = 0.2  # seconds between API calls (5 calls/sec)

    # ------------------------------------------------------------------
    # connection helpers
    # ------------------------------------------------------------------
    def _connect(self, db_name: str | None = None) -> sqlite3.Connection:
        """Return a connection to the configured database."""
        return sqlite3.connect(db_name or self.db_name)

    def _fetch_alphavantage_data(self, function: str, **params: Any) -> Dict[str, Any] | None:
        """Call the Alpha Vantage API with retries and basic rate limiting."""
        base_url = "https://www.alphavantage.co/query"
        params = {k: v for k, v in params.items() if v is not None}
        payload = {"function": function, "apikey": self.api_key}
        payload.update(params)

        for attempt in range(3):
            try:
                resp = requests.get(base_url, params=payload, timeout=10)
                time.sleep(self.call_interval)
                if resp.status_code != 200:
                    print(f"HTTP {resp.status_code}: {resp.reason}")
                    continue
                data = resp.json()
                if "Note" in data:
                    print(data["Note"])
                    time.sleep((2 ** attempt) * self.call_interval)
                    continue
                if "Error Message" in data:
                    print(data["Error Message"])
                    return None
                return data
            except requests.exceptions.RequestException as exc:
                print(f"Request error: {exc}")
                time.sleep((2 ** attempt) * self.call_interval)
        return None

    def _av_request(self, function: str, **params: Any) -> Dict[str, Any] | None:
        """Backward-compatible wrapper for :meth:`_fetch_alphavantage_data`."""
        return self._fetch_alphavantage_data(function, **params)

    # ------------------------------------------------------------------
    # database creation / logging
    # ------------------------------------------------------------------
    def create_database(self, db_name: str | None = None) -> None:
        """Create all required tables in the SQLite database."""
        conn = self._connect(db_name)
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS raw_price_data (
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
            conn.execute(
                """CREATE TABLE IF NOT EXISTS update_log (
                run_time TEXT,
                ticker TEXT,
                table_name TEXT,
                PRIMARY KEY (run_time, ticker, table_name)
            )"""
            )
        conn.close()

    def _log_update(self, ticker: str, table: str, db_name: str | None = None) -> None:
        conn = self._connect(db_name)
        with conn:
            conn.execute(
                "INSERT INTO update_log (run_time, ticker, table_name) VALUES (?, ?, ?)",
                (datetime.now().isoformat(timespec="seconds"), ticker, table),
            )
        conn.close()

    # ------------------------------------------------------------------
    # fundamental data
    # ------------------------------------------------------------------
    def store_company_overview(self, tickers: List[str], db_name: str | None = None) -> None:
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
        for t in tickers:
            self._log_update(t, table, db_name)

    def get_income_statement(self, ticker: str, period: str = "annual") -> pd.DataFrame | None:
        data = self._av_request("INCOME_STATEMENT", symbol=ticker)
        if not data:
            return None
        key = "annualReports" if period == "annual" else "quarterlyReports"
        return pd.DataFrame(data.get(key, []))

    def get_balance_sheet(self, ticker: str, period: str = "annual") -> pd.DataFrame | None:
        data = self._av_request("BALANCE_SHEET", symbol=ticker)
        if not data:
            return None
        key = "annualReports" if period == "annual" else "quarterlyReports"
        return pd.DataFrame(data.get(key, []))

    def get_cash_flow(self, ticker: str, period: str = "annual") -> pd.DataFrame | None:
        data = self._av_request("CASH_FLOW", symbol=ticker)
        if not data:
            return None
        key = "annualReports" if period == "annual" else "quarterlyReports"
        return pd.DataFrame(data.get(key, []))

    def _store_fundamental_report(
        self,
        tickers: List[str],
        function: str,
        table: str,
        period: str,
        db_name: str | None = None,
    ) -> None:
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
        for t in tickers:
            self._log_update(t, table, db_name)

    def _update_fundamental_report(
        self,
        ticker: str,
        function: str,
        table: str,
        period: str,
        db_name: str | None = None,
    ) -> None:
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
            existing = {row[0] for row in cur.fetchall()}
            data = self._av_request(function, symbol=ticker)
            if not data:
                return
            key = "annualReports" if period == "annual" else "quarterlyReports"
            for rep in data.get(key, []):
                fdate = rep.get("fiscalDateEnding")
                if fdate in existing:
                    continue
                conn.execute(
                    f"INSERT OR REPLACE INTO {table} (ticker, fiscal_date_ending, period, data)"
                    " VALUES (?, ?, ?, ?)",
                    (ticker, fdate, period, pd.Series(rep).to_json()),
                )
        conn.close()
        self._log_update(ticker, table, db_name)

    def store_income_statement(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        self._store_fundamental_report(tickers, "INCOME_STATEMENT", "fundamental_income_statement", period, db_name)

    def store_balance_sheet(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        self._store_fundamental_report(tickers, "BALANCE_SHEET", "fundamental_balance_sheet", period, db_name)

    def store_cash_flow(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        self._store_fundamental_report(tickers, "CASH_FLOW", "fundamental_cash_flow", period, db_name)

    def update_income_statement(self, ticker: str, period: str = "annual", db_name: str | None = None) -> None:
        self._update_fundamental_report(ticker, "INCOME_STATEMENT", "fundamental_income_statement", period, db_name)

    def update_balance_sheet(self, ticker: str, period: str = "annual", db_name: str | None = None) -> None:
        self._update_fundamental_report(ticker, "BALANCE_SHEET", "fundamental_balance_sheet", period, db_name)

    def update_cash_flow(self, ticker: str, period: str = "annual", db_name: str | None = None) -> None:
        self._update_fundamental_report(ticker, "CASH_FLOW", "fundamental_cash_flow", period, db_name)

    def store_all_fundamentals(self, tickers: List[str], period: str = "annual", db_name: str | None = None) -> None:
        self.store_company_overview(tickers, db_name)
        self.store_income_statement(tickers, period, db_name)
        self.store_balance_sheet(tickers, period, db_name)
        self.store_cash_flow(tickers, period, db_name)

    # ------------------------------------------------------------------
    # price data
    # ------------------------------------------------------------------
    def store_daily_prices(self, tickers: List[str], outputsize: str = "compact", db_name: str | None = None) -> None:
        for ticker in tickers:
            self.update_daily_prices(ticker, outputsize=outputsize, db_name=db_name)

    def update_daily_prices(self, ticker: str, outputsize: str = "compact", db_name: str | None = None) -> None:
        data = self._av_request("TIME_SERIES_DAILY_ADJUSTED", symbol=ticker, outputsize=outputsize)
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
            cur = conn.execute(f"SELECT date FROM {table} WHERE ticker=?", (ticker,))
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
        self._log_update(ticker, table, db_name)

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
        params = {"symbol": ticker, "interval": interval, "series_type": series_type}
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
            key = next((k for k in data.keys() if k.startswith("Technical")), None)
            if key and data.get(key):
                for dt, val in data[key].items():
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
        self._log_update(ticker, table, db_name)

    # ------------------------------------------------------------------
    # economic indicators
    # ------------------------------------------------------------------
    def store_economic_indicator(self, indicator: str, db_name: str | None = None) -> None:
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
        self._log_update(indicator, table, db_name)

    # ------------------------------------------------------------------
    # news sentiment
    # ------------------------------------------------------------------
    def store_news_sentiment(self, tickers: List[str], db_name: str | None = None) -> None:
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
                data = self._av_request("NEWS_SENTIMENT", tickers=ticker, sort="LATEST", limit=50)
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
        for t in tickers:
            self._log_update(t, table, db_name)

    # ------------------------------------------------------------------
    # metadata helpers
    # ------------------------------------------------------------------
    def get_last_update_time(self, db_name: str | None = None) -> str | None:
        conn = self._connect(db_name)
        cur = conn.execute("SELECT MAX(run_time) FROM update_log")
        row = cur.fetchone()
        conn.close()
        return row[0] if row and row[0] else None

    def get_updated_tickers(self, since: str | None = None, db_name: str | None = None) -> List[str]:
        conn = self._connect(db_name)
        query = "SELECT DISTINCT ticker FROM update_log"
        params: List[Any] = []
        if since:
            query += " WHERE run_time >= ?"
            params.append(since)
        cur = conn.execute(query, params)
        tickers = [row[0] for row in cur.fetchall()]
        conn.close()
        return tickers

    # ------------------------------------------------------------------
    # daily runner
    # ------------------------------------------------------------------
    def run_daily_update(self, tickers: List[str], db_name: str | None = None) -> None:
        for ticker in tickers:
            self.update_daily_prices(ticker, db_name=db_name)

    # ------------------------------------------------------------------
    # simplified raw data storage helpers
    # ------------------------------------------------------------------
    def store_fundamental_data(self, ticker: str, function: str, db_name: str | None = None) -> None:
        """Fetch and store fundamental data from Alpha Vantage."""
        data = self._fetch_alphavantage_data(function, symbol=ticker)
        if not data:
            return
        conn = sqlite3.connect(db_name or self.db_name)
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS fundamental_data (
                    ticker TEXT,
                    function TEXT,
                    data TEXT,
                    last_updated TEXT,
                    PRIMARY KEY(ticker, function)
                )"""
            )
            conn.execute(
                "INSERT OR REPLACE INTO fundamental_data (ticker, function, data, last_updated) VALUES (?, ?, ?, ?)",
                (
                    ticker,
                    function,
                    json.dumps(data),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        conn.close()

    def store_technical_data(
        self,
        ticker: str,
        indicator: str,
        interval: str = "daily",
        time_period: int | None = None,
        series_type: str = "close",
        db_name: str | None = None,
    ) -> None:
        """Fetch and store technical indicator data."""
        params = {
            "symbol": ticker,
            "interval": interval,
            "series_type": series_type,
            "time_period": time_period,
        }
        data = self._fetch_alphavantage_data(indicator, **params)
        if not data:
            return
        conn = sqlite3.connect(db_name or self.db_name)
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS technical_data (
                    ticker TEXT,
                    indicator TEXT,
                    interval TEXT,
                    time_period INTEGER,
                    series_type TEXT,
                    data TEXT,
                    last_updated TEXT,
                    PRIMARY KEY(ticker, indicator, interval, time_period, series_type)
                )"""
            )
            conn.execute(
                "INSERT OR REPLACE INTO technical_data (ticker, indicator, interval, time_period, series_type, data, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    ticker,
                    indicator,
                    interval,
                    time_period,
                    series_type,
                    json.dumps(data),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        conn.close()

    def store_economic_data(self, function: str, db_name: str | None = None) -> None:
        """Fetch and store economic indicator data."""
        data = self._fetch_alphavantage_data(function)
        if not data:
            return
        conn = sqlite3.connect(db_name or self.db_name)
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS economic_data (
                    function TEXT PRIMARY KEY,
                    data TEXT,
                    last_updated TEXT
                )"""
            )
            conn.execute(
                "INSERT OR REPLACE INTO economic_data (function, data, last_updated) VALUES (?, ?, ?)",
                (
                    function,
                    json.dumps(data),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        conn.close()

    def store_news_sentiment(
        self,
        tickers: str | List[str] | None = None,
        topics: str | None = None,
        db_name: str | None = None,
    ) -> None:
        """Fetch and store news sentiment data."""
        tickers_param = ",".join(tickers) if isinstance(tickers, list) else tickers
        data = self._fetch_alphavantage_data(
            "NEWS_SENTIMENT", tickers=tickers_param, topics=topics, sort="LATEST", limit=50
        )
        if not data:
            return
        conn = sqlite3.connect(db_name or self.db_name)
        with conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS news_sentiment (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tickers TEXT,
                    topics TEXT,
                    data TEXT,
                    last_updated TEXT
                )"""
            )
            conn.execute(
                "INSERT INTO news_sentiment (tickers, topics, data, last_updated) VALUES (?, ?, ?, ?)",
                (
                    tickers_param,
                    topics,
                    json.dumps(data),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        conn.close()
