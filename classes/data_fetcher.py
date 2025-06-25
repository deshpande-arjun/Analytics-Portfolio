#!/usr/bin/env python3
"""Unified data retrieval utilities for Alpha Vantage data."""

from __future__ import annotations

from sqlalchemy import text
from ..db.core import get_engine
import requests
import time
import json
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


class DataFetcher:
    """Fetch raw data from the Alpha Vantage API and store it in a database."""

    def __init__(self, db_name: str = "av_data.db", api_key: str = "demo") -> None:
        self.db_name = db_name
        self.api_key = api_key
        # Alpha Vantage allows up to 75 calls per minute for most plans.
        # ``call_interval`` defines the sleep time between API requests.
        self.call_interval = 60 / 75

    # ------------------------------------------------------------------
    # connection helpers
    # ------------------------------------------------------------------
    def _connect(self):
        """Return the shared SQLAlchemy engine."""
        return get_engine()

    def _fetch_alphavantage_data(
        self, function: str, **params: Any
    ) -> Dict[str, Any] | None:
        """Call the Alpha Vantage API respecting rate limits and retries."""

        # Remove parameters set to ``None`` so they are not sent to the API
        filtered = {k: v for k, v in params.items() if v is not None}

        base_url = "https://www.alphavantage.co/query"
        payload = {"function": function, "apikey": self.api_key}
        payload.update(filtered)

        for attempt in range(3):
            # simple rate limiting
            time.sleep(self.call_interval)
            try:
                resp = requests.get(base_url, params=payload, timeout=10)
                if resp.status_code != 200:
                    print(f"⚠️ HTTP error: {resp.status_code}")
                    time.sleep(self.call_interval * (2**attempt))
                    continue

                data = resp.json()

                # Handle API level errors and notes
                if isinstance(data, dict) and (
                    "Error Message" in data or "Note" in data
                ):
                    if "Error Message" in data:
                        print(f"⚠️ API error: {data['Error Message']}")
                        return None
                    if "Note" in data:
                        print(f"⚠️ API note: {data['Note']}")
                        # Exponential backoff on rate limit note
                        time.sleep(self.call_interval * (2**attempt))
                        continue

                return data

            except requests.RequestException as exc:  # pragma: no cover - network
                print(f"⚠️ API request error: {exc}")
                time.sleep(self.call_interval * (2**attempt))

        return None

    def _av_request(self, function: str, **params: Any) -> Dict[str, Any] | None:
        """Backward compatible wrapper around :meth:`_fetch_alphavantage_data`."""

        return self._fetch_alphavantage_data(function, **params)

    # ------------------------------------------------------------------
    # ETF data
    # ------------------------------------------------------------------
    def fetch_etf_profile(self, symbol: str) -> Dict[str, Any] | None:
        """Return the ETF profile data for ``symbol``."""

        return self._av_request("ETF_PROFILE", symbol=symbol)

    # ------------------------------------------------------------------
    # database creation / logging
    # ------------------------------------------------------------------
    def create_database(self, db_name: str | None = None) -> None:
        """Create all required tables in the configured database."""
        engine = self._connect()
        with engine.begin() as conn:
            conn.exec_driver_sql(
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
            conn.exec_driver_sql(
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
                conn.exec_driver_sql(
                    f"""CREATE TABLE IF NOT EXISTS {tbl} (
                    ticker TEXT,
                    fiscal_date_ending TEXT,
                    period TEXT,
                    data TEXT,
                    PRIMARY KEY (ticker, fiscal_date_ending, period)
                )"""
                )
            conn.exec_driver_sql(
                """CREATE TABLE IF NOT EXISTS technical_indicators (
                date TEXT,
                ticker TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, ticker, indicator)
            )"""
            )
            conn.exec_driver_sql(
                """CREATE TABLE IF NOT EXISTS economic_indicators (
                date TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, indicator)
            )"""
            )
            conn.exec_driver_sql(
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
            conn.exec_driver_sql(
                """CREATE TABLE IF NOT EXISTS update_log (
                run_time TEXT,
                ticker TEXT,
                table_name TEXT,
                PRIMARY KEY (run_time, ticker, table_name)
            )"""
            )
        # Engine connections are automatically closed

    def _log_update(self, ticker: str, table: str, db_name: str | None = None) -> None:
        engine = self._connect()
        with engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO update_log (run_time, ticker, table_name) VALUES (?, ?, ?)",
                (datetime.now().isoformat(timespec="seconds"), ticker, table),
            )

    # ------------------------------------------------------------------
    # raw Alpha Vantage data storage helpers
    # ------------------------------------------------------------------
    def store_fundamental_data(
        self,
        ticker: str,
        function: str,
        db_name: str | None = None,
        **params: Any,
    ) -> None:
        """Fetch and store raw fundamental data."""

        data = self._fetch_alphavantage_data(function, symbol=ticker, **params)
        if data is None:
            return

        engine = self._connect()
        table = "fundamental_data"
        df = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "function": function,
                    "data": json.dumps(data),
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT,
                function TEXT,
                data TEXT,
                last_updated TEXT,
                PRIMARY KEY(ticker, function)
            )"""
            )
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")

    def store_technical_data(
        self,
        ticker: str,
        indicator: str,
        interval: str = "daily",
        time_period: int | None = None,
        series_type: str = "close",
        db_name: str | None = None,
        **params: Any,
    ) -> None:
        """Fetch and store raw technical indicator data."""

        query_params = {
            "symbol": ticker,
            "interval": interval,
            "series_type": series_type,
            **params,
        }
        if time_period is not None:
            query_params["time_period"] = time_period

        data = self._fetch_alphavantage_data(indicator, **query_params)
        if data is None:
            return

        engine = self._connect()
        table = "technical_data"
        df = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "indicator": indicator,
                    "interval": interval,
                    "time_period": time_period,
                    "series_type": series_type,
                    "data": json.dumps(data),
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
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
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")

    def store_economic_data(
        self,
        function: str,
        db_name: str | None = None,
        **params: Any,
    ) -> None:
        """Fetch and store raw economic data."""

        data = self._fetch_alphavantage_data(function, **params)
        if data is None:
            return

        engine = self._connect()
        table = "economic_data"
        df = pd.DataFrame(
            [
                {
                    "function": function,
                    "data": json.dumps(data),
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                function TEXT PRIMARY KEY,
                data TEXT,
                last_updated TEXT
            )"""
            )
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")

    def store_news_sentiment(
        self,
        tickers: List[str] | str | None = None,
        topics: List[str] | str | None = None,
        db_name: str | None = None,
        **params: Any,
    ) -> None:
        """Fetch and store raw news sentiment data."""

        def _to_param(val: List[str] | str | None) -> str | None:
            if val is None:
                return None
            if isinstance(val, list):
                return ",".join(val)
            return val

        query_params = params.copy()
        ticker_param = _to_param(tickers)
        topic_param = _to_param(topics)
        if ticker_param:
            query_params["tickers"] = ticker_param
        if topic_param:
            query_params["topics"] = topic_param

        data = self._fetch_alphavantage_data("NEWS_SENTIMENT", **query_params)
        if data is None:
            return

        engine = self._connect()
        table = "news_sentiment"
        df = pd.DataFrame(
            [
                {
                    "tickers": ticker_param,
                    "topics": topic_param,
                    "data": json.dumps(data),
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tickers TEXT,
                topics TEXT,
                data TEXT,
                last_updated TEXT
            )"""
            )
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")

    # ------------------------------------------------------------------
    # fundamental data
    # ------------------------------------------------------------------
    def store_company_overview(
        self, tickers: List[str], db_name: str | None = None
    ) -> None:
        engine = self._connect()
        table = "fundamental_overview"
        records = []
        for ticker in tickers:
            data = self._av_request("OVERVIEW", symbol=ticker)
            if data:
                records.append({"ticker": ticker, "data": pd.Series(data).to_json()})

        if not records:
            return

        df = pd.DataFrame(records)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT PRIMARY KEY,
                data   TEXT
            )"""
            )
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")
        for t in tickers:
            self._log_update(t, table, db_name)

    def get_income_statement(
        self, ticker: str, period: str = "annual"
    ) -> pd.DataFrame | None:
        data = self._av_request("INCOME_STATEMENT", symbol=ticker)
        if not data:
            return None
        key = "annualReports" if period == "annual" else "quarterlyReports"
        return pd.DataFrame(data.get(key, []))

    def get_balance_sheet(
        self, ticker: str, period: str = "annual"
    ) -> pd.DataFrame | None:
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
        engine = self._connect()
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT,
                fiscal_date_ending TEXT,
                period TEXT,
                data TEXT,
                PRIMARY KEY (ticker, fiscal_date_ending, period)
            )"""
            )
            key = "annualReports" if period == "annual" else "quarterlyReports"
            records = []
            for ticker in tickers:
                data = self._av_request(function, symbol=ticker)
                if not data:
                    continue
                for rep in data.get(key, []):
                    fdate = rep.get("fiscalDateEnding")
                    records.append(
                        {
                            "ticker": ticker,
                            "fiscal_date_ending": fdate,
                            "period": period,
                            "data": pd.Series(rep).to_json(),
                        }
                    )
            if records:
                df = pd.DataFrame(records)
                df.to_sql(table, conn, if_exists="append", index=False, method="multi")
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
        engine = self._connect()
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT,
                fiscal_date_ending TEXT,
                period TEXT,
                data TEXT,
                PRIMARY KEY (ticker, fiscal_date_ending, period)
            )"""
            )
            cur = conn.exec_driver_sql(
                f"SELECT fiscal_date_ending FROM {table} WHERE ticker=? AND period=?",
                (ticker, period),
            )
            existing = {row[0] for row in cur.fetchall()}
            data = self._av_request(function, symbol=ticker)
            if not data:
                return
            key = "annualReports" if period == "annual" else "quarterlyReports"
            records = []
            for rep in data.get(key, []):
                fdate = rep.get("fiscalDateEnding")
                if fdate in existing:
                    continue
                records.append(
                    {
                        "ticker": ticker,
                        "fiscal_date_ending": fdate,
                        "period": period,
                        "data": pd.Series(rep).to_json(),
                    }
                )
            if records:
                pd.DataFrame(records).to_sql(
                    table, conn, if_exists="append", index=False, method="multi"
                )
        self._log_update(ticker, table, db_name)

    def store_income_statement(
        self, tickers: List[str], period: str = "annual", db_name: str | None = None
    ) -> None:
        self._store_fundamental_report(
            tickers, "INCOME_STATEMENT", "fundamental_income_statement", period, db_name
        )

    def store_balance_sheet(
        self, tickers: List[str], period: str = "annual", db_name: str | None = None
    ) -> None:
        self._store_fundamental_report(
            tickers, "BALANCE_SHEET", "fundamental_balance_sheet", period, db_name
        )

    def store_cash_flow(
        self, tickers: List[str], period: str = "annual", db_name: str | None = None
    ) -> None:
        self._store_fundamental_report(
            tickers, "CASH_FLOW", "fundamental_cash_flow", period, db_name
        )

    def update_income_statement(
        self, ticker: str, period: str = "annual", db_name: str | None = None
    ) -> None:
        self._update_fundamental_report(
            ticker, "INCOME_STATEMENT", "fundamental_income_statement", period, db_name
        )

    def update_balance_sheet(
        self, ticker: str, period: str = "annual", db_name: str | None = None
    ) -> None:
        self._update_fundamental_report(
            ticker, "BALANCE_SHEET", "fundamental_balance_sheet", period, db_name
        )

    def update_cash_flow(
        self, ticker: str, period: str = "annual", db_name: str | None = None
    ) -> None:
        self._update_fundamental_report(
            ticker, "CASH_FLOW", "fundamental_cash_flow", period, db_name
        )

    def store_all_fundamentals(
        self, tickers: List[str], period: str = "annual", db_name: str | None = None
    ) -> None:
        self.store_company_overview(tickers, db_name)
        self.store_income_statement(tickers, period, db_name)
        self.store_balance_sheet(tickers, period, db_name)
        self.store_cash_flow(tickers, period, db_name)

    # ------------------------------------------------------------------
    # price data
    # ------------------------------------------------------------------
    def store_daily_prices(
        self,
        tickers: List[str],
        outputsize: str = "compact",
        db_name: str | None = None,
    ) -> None:
        for ticker in tickers:
            self.update_daily_prices(ticker, outputsize=outputsize, db_name=db_name)

    def update_daily_prices(
        self, ticker: str, outputsize: str = "compact", db_name: str | None = None
    ) -> None:
        data = self._av_request(
            "TIME_SERIES_DAILY_ADJUSTED", symbol=ticker, outputsize=outputsize
        )
        if not data or "Time Series (Daily)" not in data:
            return
        engine = self._connect()
        table = "raw_price_data"
        records = []
        for dt, row in data["Time Series (Daily)"].items():
            try:
                records.append(
                    {
                        "date": dt,
                        "ticker": ticker,
                        "open": float(row["1. open"]),
                        "high": float(row["2. high"]),
                        "low": float(row["3. low"]),
                        "close": float(row["4. close"]),
                        "adjusted_close": float(row["5. adjusted close"]),
                        "volume": float(row["6. volume"]),
                    }
                )
            except (KeyError, ValueError):
                continue
        if not records:
            return
        df = pd.DataFrame(records)
        with engine.begin() as conn:
            conn.exec_driver_sql(
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
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")

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
        engine = self._connect()
        table = "technical_indicators"
        records = []
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
                records.append(
                    {"date": dt, "ticker": ticker, "indicator": indicator, "value": val}
                )
        if not records:
            return
        df = pd.DataFrame(records)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                date TEXT,
                ticker TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, ticker, indicator)
            )"""
            )
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")
        self._log_update(ticker, table, db_name)

    # ------------------------------------------------------------------
    # economic indicators
    # ------------------------------------------------------------------
    def store_economic_indicator(
        self, indicator: str, db_name: str | None = None
    ) -> None:
        data = self._av_request(indicator)
        if not data:
            return
        engine = self._connect()
        table = "economic_indicators"
        series = next((v for k, v in data.items() if isinstance(v, list)), None)
        records = []
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
                records.append({"date": dt, "indicator": indicator, "value": val})
        if not records:
            return
        df = pd.DataFrame(records)
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                date TEXT,
                indicator TEXT,
                value REAL,
                PRIMARY KEY (date, indicator)
            )"""
            )
            df.to_sql(table, conn, if_exists="append", index=False, method="multi")
        self._log_update(indicator, table, db_name)

    # ------------------------------------------------------------------
    # metadata helpers
    # ------------------------------------------------------------------
    def get_last_update_time(self, db_name: str | None = None) -> str | None:
        engine = self._connect()
        with engine.connect() as conn:
            row = conn.exec_driver_sql(
                "SELECT MAX(run_time) FROM update_log"
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_updated_tickers(
        self, since: str | None = None, db_name: str | None = None
    ) -> List[str]:
        engine = self._connect()
        query = "SELECT DISTINCT ticker FROM update_log"
        params: List[Any] = []
        if since:
            query += " WHERE run_time >= ?"
            params.append(since)
        with engine.connect() as conn:
            cur = conn.exec_driver_sql(query, params)
            tickers = [row[0] for row in cur.fetchall()]
        return tickers

    # ------------------------------------------------------------------
    # daily runner
    # ------------------------------------------------------------------
    def run_daily_update(self, tickers: List[str], db_name: str | None = None) -> None:
        for ticker in tickers:
            self.update_daily_prices(ticker, db_name=db_name)
