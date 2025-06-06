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
    def store_company_overview(self, tickers: List[str]) -> None:
        """Fetch ``OVERVIEW`` data for tickers and store in the database."""
        conn = sqlite3.connect(self.db_name)
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
    # technical indicators
    # ------------------------------------------------------------------
    def store_technical_indicator(
        self,
        ticker: str,
        indicator: str,
        interval: str = "daily",
        time_period: int | None = None,
        series_type: str = "close",
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

        conn = sqlite3.connect(self.db_name)
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
    def store_economic_indicator(self, indicator: str) -> None:
        """Fetch and store a global/economic indicator series."""
        data = self._av_request(indicator)
        if not data:
            return

        conn = sqlite3.connect(self.db_name)
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
    def store_news_sentiment(self, tickers: List[str]) -> None:
        """Fetch and store Alpha Vantage ``NEWS_SENTIMENT`` data."""
        conn = sqlite3.connect(self.db_name)
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

