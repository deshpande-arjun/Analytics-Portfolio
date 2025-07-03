#!/usr/bin/env python3
"""Normalize stored Alpha Vantage data.

This script reads the raw JSON tables produced by ``DataFetcher`` and
creates new tables with structured columns that are easier to query.
Existing classes :class:`DatabaseAccessor` and :class:`DataFetcher`
are reused to load and write data.
"""

import json
import sqlite3

import pandas as pd

from classes import DatabaseAccessor
from config import AV_db_file

# -------------------------------------------------------------
# connect to the database and initialize helper classes
# -------------------------------------------------------------
accessor = DatabaseAccessor(AV_db_file)
conn = sqlite3.connect(AV_db_file)

# -------------------------------------------------------------
# normalize fundamental overview and reports
# -------------------------------------------------------------
fundamentals = accessor.get_fundamentals(period="quarterlyReports")
overview_df = fundamentals["overview"]
income_df = fundamentals["income_statement"]
balance_df = fundamentals["balance_sheet"]
cash_df = fundamentals["cash_flow"]

# store each dataframe as a new table
overview_df.to_sql("overview_normalized", conn, if_exists="replace", index=False)
income_df.to_sql("income_statement_quarterly", conn, if_exists="replace", index=False)
balance_df.to_sql("balance_sheet_quarterly", conn, if_exists="replace", index=False)
cash_df.to_sql("cash_flow_quarterly", conn, if_exists="replace", index=False)

# -------------------------------------------------------------
# normalize raw technical indicator JSON
# -------------------------------------------------------------
tech_df = pd.read_sql_query(
    "SELECT date, ticker, indicator, value FROM technical_indicators",
    conn,
    parse_dates=["date"],
)

if not tech_df.empty:
    tech_df.to_sql(
        "technical_indicators_normalized",
        conn,
        if_exists="replace",
        index=False,
    )
    
# -------------------------------------------------------------
# normalize economic indicator JSON
# -------------------------------------------------------------
econ_df = pd.read_sql_query(
    "SELECT date, indicator, value FROM economic_indicators",
    conn,
    parse_dates=["date"],
)
if not econ_df.empty:
    econ_df.to_sql(
        "economic_indicators_normalized",
        conn,
        if_exists="replace",
        index=False,
    )

# -------------------------------------------------------------
# normalize news sentiment JSON
# -------------------------------------------------------------
# news_rows = conn.execute("SELECT tickers, topics, data FROM news_sentiment").fetchall()
# news_records = []
# for tickers, topics, raw in news_rows:
#     data = json.loads(raw) if raw else {}
#     for article in data.get("feed", []):
#         base = {
#             "time_published": article.get("time_published"),
#             "headline": article.get("title") or article.get("headline"),
#             "summary": article.get("summary"),
#             "sentiment": article.get("overall_sentiment_label"),
#         }
#         base_score = article.get("overall_sentiment_score")
#         for ts in article.get("ticker_sentiment", []):
#             record = base.copy()
#             record["ticker"] = ts.get("ticker")
#             record["score"] = ts.get("ticker_sentiment_score", base_score)
#             record["request_tickers"] = tickers
#             record["request_topics"] = topics
#             news_records.append(record)
# news_df = pd.DataFrame(news_records)
# if not news_df.empty:
#     news_df.to_sql("news_sentiment_normalized", conn, if_exists="replace", index=False)

conn.close()
