import pandas as pd
import os
import sys

from config import Class_dir, Data_dir, Base_dir, Portfolio_file

# Import necessary classes
from classes import MarketData, PortfolioDecomposer, PortfolioCalculations, AlphaVantageData

# Initialize MarketData
# db_name = os.path.join(Data_dir, "stocks_data_5yr.db")
# meta_file = os.path.join(Data_dir, "etf_metadata.json")
# market_data = MarketData(db_name, meta_file)

av_data = AlphaVantageData()

#%%
from classes import AlphaVantageData

# Instantiate the helper using the demo API key and a temporary database
av_helper = AlphaVantageData(db_name="av_data_test1.db", api_key="53U4JBZUJNQX0EVO")

# Tickers used for testing
TEST_TICKERS = ["IBM", "AAPL", "MSFT"]

# Fundamental company data
av_helper.store_company_overview(TEST_TICKERS)

# Technical indicators to fetch
TECHNICALS = [
    ("RSI", 14),
    ("SMA", 20),
    ("EMA", 20),
]
for ticker in TEST_TICKERS:
    for indicator, period in TECHNICALS:
        av_helper.store_technical_indicator(ticker, indicator, time_period=period)

# Example economic indicator
av_helper.store_economic_indicator("REAL_GDP")

# News sentiment
av_helper.store_news_sentiment(TEST_TICKERS)

print("Data fetching complete.")

#%%
#!/usr/bin/env python3
"""Example script demonstrating usage of ``AlphaVantageData``.

This script initializes the :class:`AlphaVantageData` class, then
fetches sample fundamental data, technical indicators, news sentiment
and economic indicators for a small list of tickers. The downloaded
data is stored in an SQLite database under the ``Market data`` folder.

The script is intended as a functional smoke test for the
``AlphaVantageData`` class and can be adapted for experimentation.
"""

from __future__ import annotations

import pandas as pd
import os
import sys

from config import Data_dir, AV_api_key
from classes import AlphaVantageData

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# SQLite database file used to store retrieved data
DB_FILE = os.path.join(Data_dir, "av_data_test1.db")
AV_api_key = "53U4JBZUJNQX0EVO"

# Example tickers to fetch data for
TICKERS = ["AAPL", "MSFT", "GOOGL"]

# ---------------------------------------------------------------------------
# Initialize AlphaVantageData
# ---------------------------------------------------------------------------
# The default demo key works but limits the amount of data returned. Set
# ``AV_api_key`` in ``config.py`` to your own key for full access.
av = AlphaVantageData(db_name=DB_FILE, api_key=AV_api_key)

# ---------------------------------------------------------------------------
# Fundamental data
# ---------------------------------------------------------------------------
# Store overview, income statement, balance sheet and cash flow reports
# for each ticker. Data is written to ``DB_FILE``.
av_data = av.store_all_fundamentals(TICKERS, period="annual")

# Optionally fetch DataFrames directly
income_statements = av.get_income_statements(TICKERS)
balance_sheets = av.get_balance_sheets(TICKERS)
cash_flows = av.get_cash_flows(TICKERS)
print("Sample income statement rows:")
for ticker, df in income_statements.items():
    print(ticker, df.head())

# ---------------------------------------------------------------------------
# Technical indicators
# ---------------------------------------------------------------------------
# Here we fetch a couple of popular indicators for each ticker.
for ticker in TICKERS:
    av.store_technical_indicator(ticker, "SMA", interval="daily", time_period=20)
    av.store_technical_indicator(ticker, "RSI", interval="daily", time_period=14)

# ---------------------------------------------------------------------------
# News sentiment
# ---------------------------------------------------------------------------
av.store_news_sentiment(TICKERS)

# ---------------------------------------------------------------------------
# Economic indicator example
# ---------------------------------------------------------------------------
# Fetch one of the economic series provided by Alpha Vantage. Any of the
# valid function names documented by Alpha Vantage can be used here.
av.store_economic_indicator("REAL_GDP")

print("Data fetch complete. Stored data in", DB_FILE)