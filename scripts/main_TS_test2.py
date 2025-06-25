#%% Get Russell 3000 stock tickers

import pandas as pd

def get_russell_3000_tickers():
    """
    Downloads the Russell 3000 ETF (IWV) holdings CSV from iShares
    and returns the full list of tickers (~3,000).
    """
    # iShares Russell 3000 ETF (IWV) holdings CSV endpoint
    url = (
        "https://www.ishares.com/us/products/239714/ishares-russell-3000"
        "/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"
    )
    try:
        # Read CSV directly
        df = pd.read_csv(url, skiprows=9)
        tickers = df["Ticker"].dropna().unique().tolist()
        print(f"✅ Retrieved {len(tickers)} tickers (expected ~3000).")
        return tickers
    except Exception as e:
        print("⚠️ Error fetching tickers:", e)
        return []

if __name__ == "__main__":
    tickers_R3000 = get_russell_3000_tickers()
    print(tickers_R3000[:10])
    

#%% Testing AlphaVantageData DataAccessor & FeatureEngineer June 11


"""Example data pipeline using AlphaVantageData, DatabaseAccessor and FeatureEngineer.

The script fetches various data from Alpha Vantage, stores it in a PostgreSQL
database, then reads the data back and computes sample features. The final
result is made available as a pandas DataFrame for further analysis.
"""


import os
import pandas as pd
from dotenv import load_dotenv

from config import AV_api_key
from classes import DataFetcher, DatabaseAccessor, FeatureEngineer

# Ensure DATABASE_URL is set for PostgreSQL storage
load_dotenv()
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:SuperSecretPW@localhost:5432/av_data_test1",
)
# ---------------------------------------------------------------------------
# Fetch and store raw data
# ---------------------------------------------------------------------------

# DONT run everytime
# =============================================================================
fetcher = DataFetcher(api_key=AV_api_key)

# Fundamental reports: overview, income statement, balance sheet, cash flow
fetcher.store_all_fundamentals(tickers_R3000, period='quarterly')

# Daily price history
fetcher.store_daily_prices(tickers_R3000, outputsize="full")

# News sentiment
fetcher.store_news_sentiment(tickers_R3000)

# Technical indicators we want to store
TECHNICALS = [
    ("SMA", 20),
    ("EMA", 20),
    ("RSI", 14),
]

for ticker in tickers_R3000:
    for indicator, period in TECHNICALS:
        fetcher.store_technical_indicator(ticker, indicator, time_period=period)

print("Raw data downloaded and stored.")


# ---------------------------------------------------------------------------
# Access stored data (runs in 3 mins)
# ---------------------------------------------------------------------------
accessor = DatabaseAccessor()
prices = accessor.get_prices(tickers_R3000)
fundamentals = accessor.get_fundamentals(tickers_R3000)

# Example join of prices with company overview
#price_overview = accessor.get_prices_with_overview(tickers_R3000)

# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
engineer = FeatureEngineer(accessor)
ratios = engineer.compute_financial_ratios(tickers_R3000) ##code ran till this line ?whats diff btw price & price_overview
volatility = engineer.rolling_volatility(prices, window=20)

# Merge engineered features with daily prices
full_data = prices.merge(ratios, on="ticker", how="left")
full_data = full_data.merge(volatility[["ticker", "date", "volatility"]],
                            on=["ticker", "date"], how="left")

# ``full_data`` now contains price history along with basic ratios and
# volatility estimates. This DataFrame can be used for modeling or
# regression analysis.
print(full_data.tail())

full_data.ticker.unique_values()

# Export data to excel spreadsheet:
# Create an ExcelWriter object
writer = pd.ExcelWriter('data_june12.xlsx', engine='xlsxwriter')

fundamentals['balance_sheet'].to_excel(writer, "balance_sheet.xlsx")
fundamentals['overview'].to_excel(writer, "overview.xlsx")
fundamentals['income_statement'].to_excel(writer, "income_statement.xlsx")
fundamentals['cash_flow'].to_excel(writer, "cash_flow.xlsx")
# save excel
writer.close()


print(price_overview.head())


