#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:06:22 2025

@author: arjundeshpande
"""



#%% Main script
from config import Class_dir, Data_dir, Base_dir, Portfolio_file

import pandas as pd
import os
import sys
# import requests
# import json

# Add `classes/` directory to Python's module search path
sys.path.append(Class_dir)

# Import necessary classes
from classes import MarketData, PortfolioDecomposer, PortfolioCalculations

# Initialize MarketData
db_name=os.path.join(Data_dir, "stocks.db")
meta_file=os.path.join(Data_dir, "etf_metadata.json")
market_data = MarketData(db_name,meta_file)

# Load Portfolio DataFrame
if os.path.exists(Portfolio_file):
    portfolio = pd.read_csv(Portfolio_file)
    print("Portfolio data loaded successfully.")
else:
    raise FileNotFoundError(f"Portfolio file not found: {Portfolio_file}")

etf_list = [x for x in portfolio.Symbol[portfolio.SubCategory=='ETF']] # filter the etf list from portfolio
#stock_list = [x for x in real_port.Symbol[real_port.SubCategory=='COMMON']] # filter the etf list from portfolio
# etf_sectors_dict = market_data.get_etf_sectors()
# etf_holdings_dict = market_data.get_etf_holdings()

port_decomposer = PortfolioDecomposer(portfolio, market_data)

port_to_stocks = port_decomposer.decompose_stocks()

port_to_stocks2,port_to_sectors = port_decomposer.decompose_sectors()

port_to_stocks2.to_excel("port_to_stocks.xlsx", index=False)

#%% Rough Work for stock prices data
zstock_list = list(port_to_stocks2.ticker)
zstock_data = market_data.get_stock_data(zstock_list)

popular_etf_tickers = ['SPY','IVV','VOO','QQQ','DIA','VTI','IWM','EFA','EEM','TLT','HYG','GLD','SLV','GDX']
market_data.store_etf_data(popular_etf_tickers )

sp500_sector_tickers = ['XLY','XLP','XLE','XLF','XLV','XLI','XLB','XLK','XLU','XLRE','XLC']
market_data.store_etf_data(sp500_sector_tickers)

stock_list_tickers = list(port_to_stocks.ticker)

stock_sectorETF_list = stock_list_tickers + sp500_sector_tickers

import yfinance as yf
zticker_list = ['AAPL', 'META', 'NVDA']
zstart_date = "2025-02-10"
zend_date   = "2025-02-19"
df    = yf.download(zticker_list, start=zstart_date, end=zend_date)#['Adj Close']  #,threads=True
dfaa  = yf.download(zticker_list, period='5d',threads=True, group_by='ticker')#['Adj Close']  #,threads=True
df_meta = dfaa['META']
df_reset = dfaa.reset_index(['ticker'])

# =============================================================================
dfaa  = yf.download(zticker_list, period='5d',threads=True)#, group_by='ticker')#['Adj Close']  #,threads=True
df_pull  = yf.download(zticker_list, period='5d',threads=True)#['Adj Close']  #, group_by='ticker',threads=True


market_data = MarketData(db_name,meta_file)
market_data.store_stock_prices(stock_sectorETF_list, period=None, start_date=zstart_date,end_date=zend_date, chunk_size=50)

import sqlite3
conn = sqlite3.connect(db_name)
cursor = conn.cursor()
aa6 = cursor.execute("SELECT * FROM stock_prices2").fetchall()
conn.close()

zstart_date = "2025-02-10"
zend_date   = "2025-02-19"

#########
import time
import datetime

zstart_date = "2019-01-01"
zend_date   = "2024-01-02"

end_time_str = "04:30"  # Desired end time in HH:MM format
while True:
    now = datetime.datetime.now()
    end_time = datetime.datetime.strptime(end_time_str, "%H:%M").replace(year=now.year, month=now.month, day=now.day)
    
    if now >= end_time:
        break
    
    # Your script's code here
    market_data = MarketData(db_name,meta_file)
    market_data.store_stock_prices(stock_sectorETF_list, period=None, start_date=zstart_date,end_date=zend_date, chunk_size=50)
    
    time.sleep(300)  # Wait for 5 minutes (300 seconds)

#########


#%% Rough Work for Performance attribution:

etf_sectors_dict = market_data.get_etf_sectors()

#set SPY as Benchmark 
benchmark = etf_sectors_dict['SPY']

#all stock and sector etf tickers needed:
sp500_sector_tickers = ['XLY','XLP','XLE','XLF','XLV','XLI','XLB','XLK','XLU','XLRE','XLC']
stock_list_tickers = list(port_to_stocks.ticker)
stock_sectorETF_list = stock_list_tickers + sp500_sector_tickers

#get price data:
price_data = market_data.get_stock_prices_data(stock_sectorETF_list )

#initiate Portfolio Calculations
#porfolio_calculations = 

# reshape data into standard df format:
price_df = reshape_stock_prices(price_data, metric="close")
returns_data = calculate_returns(price_df)
#convert daily returns to monthly:
returns_monthly = aggregate_returns(returns_data, "monthly")

port_to_stocks2["weight"] = port_to_stocks2["allocation"]/port_to_stocks2["allocation"].sum()

#####
# Run the function to get sector weights and returns of the portfolios
zport_sector_weights, zport_sector_returns = aggregate_portfolio_by_sector(returns_monthly.reset_index(), port_to_stocks2)


etf_sectors_dict = market_data.get_etf_sectors()
etf_holdings_dict = market_data.get_etf_holdings()
# Select SP500 or QQQ as the benchmark:
zbenchmark_sector_weights = etf_sectors_dict['SPY']
# get the constituents of the benchmark ETF
benchmark_holdings = etf_holdings_dict['SPY']

#decomposing benchmark holdings
benchmark_holdings['PositionValue'] = benchmark_holdings['weight']*100
benchmark_decomposer = PortfolioDecomposer(benchmark_holdings, market_data)
benchmark_to_stocks2,benchmark_to_sectors = benchmark_decomposer.decompose_sectors()

#add the port weight column to the benchmark wt=allocation_i/total_allocation
benchmark_to_stocks2['weight'] = benchmark_to_stocks2['allocation']/benchmark_to_stocks2['allocation'].sum()

# #convert sector to GICS sector categories:
# benchmark_holdings["gics_sector"] = benchmark_holdings.sector.apply(map_to_gics_sector)

# Run the function to get sector weights and returns of the benchmark
zbenchmark_sector_weights, zbenchmark_sector_returns = aggregate_portfolio_by_sector(returns_monthly.reset_index(), benchmark_to_stocks2)

# Run the function
attribution_results = brinson_hood_beebower(zport_sector_weights, zport_sector_returns, zbenchmark_sector_weights, zbenchmark_sector_returns)

columns_to_multiply = ['allocation_effect', 'selection_effect', 'total_active_return']
attribution_results[columns_to_multiply] = attribution_results[columns_to_multiply] * 100

# Display results
import ace_tools as tools
tools.display_dataframe_to_user(name="BHB Attribution Results", dataframe=attribution_results)



#%% FUNCTIONS TO BE ADDED TO Portfolio Calculations
import numpy as np

import pandas as pd
import numpy as np

def aggregate_portfolio_by_sector(returns_df, port_to_stocks):
    """
    Aggregates stock-level portfolio data into sector-level weights and returns.
    
    Parameters:
    - returns_df (DataFrame): DataFrame of stock-level returns (indexed by date).
    - port_to_stocks (DataFrame): Portfolio allocation details with tickers and sectors.

    Returns:
    - sector_weights_df (DataFrame): Sector weights per date.
    - sector_returns_df (DataFrame): Sector returns per date.
    """
    # Merge returns with sector data
    merged_df = returns_df.melt(id_vars=["date"], var_name="ticker", value_name="return")
    merged_df = merged_df.merge(port_to_stocks, on="ticker", how="left")

    # Calculate sector returns: Weighted sum of stock returns in each sector
    sector_returns_df = merged_df.groupby(["date", "gics_sector"]).apply(
        lambda x: np.sum(x["return"] * x["weight"])
    ).reset_index(name="sector_return")

    # Calculate sector weights
    sector_weights_df = merged_df.groupby(["date", "gics_sector"])["weight"].sum().reset_index()

    return sector_weights_df, sector_returns_df





# sector_weights    = zport_sector_weights.copy() 
# sector_returns    = zport_sector_returns.copy() 
# benchmark_weights = zbenchmark_sector_weights.copy() 
# benchmark_returns = zbenchmark_sector_returns.copy()

import pandas as pd

import pandas as pd

def brinson_hood_beebower(portfolio_weights, portfolio_returns, benchmark_weights, benchmark_returns):
    """
    Calculates Brinson-Hood-Beebower (BHB) attribution.

    Parameters:
    - portfolio_weights (DataFrame): Portfolio sector weights over time. 
      Columns: ["date", "gics_sector", "weight"]
    - portfolio_returns (DataFrame): Portfolio sector returns over time.
      Columns: ["date", "gics_sector", "sector_return"]
    - benchmark_weights (DataFrame): Benchmark sector weights over time.
      Columns: ["date", "gics_sector", "weight"]
    - benchmark_returns (DataFrame): Benchmark sector returns over time.
      Columns: ["date", "gics_sector", "sector_return"]

    Returns:
    - attribution_df (DataFrame): Allocation, Selection, and Total Active Return per date.
    """

    # Merge portfolio weights with returns
    merged_df = portfolio_weights.merge(portfolio_returns, on=["date", "gics_sector"], how="inner")
    
    # Rename portfolio weight to avoid confusion
    merged_df.rename(columns={"weight": "weight_portfolio", "sector_return": "return_portfolio"}, inplace=True)

    # Merge with benchmark weights
    merged_df = merged_df.merge(benchmark_weights, on=["date", "gics_sector"], how="left", suffixes=("", "_benchmark"))
    
    # Merge with benchmark returns
    merged_df = merged_df.merge(benchmark_returns, on=["date", "gics_sector"], how="left", suffixes=("", "_benchmark_return"))

    # Rename columns for clarity
    merged_df.rename(columns={"weight": "weight_benchmark", "sector_return": "return_benchmark"}, inplace=True)

    # Fill missing benchmark data with 0 (assumption)
    merged_df.fillna(0, inplace=True)

    # Compute allocation effect: (Wp - Wb) * Rb
    merged_df["allocation_effect"] = (merged_df["weight_portfolio"] - merged_df["weight_benchmark"]) * merged_df["return_benchmark"]

    # Compute selection effect: Wp * (Rp - Rb)
    merged_df["selection_effect"] = merged_df["weight_portfolio"] * (merged_df["return_portfolio"] - merged_df["return_benchmark"])

    # Compute total active return
    merged_df["total_active_return"] = merged_df["allocation_effect"] + merged_df["selection_effect"]

    # Summarize by date
    attribution_df = merged_df.groupby("date")[["allocation_effect", "selection_effect", "total_active_return"]].sum().reset_index()

    return attribution_df





#%%

    # mapping = {
    #     "XLB": "Materials",
    #     "XLC":"Communication Services",
    #     "XLY": "Consumer Discretionary",
    #     "XLP": "Consumer Staples",
    #     "XLE": "Energy",
    #     "XLF Services": "Financials",
    #     "XLV": "Health Care",
    #     "XLI": "Industrials",
    #     "XLRE": "Real Estate",
    #     "XLK": "Information Technology",
    #     "XLU": "Utilities",
    #     "N/A": "Unknown Unmapped",
    #     }

#this is to create a df of benchmark sector weights for each month:
# Define all 12 GICS sectors (including "Unknown unmapped")
def convert_benchmark_wts_df(zbenchmark_sector_weights):
    all_sectors = ["tech", "healthcare", "financials", "energy", "utilities", 
               "industrials", "consumer staples", "consumer discretionary",
               "materials", "real estate", "communication", "Unknown unmapped"]

    # Define 14 month-end dates (assuming latest month-end as a reference)
    date_range = pd.date_range(end="2025-02-28", periods=14, freq="M")
    
    # Initialize expanded DataFrame
    expanded_df = []
    
    for date in date_range:
        for sector in all_sectors:
            weight = zbenchmark_sector_weights.loc[zbenchmark_sector_weights["gics_sector"] == sector, "weight"].sum()
            weight = weight if not np.isnan(weight) else 0  # Assign 0 if sector not found
            expanded_df.append({"date": date, "gics_sector": sector, "weight": weight})
    
    # Convert list to DataFrame
    expanded_df = pd.DataFrame(expanded_df)
    return expanded_df

def map_to_gics_sector(label):
    """Maps sector labels from Yahoo Finance! to Official GICS sector names.
       This function is coded based on the YF names, might need to be updated
       if the data source is changed.        
    """
    
    mapping = {
        "Basic Materials": "Materials",
        "Communication Services":"Communication Services",
        "Consumer Cyclical": "Consumer Discretionary",
        "Consumer Defensive": "Consumer Staples",
        "Energy": "Energy",
        "Financial Services": "Financials",
        "Healthcare": "Health Care",
        "Industrials": "Industrials",
        "Real Estate": "Real Estate",
        "Technology": "Information Technology",
        "Utilities": "Utilities",
        "N/A": "Unknown Unmapped",
        }
    
    return mapping.get(label, "Unknown Unmapped")  # Default to "Unknown Unmapped" if not found


def reshape_stock_prices(stock_prices_df, metric="close"):
    """
    Reshape stock prices data from long format to wide format with tickers as columns.

    Parameters:
    - stock_prices_df (DataFrame): DataFrame containing stock price data with columns ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'].
    - metric (str): The price metric to extract ('open', 'high', 'low', 'close', 'volume').

    Returns:
    - DataFrame: Wide-format DataFrame with dates as index and tickers as columns.
    """
    # Step 1: Validate Input
    if metric not in stock_prices_df.columns:
        print(f"Error: The requested metric '{metric}' is not found in the data.")
        return None

    print(f"Before pivoting: {stock_prices_df['ticker'].nunique()} unique stocks")

    # Step 2: Ensure 'date' is a valid datetime format
    stock_prices_df["date"] = pd.to_datetime(stock_prices_df["date"], errors="coerce")

    # Step 3: Ensure the DataFrame has unique (date, ticker) pairs
    duplicates = stock_prices_df.duplicated(subset=["date", "ticker"], keep=False)
    if duplicates.any():
        print(f"Warning: Found {duplicates.sum()} duplicate rows. Aggregating by mean.")
        stock_prices_df = stock_prices_df.groupby(["date", "ticker"])[metric].mean().reset_index()

    # Step 4: Pivot Data to Wide Format
    reshaped_df = stock_prices_df.pivot(index="date", columns="ticker", values=metric)

    # Step 5: Validate Output
    print(f"After pivoting: {reshaped_df.shape[1]} stocks")  # Check if the number of stocks matches

    return reshaped_df

import numpy as np
def calculate_returns(price_data):
    """Compute daily log returns."""
    return (price_data / price_data.shift(1) -1) #.dropna() 
#with drop na it did not output returns for many dates such as Feb 2024 to July 9,2025

    
def aggregate_returns(df_returns, frequency):
    """
    Aggregates daily returns to the specified frequency.

    Parameters:
        df_returns (pd.DataFrame): Daily returns DataFrame.
        frequency (str): 'daily', 'monthly', or 'annually'.

    Returns:
        agg_returns (pd.DataFrame): Returns aggregated to the chosen frequency.
    """
    if frequency == 'daily':
        agg_returns = df_returns  # No aggregation
    elif frequency == 'monthly':
        # Compound returns over each month: (1 + r1)*(1 + r2)*... - 1
        agg_returns = df_returns.resample('M').apply(lambda x: np.prod(1 + x) - 1)
    elif frequency == 'annually':
        agg_returns = df_returns.resample('A').apply(lambda x: np.prod(1 + x) - 1)
    else:
        raise ValueError("Frequency must be 'daily', 'monthly', or 'annually'")
    return agg_returns

