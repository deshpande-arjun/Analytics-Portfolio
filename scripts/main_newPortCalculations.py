# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 11:52:39 2025

@author: arjundeshpande
"""
## Main script

import pandas as pd
import numpy as np
import os
#import sys

from config import Class_dir, Data_dir, Base_dir, Portfolio_file

# Import necessary classes
from classes import MarketData, PortfolioDecomposer, PortfolioCalculations

# Initialize MarketData
db_name = os.path.join(Data_dir, "stocks_1yr.db")
meta_file = os.path.join(Data_dir, "etf_metadata.json")
market_data = MarketData(db_name, meta_file)

# Load Portfolio DataFrame
if os.path.exists(Portfolio_file):
    portfolio = pd.read_csv(Portfolio_file)
    print("Portfolio data loaded successfully.")
else:
    raise FileNotFoundError(f"Portfolio file not found: {Portfolio_file}")

# Initialize PortfolioDecomposer with portfolio and market_data
port_decomposer = PortfolioDecomposer(portfolio, market_data)

# Initialize PortfolioCalculations with portfolio and market_data
port_calc = PortfolioCalculations()

# Display relevant columns of portfolio 
keep_cols = ["Symbol", "Description", "PositionValue", "AssetClass", "SubCategory"]
portfolio_display = ( portfolio.loc[:, keep_cols].copy() )     # keep only the requested columns and make a copy
portfolio_display["Portfolio Weight pct"] = portfolio_display.PositionValue * 100 / portfolio_display.PositionValue.sum()
portfolio_display = portfolio_display.sort_values(by="Portfolio Weight pct", ascending=False).reset_index(drop=True)
print("User's portfolio:")
print(portfolio_display)

  
# Decompose the portfolio into stock-level allocations
port_stocks_df = port_decomposer.decompose_stocks()  # Returns [ticker, name, allocation, port_weight_pct]
print("Decomposed stock-level DataFrame:")
print(port_stocks_df.head(20))

# Decompose the portfolio into sector-level allocations
port_sectors_df = port_decomposer.decompose_sectors()  # Returns [gics_sector, allocation, port_weight_pct]
print("Decomposed sector-level DataFrame:")
print(port_sectors_df)

# =============================================================================
# Script for Performance attribution:

### Get monthly returns from price for al stocks in the portfolio
##### Here the portfolio stocks include all the benchmark stocks since both SPY and QQ were part of portfolio
    #   In future if the port does not, then we need to add ticker list of the benchmark stocks
stock_list_tickers = list(port_stocks_df.ticker)
#get price data:
price_data = market_data.get_stock_prices_data(stock_list_tickers)
# reshape data into standard df format:
price_df = port_calc.reshape_stock_prices(price_data, metric="close")
returns_data = port_calc.calculate_returns(price_df)
#convert daily returns to monthly:
returns_monthly = port_calc.aggregate_returns(returns_data, "monthly")

# new addition; Returns stocks and gic_sector at stock level:
port_stock_and_sectors = port_decomposer.decompose_stock_and_sectors() 
#add the port weight column to the benchmark wt=allocation_i/total_allocation
port_stock_and_sectors['weight'] = port_stock_and_sectors['allocation']/port_stock_and_sectors['allocation'].sum()
# Get sector weights and returns of the portfolios
port_sector_weights, port_sector_returns = port_calc.aggregate_portfolio_by_sector(returns_monthly.reset_index(), port_stock_and_sectors)

### Now get Benchmark weights and returns
etf_holdings_dict = market_data.get_etf_holdings()
# Select SP500 or QQQ as the benchmark:
benchmark_holdings = etf_holdings_dict['SPY']
#decomposing benchmark holdings
benchmark_holdings['PositionValue'] = benchmark_holdings['weight']*100
benchmark_decomposer = PortfolioDecomposer(benchmark_holdings, market_data)
# new addition; Returns stocks and gic_sector at stock level:
benchmark_stock_and_sectors = benchmark_decomposer.decompose_stock_and_sectors() # Returns [ticker, name, allocation, port_weight_pct]
#add the port weight column to the benchmark wt=allocation_i/total_allocation
benchmark_stock_and_sectors['weight'] = benchmark_stock_and_sectors['allocation']/benchmark_stock_and_sectors['allocation'].sum()
# Get sector weights and returns of the benchmark
benchmark_sector_weights, benchmark_sector_returns = port_calc.aggregate_portfolio_by_sector(returns_monthly.reset_index(), benchmark_stock_and_sectors)

# Active weights:


# Run Attribution
attribution_results = port_calc.brinson_hood_beebower(port_sector_weights, port_sector_returns, benchmark_sector_weights, benchmark_sector_returns)

# convert returns to percentage
columns_to_multiply = ['allocation_effect', 'selection_effect', 'total_active_return']
attribution_results[columns_to_multiply] = attribution_results[columns_to_multiply] * 100

print("Performance Attribution results in percentage ")
print(attribution_results)