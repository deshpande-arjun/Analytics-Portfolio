# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 18:08:58 2025

@author: arjundeshpande
"""

import pandas as pd
import os
#import sys

from config import Class_dir, Data_dir, Base_dir, Portfolio_file

# Import necessary classes
from classes import MarketData, PortfolioDecomposer, PortfolioCalculations

# Initialize MarketData
db_name = os.path.join(Data_dir, "stocks_data_5yr.db")
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

# Decompose the portfolio into stock-level allocations
port_stocks_df = port_decomposer.decompose_stocks()  # Returns [ticker, name, allocation, port_weight]
print("Decomposed stock-level DataFrame:")
print(port_stocks_df.head())

# Decompose the portfolio into sector-level allocations
port_sectors_df = port_decomposer.decompose_sectors()  # Returns [gics_sector, allocation, port_weight]
print("Decomposed sector-level DataFrame:")
print(port_sectors_df.head())

#%%
### cheking the stock info data
ticker_list =['NVDA','AAPL']
stock_info_data = market_data.get_stock_info_data(ticker_list)
