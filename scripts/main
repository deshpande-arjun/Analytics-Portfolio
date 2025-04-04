#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:06:22 2025

@author: arjundeshpande
"""



#%% Rough Work
from portfolio_analytics.config import Class_dir, Data_dir, Base_dir, Portfolio_file

import os
import sys
# import requests
# import json

# Add `classes/` directory to Python's module search path
sys.path.append(Class_dir)

# Import necessary classes
# Import necessary classes
from portfolio_analytics.classes import MarketData, PortfolioDecomposer, PortfolioCalculations

# Initialize MarketData
market_data = MarketData()

# Load Portfolio DataFrame
import pandas as pd

if os.path.exists(Portfolio_file):
    portfolio = pd.read_csv(Portfolio_file)
    print("✅ Portfolio data loaded successfully.")
else:
    raise FileNotFoundError(f"❌ Portfolio file not found: {Portfolio_file}")

etf_list = [x for x in portfolio.Symbol[portfolio.SubCategory=='ETF']] # filter the etf list from portfolio
#stock_list = [x for x in real_port.Symbol[real_port.SubCategory=='COMMON']] # filter the etf list from portfolio

# =============================================================================
# revisit
# =============================================================================
db_name=os.path.join(Data_dir, "stocks.db")
meta_file=os.path.join(Data_dir, "etf_metadata.json")
market_data = MarketData(db_name,meta_file)

#etf_sectors_dict = market_data.get_etf_sectors()
#etf_holdings_dict = market_data.get_etf_holdings()

port_decomposer = PortfolioDecomposer(real_port, market_data)

port_to_stocks = port_decomposer.decompose_stocks()

