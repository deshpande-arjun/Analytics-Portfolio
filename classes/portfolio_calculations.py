#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:02:51 2025

@author: arjundeshpande
"""

import pandas as pd
import numpy as np

class PortfolioCalculations:
    """
    Processes market data for financial calculations such as returns, volatility, and correlation.
    """
    ### NEED TO ADD:
        # Initiate method
        # Think about what could be the inputs for this class
        # Integrate returns and other functions into this class
    ###
    
    
    def reshape_stock_prices(stock_prices_df, metric="close"):
        """
        Reshape stock prices data to have tickers as columns and dates as the index.
        
        Parameters:
        - stock_prices_df (DataFrame): DataFrame from get_stock_prices_data with columns ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'].
        - metric (str): The column name to extract ('open', 'high', 'low', 'close', 'volume').
        
        Returns:
        - DataFrame: Pivoted DataFrame with tickers as columns and dates as index.
        """
        if stock_prices_df.empty:
            print("⚠️ Warning: No stock price data available!")
            return pd.DataFrame()
    
        # ✅ Step 1: Ensure column exists in data
        if metric not in stock_prices_df.columns:
            raise ValueError(f"❌ Invalid metric '{metric}'. Available options: ['open', 'high', 'low', 'close', 'volume']")
    
        # ✅ Step 2: Pivot data to have tickers as columns
        reshaped_df = stock_prices_df.pivot(index="date", columns="ticker", values=metric)
    
        # ✅ Step 3: Sort index for time-series consistency
        reshaped_df = reshaped_df.sort_index()
    
        print(f"✅ Reshaped data for '{metric}': {reshaped_df.shape[0]} rows, {reshaped_df.shape[1]} tickers.")
    
        return reshaped_df

    @staticmethod
    def calculate_returns(price_data):
        """Compute daily log returns."""
        return np.log(price_data / price_data.shift(1)).dropna()

    @staticmethod
    def calculate_volatility(price_data, window=30):
        """Compute rolling volatility."""
        return price_data.pct_change().rolling(window=window).std()

    @staticmethod
    def calculate_correlation(price_data):
        """Compute correlation matrix for assets."""
        return price_data.pct_change().corr()
