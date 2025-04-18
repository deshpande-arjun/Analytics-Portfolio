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
    def __init__(self, market_data=None):            # ← CHANGED (new)
        """
        Parameters
        ----------
        market_data : object, optional
            Pass your MarketData helper here if you want the class to be able
            to fetch prices or ETF holdings internally. For the methods below
            we don't actually use it, but storing the reference keeps the door
            open.
        """
        self.market_data = market_data
    

    def reshape_stock_prices(self,stock_prices_df, metric="close"):
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

    def calculate_returns(self,price_data):
        """Compute daily log returns."""
        return (price_data / price_data.shift(1) -1) #.dropna() 
    #with drop na it did not output returns for many dates such as Feb 2024 to July 9,2025
    
        
    def aggregate_returns(self,df_returns, frequency):
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

    def aggregate_portfolio_by_sector(self,returns_df, port_to_stocks):
        """
        Aggregates stock-level portfolio data into sector-level weights and returns.
        
        Parameters:
        - returns_df (DataFrame): DataFrame of stock-level returns (indexed by date).
        - port_to_stocks (DataFrame): Portfolio allocation details with tickers and sectors. 
                                        [ticker,name, allocation, port_weight_pct]
    
        Returns:
        - sector_weights_df (DataFrame): Sector weights per date.
        - sector_returns_df (DataFrame): Sector returns per date.
        """
        # Convert weight percentage to decimal format and rename the field to 'weight'
        port_to_stocks = (
            port_to_stocks
              .assign(weight = port_to_stocks["port_weight_pct"] / 100)   # new column
              .drop(columns="port_weight_pct")                            # remove the old one
        )
        
        # Merge returns with sector data
        merged_df = returns_df.melt(id_vars=["date"], var_name="ticker", value_name="return")
        merged_df = merged_df.merge(port_to_stocks, on="ticker", how="left")
    
        # Calculate sector returns: Weighted sum of stock returns in each sector
        
        #earlier approach commented out
        # sector_returns_df = merged_df.groupby(["date", "gics_sector"]).apply(
        #     lambda x: np.sum(x["return"] * x["port_weight_pct"] /100)
        # ).reset_index(name="sector_return")

        sector_returns_df = (
                                merged_df
                                .assign(weighted_return = merged_df["return"] * merged_df["weight"] )
                                .groupby(["date", "gics_sector"], as_index=False)["weighted_return"]
                                .sum()
                                .rename(columns={"weighted_return": "sector_return"})
                            )
    
        # Calculate sector weights
        
        #earlier approach commented out
        # sector_weights_df = merged_df.groupby(["date", "gics_sector"])["port_weight_pct"].sum().reset_index()
        
        sector_weights_df = (
                                merged_df
                                .groupby(["date", "gics_sector"], as_index=False)["weight"]
                                .sum()
                                .rename(columns={"weight": "sector_weight"})
                            )        
        
        return sector_weights_df, sector_returns_df
    
    
    #this is to create a df of benchmark sector weights for each month:
    # Define all 12 GICS sectors (including "Unknown unmapped")
    def convert_benchmark_wts_df(self,benchmark_sector_weights):
        all_sectors = ["tech", "healthcare", "financials", "energy", "utilities", 
                   "industrials", "consumer staples", "consumer discretionary",
                   "materials", "real estate", "communication", "Unknown unmapped"]
    
        # Define 14 month-end dates (assuming latest month-end as a reference)
        date_range = pd.date_range(end="2025-02-28", periods=14, freq="M")
        
        # Initialize expanded DataFrame
        expanded_df = []
        
        for date in date_range:
            for sector in all_sectors:
                weight = benchmark_sector_weights.loc[benchmark_sector_weights["gics_sector"] == sector, "weight"].sum()
                weight = weight if not np.isnan(weight) else 0  # Assign 0 if sector not found
                expanded_df.append({"date": date, "gics_sector": sector, "weight": weight})
        
        # Convert list to DataFrame
        expanded_df = pd.DataFrame(expanded_df)
        return expanded_df
    
    @staticmethod
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

    
    def brinson_hood_beebower(self,portfolio_weights, portfolio_returns, benchmark_weights, benchmark_returns):
        """
        Calculates Brinson-Hood-Beebower (BHB) attribution.
    
        Parameters:
        - portfolio_weights (DataFrame): Portfolio sector weights over time. 
          Columns: ["date", "gics_sector", "sector_weight"]
        - portfolio_returns (DataFrame): Portfolio sector returns over time.
          Columns: ["date", "gics_sector", "sector_return"]
        - benchmark_weights (DataFrame): Benchmark sector weights over time.
          Columns: ["date", "gics_sector", "sector_weight"]
        - benchmark_returns (DataFrame): Benchmark sector returns over time.
          Columns: ["date", "gics_sector", "sector_return"]
    
        Returns:
        - attribution_df (DataFrame): Allocation, Selection, and Total Active Return per date.
        """
    
        # Merge portfolio weights with returns
        merged_df = portfolio_weights.merge(portfolio_returns, on=["date", "gics_sector"], how="inner")
        
        # Rename portfolio weight to avoid confusion
        merged_df.rename(columns={"sector_weight": "weight_portfolio", "sector_return": "return_portfolio"}, inplace=True)
    
        # Merge with benchmark weights
        merged_df = merged_df.merge(benchmark_weights, on=["date", "gics_sector"], how="left", suffixes=("", "_benchmark"))
        
        # Merge with benchmark returns
        merged_df = merged_df.merge(benchmark_returns, on=["date", "gics_sector"], how="left", suffixes=("", "_benchmark_return"))
    
        # Rename columns for clarity
        merged_df.rename(columns={"sector_weight": "weight_benchmark", "sector_return": "return_benchmark"}, inplace=True)
    
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
    
    # sector_weights    = zport_sector_weights.copy() 
    # sector_returns    = zport_sector_returns.copy() 
    # benchmark_weights = zbenchmark_sector_weights.copy() 
    # benchmark_returns = zbenchmark_sector_returns.copy()
    

    
    

    
    
