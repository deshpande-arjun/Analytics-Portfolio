#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:04:16 2025

@author: arjundeshpande
"""
import pandas as pd

class PortfolioDecomposer:
    """
    Decomposes ETF and stock positions into stock-level or sector-level allocations.
    """

    def __init__(self, port, market_data):
        """
        Initialize the PortfolioDecomposer with:
        - port: Raw portfolio DataFrame of ETFs and stocks
        - market_data: An object providing get_etf_sectors(), get_etf_holdings(), and get_stock_info_data()
        """
        self.port = port.copy()
        self.market_data = market_data
        # Pull dictionary of sector data for each ETF
        self.etf_sectors_dict  = market_data.get_etf_sectors() 
        # Pull dictionary of holdings for each ETF
        self.etf_holdings_dict = market_data.get_etf_holdings()

    # --------------------------------------------------------------------------
    # Internal Helper Methods
    # --------------------------------------------------------------------------

    def _get_portfolio_etf_stocks(self):
        """
        Separate the portfolio DataFrame into the subset of ETFs (with data) and the subset of stocks.
        Returns:
            (port_etf, port_stock): DataFrames for the portion of the portfolio that is ETF vs. direct stocks.
        """
        # Rename columns to a consistent format
        self.port = self.port.rename(columns={"Symbol": "ticker", "Description": "name"})  

        # DataFrame of ETFs that exist in the etf_holdings_dict
        port_etf_df = pd.DataFrame(self.etf_holdings_dict.keys(), columns=['ticker'])
        
        # separate direct-stock portion vs. etf portion of the portfolio
        port_stock = self.port[~self.port["ticker"].isin(port_etf_df["ticker"])]
        port_etf = pd.merge(port_etf_df, self.port, on='ticker', how='inner')  # only ETFs that exist in the portfolio

        return port_etf, port_stock

    def _decompose_etf_to_stocks(self, etf_portion):
        """
        Decompose each ETF in 'etf_portion' into its underlying stock holdings.
        Returns a list of DataFrames, each containing the stocks for one ETF plus their allocations.
        """
        decomposed_etf_list = []
        for etf_ticker, position_value in zip(etf_portion["ticker"], etf_portion["PositionValue"]):
            if etf_ticker not in self.etf_holdings_dict:
                # If the holdings are not found, skip
                continue
            temp_df = self.etf_holdings_dict[etf_ticker].copy()
            temp_df["allocation"] = temp_df["weight"] * position_value
            decomposed_etf_list.append(temp_df)
        return decomposed_etf_list

    # --------------------------------------------------------------------------
    # Public Methods
    # --------------------------------------------------------------------------

    # def decompose_stocks(self):
    #     """
    #     Decompose the entire portfolio (stocks + ETFs) into stock-level allocations.
    #     Returns a single DataFrame with columns [ticker, name, allocation, port_weight].
    #     """
    #     port_etf, port_stock = self._get_portfolio_etf_stocks()

    #     # Decompose each ETF into underlying stocks
    #     decomposed_etf_list = self._decompose_etf_to_stocks(port_etf)

    #     # Rename 'PositionValue' -> 'allocation' for the direct stock portion
    #     port_stock = port_stock.rename(columns={"PositionValue": "allocation"})
    #     # Only keep relevant columns
    #     port_stock = port_stock[["ticker", "name", "allocation"]]

    #     # Combine the decomposed ETFs and the direct stocks
    #     all_stocks = pd.concat(decomposed_etf_list + [port_stock], ignore_index=True)
        
    #     # Group by ticker (and name if you wish) to sum allocations from multiple ETFs
    #     # name might differ if an ETF invests in the same ticker, so be cautious with that merge
    #     # We'll group only by ticker for now
    #     grouped_stocks = all_stocks.groupby("ticker", as_index=False, dropna=False).agg({"allocation": "sum"})

    #     # We might lose "name" here if there's a mismatch, so let's handle it carefully
    #     # If there's a single unique name per ticker, you can merge back or group by (ticker, name).
    #     # For simplicity, skip reattaching 'name' if it might be inconsistent.
        
    #     # Compute portfolio weights
    #     total_alloc = grouped_stocks["allocation"].sum()
    #     grouped_stocks["port_weight"] = grouped_stocks["allocation"] / total_alloc

    #     # Return final DataFrame with columns: [ticker, allocation, port_weight]
    #     # If you want 'name' columns, you'll merge or group by (ticker, name).
    #     return grouped_stocks
    def decompose_stocks(self):
        """
        Decompose the entire portfolio (stocks + ETFs) into stock-level allocations.
        Returns a single DataFrame with columns [ticker, name, allocation, port_weight].
        """
        port_etf, port_stock = self._get_portfolio_etf_stocks()
    
        # Decompose each ETF into underlying stocks
        decomposed_etf_list = self._decompose_etf_to_stocks(port_etf)
    
        # Rename 'PositionValue' -> 'allocation' for the direct stock portion
        port_stock = port_stock.rename(columns={"PositionValue": "allocation"})
        # Only keep relevant columns
        # 'name' is already in port_stock, so we keep it
        port_stock = port_stock[["ticker", "name", "allocation"]]
    
        # Combine the decomposed ETFs and the direct stocks
        all_stocks = pd.concat(decomposed_etf_list + [port_stock], ignore_index=True)
        
        # Group by both ticker and name to retain name information
        grouped_stocks = all_stocks.groupby(["ticker", "name"], as_index=False, dropna=False).agg({"allocation": "sum"})
    
        # Compute portfolio weights
        total_alloc = grouped_stocks["allocation"].sum()
        grouped_stocks["port_weight"] = grouped_stocks["allocation"] / total_alloc
    
        # Return final DataFrame with columns: [ticker, name, allocation, port_weight]
        return grouped_stocks
    
    def decompose_sectors(self):
        """
        Decompose stock-level allocations into sector-level allocations.
        Returns a single DataFrame with columns [gics_sector, allocation, port_weight].
        """
        # Step 1: Decompose to stock-level
        port_to_stocks = self.decompose_stocks()  # [ticker, allocation, port_weight]

        # Step 2: Get a list of all tickers in that stock-level DataFrame
        ticker_list = list(port_to_stocks["ticker"].unique())

        # Step 3: Pull stock info data from the market_data object
        stock_info_data = self.market_data.get_stock_info_data(ticker_list)
        if stock_info_data.empty or "sector" not in stock_info_data.columns:
            raise ValueError("Stock info data is empty or missing 'sector' column.")

        # Step 4: Standardize the sector name to a GICS sector
        stock_info_data["gics_sector"] = stock_info_data["sector"].apply(self.map_to_gics_sector)

        # Step 5: Merge stock_info_data into port_to_stocks
        # We only need ticker -> gics_sector
        stock_info_data_simplified = stock_info_data[["ticker", "gics_sector"]].drop_duplicates()
        port_stocks_sectors = pd.merge(port_to_stocks, stock_info_data_simplified, on="ticker", how="left")

        # Step 6: Group by sector to sum 'allocation'
        sector_df = port_stocks_sectors.groupby("gics_sector", as_index=False)["allocation"].sum()

        # Step 7: Compute sector-level weights
        total_alloc = sector_df["allocation"].sum()
        sector_df["port_weight"] = sector_df["allocation"] / total_alloc

        # Return final DataFrame [gics_sector, allocation, port_weight]
        return sector_df

    def map_to_gics_sector(self, label):
        """
        Maps Yahoo Finance sector labels to official GICS sector names.
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
        return mapping.get(label, "Unknown Unmapped")


    