#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:04:16 2025

@author: arjundeshpande
"""
import pandas as pd

class PortfolioDecomposer:
    """
    Decomposes ETF and stock positions into sector allocations.
    """
    
    ### Consider updating the code to get the sector and holdings
    ### only for the etf tickeers required by the function/class
    def __init__(self, port, market_data):
        self.port = port.copy()
#        self.stock_universe = stock_universe
        self.market_data = market_data
        self.etf_sectors_dict  = market_data.get_etf_sectors() 
        self.etf_holdings_dict = market_data.get_etf_holdings()

    def decompose_stocks(self):
#    port_decomp_stocks(port, etf_holdings_dict):
        """
        To decompose the ETF positions into constituent stock positions
        
        inputs:
        port: dataframe of all ETFs and stocks that are held in the portfolio
        etf_holdings_dict: dictionary of dataframes with stock holdings for each ETF
        
        output:
        etf_to_stock: dataframe of all stock positions decomposed from ETF positions
        
        """
        #port = port.copy()
        
        #rename columns:
        self.port = self.port.rename(columns={"Symbol": "ticker", "Description": "name"})  # , "positionvalue": "allocation" 
        
        # creating a list of ETFs with available holdings data
        port_etf = pd.DataFrame(self.etf_holdings_dict.keys(), columns=['ticker'])
        
        # separate stock and etf 
        port_stock = self.port[~self.port.ticker.isin(port_etf["ticker"])]
        port_etf = pd.merge(port_etf,self.port, on='ticker')
    
        
        # Decomposing ETF into stocks:
        decomposed_etf = []
        for etf_ticker, position_value in zip(port_etf.ticker, port_etf.PositionValue):
            
            temp_df = self.etf_holdings_dict[etf_ticker].copy()  # pull holdings of the etf from etf_data
            temp_df["allocation"] = temp_df["weight"] * position_value # allocation of each stock
            
            decomposed_etf.append(temp_df) # append the stocks in each iteration
        
    
        port_stock = port_stock.rename(columns={"PositionValue": "allocation"})
        port_stock = port_stock[["ticker", "name", "allocation"]]  #remove unnecessary columns
        
        decomposed_etf.append(port_stock) # append stocks to decomposed etf 
        merged_df = pd.concat(decomposed_etf) #concat lists into a df
        # groupby on ticker and name, then sum allocations from all etfs
        merged_df = merged_df.groupby(['ticker'], as_index= False).agg({'allocation': 'sum'})
        ###
        #check whether groouping on ticker is better 
        ###
        #merged_df = merged_df.groupby(['ticker', 'name'], as_index= False).agg({'allocation': 'sum'})
        merged_df['port_weight'] = merged_df["allocation"]/merged_df["allocation"].sum()
        
        return merged_df
    
    # def decompose_sectors(self,stock_universe):
    #     """Convert ETF positions into sector allocations."""
    #     self.port = self.port.rename(columns={"Symbol": "ticker", "Description": "name"})

    #     port_etf = pd.DataFrame(self.etf_sectors_dict.keys(), columns=['ticker'])
    #     port_stock = self.port[~self.port["ticker"].isin(port_etf["ticker"])].copy()
    #     port_etf = pd.merge(port_etf, self.port, on="ticker").copy()

    #     decomposed_etf = []
    #     for etf_ticker, position_value in zip(port_etf["ticker"], port_etf["PositionValue"]):
    #         temp_df = self.etf_sectors_dict[etf_ticker].copy()
    #         temp_df["allocation"] = temp_df["weight"] * position_value
    #         decomposed_etf.append(temp_df)

    #     port_stock = port_stock.rename(columns={"PositionValue": "allocation"}).copy()
    #     port_stock = pd.merge(port_stock, stock_universe, on="ticker", how="inner").copy()
    #     port_stock = port_stock[["sector", "allocation"]]

    #     decomposed_etf.append(port_stock)
        
    #     merged_df = pd.concat(decomposed_etf, ignore_index=True)
    #     merged_df = merged_df.groupby(["sector"], as_index=False).agg({"allocation": "sum"})
    #     merged_df["port_weight"] = merged_df["allocation"] / merged_df["allocation"].sum()
    
    #     return merged_df
 
    
    def map_to_gics_sector(self,label):
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
       
    
    def decompose_sectors(self):
        """Convert ETF positions into sector allocations."""
        port_to_stocks = self.decompose_stocks().copy()
        port_to_stocks_tickerlist = [x for x in port_to_stocks.ticker] # filter the etf list from portfolio
        # get the stock data including sector names from YFinance
        stock_info_data = self.market_data.get_stock_info_data(port_to_stocks_tickerlist) 
        
        
        if stock_info_data.empty or "sector" not in stock_info_data.columns:
            raise ValueError(" ERROR: stock_info_data is empty or missing 'sector' column.")

        # standardize the sectors using the official GICS Sector names:
        stock_info_data["gics_sector"] = stock_info_data["sector"].apply(self.map_to_gics_sector)
        
        # Add the GICS sector to the existing port_to_stock:
        port_to_stocks = pd.merge(port_to_stocks, stock_info_data, how="inner", on=["ticker"])
        port_to_stocks = port_to_stocks[['ticker', 'name', 'allocation','gics_sector' ]]
        #port_to_stocks = port_to_stocks.rename(columns={"name_y": "name"})
        
        port_to_sectors = port_to_stocks.groupby(['gics_sector'], as_index= False).agg({
            'allocation': 'sum'}) #, 'port_weight': 'sum' 
        port_to_sectors['port_weight'] = port_to_sectors["allocation"]/port_to_sectors["allocation"].sum()

        return port_to_stocks,port_to_sectors
        
    
    ### NOT USED: RECONSIDER keeping it here:
    def decompose_benchmark_sectors(self):
        """Convert ETF positions into sector allocations."""
        port_to_stocks = self.decompose_stocks().copy()
        port_to_stocks_tickerlist = [x for x in port_to_stocks.ticker] # filter the etf list from portfolio
        # get the stock data including sector names from YFinance
        stock_info_data = self.market_data.get_stock_info_data(port_to_stocks_tickerlist) 
        
        
        if stock_info_data.empty or "sector" not in stock_info_data.columns:
            raise ValueError(" ERROR: stock_info_data is empty or missing 'sector' column.")

        # standardize the sectors using the official GICS Sector names:
        stock_info_data["gics_sector"] = stock_info_data["sector"].apply(self.map_to_gics_sector)
    
    # def decompose_sectors(self):
    #     """Convert ETF positions into sector allocations."""
    #     port_to_stocks = port_decomposer.decompose_stocks().copy()
    #     port_to_stocks_tickerlist = [x for x in port_to_stocks.ticker] # filter the etf list from portfolio
    #     # get the stock data including sector names from YFinance
    #     stock_info_data = market_data.get_stock_info_data(port_to_stocks_tickerlist) 
        
        
    #     if stock_info_data.empty or "sector" not in stock_info_data.columns:
    #         raise ValueError("❌ ERROR: stock_info_data is empty or missing 'sector' column.")
        
    #     # standardize the sectors using the official GICS Sector names:
    #     stock_info_data["gics_sector"] = stock_info_data["sector"].apply(map_to_gics_sector)
        
    #     # Add the GICS sector to the existing port_to_stock:
    #     port_to_stocks = pd.merge(port_to_stocks, stock_info_data, on=["ticker"])  #, "name"
    #     port_to_stocks = port_to_stocks[['ticker', 'name_y', 'allocation', 'port_weight','gics_sector' ]]
    #     port_to_stocks = port_to_stocks.rename(columns={"name_y": "name"})
        
    #     port_to_sectors = port_to_stocks.groupby(['gics_sector'], as_index= False).agg({
    #         'allocation': 'sum'}) #, 'port_weight': 'sum' 
    #     port_to_sectors['port_weight'] = port_to_sectors["allocation"]/port_to_sectors["allocation"].sum()


    
    
    