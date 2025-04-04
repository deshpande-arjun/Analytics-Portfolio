#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:01:54 2025

@author: arjundeshpande
"""

import sqlite3
import requests
import yfinance as yf
import json
import os
from datetime import datetime
import pandas as pd
import time

class MarketData:
    """
    Manages database creation, updates, and refresh policies for stock universe & ETF data.
    Also fetches data from Alpha Vantage, Yahoo Finance, and other APIs.
    """

    def __init__(self, db_name="stocks.db", meta_file="etf_metadata.json", av_api_key="KZDZF6D34D3E50IG"):
        self.db_name = db_name
        self.meta_file = meta_file
        self.api_key = av_api_key
        self.meta_data = self.load_meta()

    # 🔹 Load ETF Metadata from File
    def load_meta(self):
        """Load ETF metadata from a file."""
        if os.path.exists(self.meta_file):
            with open(self.meta_file, "r") as file:
                return json.load(file)
        return {}

    # 🔹 Save ETF Metadata to File
    def save_meta(self):
        """Save ETF metadata to a file."""
        with open(self.meta_file, "w") as file:
            json.dump(self.meta_data, file, indent=4)

    # 🔹 Fetch Stock Info from Yahoo Finance
    def _fetch_yfinance_stock_info(self, ticker):
        """Fetch stock details from Yahoo Finance."""
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            "name": info.get("longName", "N/A"),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "market_cap": info.get("marketCap", 0),
            "currency": info.get("currency", "N/A"),
            "exchange": info.get("exchange", "N/A"),
            "dividend_yield": info.get("dividendYield", 0),
            "pe_ratio": info.get("trailingPE", None),
            "beta": info.get("beta", None),
            "high_52_week": info.get("fiftyTwoWeekHigh", None),
            "low_52_week": info.get("fiftyTwoWeekLow", None),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }



    # 🔹 Fetch ETF Data from Alpha Vantage
    def _fetch_alphavantage_etf_data(self, etf_ticker):
        """Retrieve ETF holdings from Alpha Vantage."""
        url = "https://www.alphavantage.co/query"
        params = {"function": "ETF_PROFILE", "symbol": etf_ticker, "apikey": self.api_key}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            return response.json()
        return None
    
    # 🔹 Store Stock Universe Database
    def store_stock_info(self, tickers, refresh_days=30):
        """Create or update the stock universe database."""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS stock_universe (
            ticker TEXT PRIMARY KEY, name TEXT, sector TEXT, last_updated TEXT)''')

        cursor.execute("SELECT ticker, last_updated FROM stock_universe")
        existing_data = {row[0]: row[1] for row in cursor.fetchall()}
        outdated_tickers = [t for t, last_updated in existing_data.items() 
                            if (datetime.now() - datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")).days >= refresh_days]

        tickers_to_fetch = [t for t in tickers if t not in existing_data or t in outdated_tickers]

        for ticker in tickers_to_fetch:
            stock_info_data = self._fetch_yfinance_stock_info(ticker)
            cursor.execute('''INSERT INTO stock_universe (ticker, name, sector, last_updated)
                              VALUES (?, ?, ?, ?) ON CONFLICT(ticker) DO UPDATE SET 
                              name=excluded.name, sector=excluded.sector, last_updated=excluded.last_updated''',
                           (ticker, stock_info_data["name"], stock_info_data["sector"], stock_info_data["last_updated"]))

        conn.commit()
        conn.close()

    # 🔹 Fetch Stock Prices from Yahoo Finance
    def _fetch_yfinance_stock_prices(self, ticker_list, period="5y",start_date="2020-01-01",end_date="2023-12-01"):
        """stock prices from Yahoo Finance."""
        """
        Fetch historical price data from yfinance for the given tickers.
        You can either specify a period (e.g., "5y") or provide a start and end date.
        It then computes daily returns and calculates the covariance matrix.
    
        Parameters:
            tickers (list): List of ticker symbols.
            period (str): Time period to download data for (default "5y").
                          Used if start and end are not provided.
            start (str): Start date in 'YYYY-MM-DD' format (optional).
            end (str): End date in 'YYYY-MM-DD' format (optional).

        Returns:
            prices_data (pd.DataFrame): DataFrame of daily prices.
        """
        try:
            if start_date is not None and end_date is not None:
                prices_data = yf.download(ticker_list, start=start_date, end=end_date,threads=True)#['Close']
            else:
                prices_data = yf.download(ticker_list, period=period,threads=True)#['Close']
            
            return prices_data
        except Exception as e:
            return print(f"error fetching stock prices for ticker: {e}")
    
    def _convert_yf_stockprices_to_long(self, df):
        """Convert Multi-Index DataFrame to long format for SQLite storage."""
        
        # Step 1: Flatten MultiIndex column names (handles single & multi-level columns)
        df.columns = df.columns.map(lambda x: "_".join(x) if isinstance(x, tuple) else x)
    
        # Step 2: Convert to long format
        df_long = df.reset_index().melt(id_vars=["Date"], var_name="metric_ticker", value_name="value")
    
        # Step 3: Split 'metric' and 'ticker'
        df_long[["metric", "ticker"]] = df_long["metric_ticker"].str.rsplit("_", n=1, expand=True)
        df_long = df_long.drop(columns=["metric_ticker"])
    
        # Step 4: Pivot table to reshape into a structured format
        df_final = df_long.pivot_table(index=["Date", "ticker"], columns="metric", values="value").reset_index()
    
        # Step 5: Ensure all expected columns exist (fill missing ones with NaN)
        expected_columns = ["Date", "ticker", "Open", "High", "Low", "Close", "Volume"]
        df_final = df_final.reindex(columns=expected_columns)
    
        # Step 6: Rename columns to lowercase
        df_final.columns = df_final.columns.str.lower()
        
        return df_final
        

    def store_stock_prices(self, ticker_list, period="5y", start_date=None, end_date=None, chunk_size=50):
        """
        Fetch and store stock prices in an SQLite database in chunks.
        Ensures no duplicate (date, ticker) entries are inserted.
    
        Parameters:
        - ticker_list (list): List of stock tickers to fetch prices for.
        - period (str): Time period (default: "5y").
        - start_date (str): Start date for fetching data.
        - end_date (str): End date for fetching data.
        - chunk_size (int): Number of tickers to fetch per batch to avoid API limits.
        """
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
    
        # ✅ Step 1: Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_prices4 (
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (date, ticker)
            )
        ''')
        conn.commit()
    
        # ✅ Step 2: Fetch existing (date, ticker) pairs from the database
        existing_dates_tickers = set(cursor.execute("SELECT date, ticker FROM stock_prices4").fetchall())
    
        # ✅ Step 3: Fetch stock prices in chunks
        for i in range(0, len(ticker_list), chunk_size):
            chunk = ticker_list[i:i + chunk_size]  # Get a batch of tickers
            print(f" Fetching stock prices for batch {i//chunk_size + 1}/{(len(ticker_list) // chunk_size) + 1}...")
    
            # ✅ Step 4: Identify tickers needing updates
            tickers_to_fetch = []
            for ticker in chunk:
                query = "SELECT DISTINCT date FROM stock_prices4 WHERE ticker = ?"
                cursor.execute(query, (ticker,))
                existing_dates = {row[0] for row in cursor.fetchall()}  # Fetch all existing dates for this ticker
    
                # Generate required date range
                if start_date and end_date:
                    all_dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime('%Y-%m-%d').tolist()
                else:
                    all_dates = []  # If no specific dates are provided, skip date filtering
                
                # Find missing dates
                missing_dates = set(all_dates) - existing_dates
                if not all_dates or missing_dates:  # Fetch if either: (a) no date filter OR (b) missing dates exist
                    tickers_to_fetch.append(ticker)
    
            if not tickers_to_fetch:
                print(f" Batch {i//chunk_size + 1}: No new data needed. Skipping API call.")
                continue  # Skip API call if no updates required
    
            # ✅ Step 5: Fetch data from Yahoo Finance
            df = self._fetch_yfinance_stock_prices(tickers_to_fetch, period, start_date, end_date)
            if df is None or df.empty:
                print(f" No data returned for batch {i//chunk_size + 1}. Skipping...")
                continue
    
            # ✅ Step 6: Convert data to long format
            df_sql = self._convert_yf_stockprices_to_long(df)
    
            # ✅ Step 7: Ensure no duplicate (date, ticker) pairs before inserting
            df_sql = df_sql[~df_sql.set_index(["date", "ticker"]).index.isin(existing_dates_tickers)]
            
            # ✅ Step 8: Insert only non-duplicate records into database
            if not df_sql.empty:
                try:
                    df_sql.to_sql("stock_prices4", conn, if_exists="append", index=False)
                    print(f" Stored {len(df_sql)} new rows in the database.")
                except sqlite3.IntegrityError as e:
                    print(f"Error: Duplicate entries found. Skipping batch {i//chunk_size + 1}. {e}")
    
            # ✅ Step 9: Sleep to avoid rate limits
            time.sleep(2)
    
        conn.close()
        print(" Stock price data storage complete!")




    
    
    # def store_stock_prices(self, tickers, refresh_days=30):
    #     """Create or update the stock universe database."""
    #     conn = sqlite3.connect(self.db_name)
    #     cursor = conn.cursor()

    #     cursor.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
    #         ticker TEXT PRIMARY KEY, name TEXT, sector TEXT, last_updated TEXT)''')

    #     cursor.execute("SELECT ticker, last_updated FROM stock_prices") ### NEED TO UPDATE###
    #     existing_data = {row[0]: row[1] for row in cursor.fetchall()}
    #     outdated_tickers = [t for t, last_updated in existing_data.items() 
    #                         if (datetime.now() - datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")).days >= refresh_days]

    #     tickers_to_fetch = [t for t in tickers if t not in existing_data or t in outdated_tickers]

    #     for ticker in tickers_to_fetch:
    #         stock_info_data = self._fetch_yfinance_stock_info(ticker)
    #         cursor.execute('''INSERT INTO stock_universe (ticker, name, sector, last_updated)
    #                           VALUES (?, ?, ?, ?) ON CONFLICT(ticker) DO UPDATE SET 
    #                           name=excluded.name, sector=excluded.sector, last_updated=excluded.last_updated''',
    #                        (ticker, stock_info_data["name"], stock_info_data["sector"], stock_info_data["last_updated"]))

    #     conn.commit()
    #     conn.close()



    # 🔹 Store ETF Metadata in File
    def store_etf_data(self, etf_list):
        """Fetch multiple ETF data from Alpha Vantage and store ETF metadata."""
        for etf_ticker in etf_list:
            if etf_ticker in self.meta_data:
                print(f"{etf_ticker} already exists in metadata. Skipping API call.")
                continue  

            etf_data = self._fetch_alphavantage_etf_data(etf_ticker)
            if etf_data:
                self.meta_data[etf_ticker] = etf_data  
                print(f"Stored metadata for {etf_ticker}.")
            else:
                print(f"Failed to retrieve data for {etf_ticker}.")

        self.save_meta()
        
    def _process_etf_data(self, parameter):
        """Process ETF meta data from Alpha Vantage and return sector and 
        stock holdings. """
        etf_metadata_dict = self.get_etf_metadata()
        etf_tickers = list(etf_metadata_dict.keys())
        etf_dict = {}
        for ticker in etf_tickers:
            data = etf_metadata_dict[ticker].get(parameter, [])
            
            df = pd.DataFrame(data)
            # Convert numeric columns to float
            #df = df.apply(pd.to_numeric, errors='ignore')
            # updating the code with for loop as errors=ignore will not work in future pandas packages
            for col in df.select_dtypes(include=['object', 'string']):  
                try:
                    df[col] = pd.to_numeric(df[col])  # Attempt conversion
                except ValueError:
                    pass  # Ignore errors manually

            if not df.empty:
                etf_dict[ticker] = df
            else:
                print(parameter," data not available for ",ticker)
        
        return etf_dict
    
    def get_etf_holdings(self, parameter = "holdings"):
        etf_dict = self._process_etf_data(parameter)
        etf_dict = {
            key: df.rename(columns={"symbol": "ticker", "description": "name"})
            for key, df in etf_dict.items()
            }

        return etf_dict
    
    
    def get_etf_sectors(self, parameter = "sectors"):
        return self._process_etf_data(parameter)
    
    
    def get_etf_metadata(self):
        """get etf meta data from meta data"""
        etf_metadata = self.meta_data
        
        etf_list = list(etf_metadata.keys())
        etf_metadata_dict = {}
        for ticker in etf_list:
            df = etf_metadata[ticker]
            
            if not df:
                print("etf not in the data base",{ticker})
            else:
                etf_metadata_dict[ticker] =df
        
        return etf_metadata_dict
    
    def get_stock_info_data(self, stock_list):
        """get stock data from database"""
        conn = sqlite3.connect(self.db_name)  # Connect to the database
        limit_sql_variables = 900 # Less than 999 to stay safe
        temp_data = []
        for i in range(0, len(stock_list), limit_sql_variables):
            one_lot = stock_list[i:i+limit_sql_variables-1]
            # Convert stock_list into a format for SQL IN clause
            placeholders = ','.join(['?'] * len(one_lot))
            query = f"SELECT * FROM stock_universe WHERE ticker IN ({placeholders})"
            # Fetch data as a Pandas DataFrame
            df = pd.read_sql_query(query, conn, params=one_lot)
            temp_data.append(df)
        conn.close()  # Close the database connection
        
        stock_info_data =pd.concat(temp_data, ignore_index=True)
        if stock_info_data.empty:
            print("error in pulling stock data")
        else:
            return stock_info_data

    def get_stock_prices_data(self, stock_list, start_date=None, end_date=None):
        """Fetch historical stock prices from the database."""
        
        conn = sqlite3.connect(self.db_name)  # Connect to the database
        limit_sql_variables = 900  # Less than 999 to stay safe
        temp_data = []
    
        for i in range(0, len(stock_list), limit_sql_variables):
            one_lot = stock_list[i:i + limit_sql_variables - 1]
            
            # Construct SQL query dynamically
            placeholders = ','.join(['?'] * len(one_lot))
            query = f"SELECT * FROM stock_prices3 WHERE ticker IN ({placeholders})"
    
            # Add date filtering if provided
            if start_date and end_date:
                query += " AND date BETWEEN ? AND ?"
                params = one_lot + [start_date, end_date]
            elif start_date:
                query += " AND date >= ?"
                params = one_lot + [start_date]
            elif end_date:
                query += " AND date <= ?"
                params = one_lot + [end_date]
            else:
                params = one_lot  # No date filtering
    
            # Fetch data as a Pandas DataFrame
            df = pd.read_sql_query(query, conn, params=params)
            temp_data.append(df)
    
        conn.close()  # Close the database connection
    
        # Combine all chunks into a single DataFrame
        stock_prices_data = pd.concat(temp_data, ignore_index=True)
    
        if stock_prices_data.empty:
            print("⚠️ No stock price data found for the given stock list & date range.")
            return None
        else:
            return stock_prices_data



# =============================================================================
#     def get_etf_metadata(self, etf_list):
#         """get etf meta data from data base"""
#         conn = sqlite3.connect(self.meta_data)
#         #cursor = conn.cursor() # not used here, can be used instead of pd.read_sql
# 
#         etf_metadata_dict = {}
#         query = "SELECT * FROM meta_data WHERE ticker=?" #similar to sql query ? will be replace by a variable ticker
#         
#         for ticker in etf_list:
#             # Query database for the ETF's metadata
#             df = pd.read_sql(query,conn, params=(ticker,))
#             # Store the result in dictionary (only if data exists)
#             if not df.empty:
#                 etf_metadata[ticker] = df
# 
#         conn.close()
#         return etf_metadata_dict
# =============================================================================
# =============================================================================
#         stock_universe = self.db_name
#         stock_info_data = stock_universe[stock_universe["ticker"].isin(stock_list)]
#         #for ticker in stock_list:
#         return stock_info_data
#             
# =============================================================================
