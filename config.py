#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 14:56:38 2025

@author: arjundeshpande
"""

import os

# Set base directory to the current location of config.py (inside Analytics-Portfolio)
Base_dir = os.path.dirname(os.path.abspath(__file__))

# Parent directory (one level above Analytics-Portfolio)
Parent_dir = os.path.abspath(os.path.join(Base_dir, os.pardir))

# Directory that holds the data (e.g., "Market data" is one level up)
Data_dir = os.path.join(Base_dir, "data")

# Folder that holds classes (within Analytics-Portfolio)
Class_dir = os.path.join(Base_dir, "classes")

# Define specific file paths
Portfolio_file = os.path.join(Data_dir, "CurrentPositions_dummy.csv")
Etf_data_file = os.path.join(Data_dir, "etf_metadata.json")
Stocksdb_file = os.path.join(Data_dir, "stocks.db")
AV_db_file = os.path.join(Data_dir, "av_data.db")


# API Key (if needed)
AV_api_key="53U4JBZUJNQX0EVO"
