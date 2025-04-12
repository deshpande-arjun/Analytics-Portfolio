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
Data_dir = os.path.join(Parent_dir, "Market data")

# Folder that holds classes (within Analytics-Portfolio)
Class_dir = os.path.join(Base_dir, "classes")

# Define specific file paths
Portfolio_file = os.path.join(Data_dir, "CurrentPositions_dummy.csv")
Etf_data_file = os.path.join(Data_dir, "etf_metadata.json")
Stocksdb_file = os.path.join(Data_dir, "stocks.db")

# API Key (if needed)
AV_api_key="KZDZF6D34D3E50IG"

# import os

# # Detect system type
# system = 'windows' if os.name == 'nt' else 'mac'

# if system=='mac':
#     Base_dir = '/Users/arjundeshpande/Library/Mobile Documents/com~apple~CloudDocs/ETF decomposition/Analytics-Portfolio'
# else:
#     Base_dir = 'C:\ArjunDesktop\iCloudDrive\ETF decomposition\Analytics-Portfolio'

# #os.chdir(Base_dir)

# Data_dir = os.path.join(Base_dir, "data")

# Class_dir = os.path.join(Base_dir, "classes")

# Script_dir = os.path.join(Base_dir, "scripts")

# # Define file paths
# Portfolio_file = os.path.join(Data_dir, "CurrentPositions_dummy.csv")
# Etf_data_file = os.path.join(Data_dir, "etf_metadata.json")
# Stocksdb_file = os.path.join(Data_dir, "stocks.db")



# #os.chdir(Script_dir)

