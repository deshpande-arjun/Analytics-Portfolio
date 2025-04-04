#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 14:56:38 2025

@author: arjundeshpande
"""

import os

# Detect system type
system = 'windows' if os.name == 'nt' else 'mac'

if system=='mac':
    Base_dir = '/Users/arjundeshpande/Library/Mobile Documents/com~apple~CloudDocs/ETF decomposition/Portfolio-Analytics'
else:
    Base_dir = 'C:\ArjunDesktop\iCloudDrive\ETF decomposition\Portfolio-Analytics'

#os.chdir(Base_dir)

Data_dir = os.path.join(Base_dir, "data")

Class_dir = os.path.join(Base_dir, "classes")

Script_dir = os.path.join(Base_dir, "scripts")

# Define file paths
Portfolio_file = os.path.join(Data_dir, "CurrentPositions_dummy.csv")
Etf_data_file = os.path.join(Data_dir, "etf_metadata.json")
Stocksdb_file = os.path.join(Data_dir, "stocks.db")

# API Key (if needed)
AV_api_key="KZDZF6D34D3E50IG"

#os.chdir(Script_dir)

