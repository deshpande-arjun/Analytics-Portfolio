#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 01:44:01 2025

@author: arjundeshpande
"""

from setuptools import setup, find_packages

setup(
    name="portfolio_analytics",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "yfinance",
        "requests",
        "pandas",
        "numpy",      
    ],
    author="Arjun Deshpande",
    description="A Python package for portfolio analytics",
    long_description=open("README.md").read(),
    url="https://github.com/deshpande-arjun/portfolio-analytics",
)
