#!/usr/bin/env python3
"""Example script to fetch and store ETF holdings."""

from typing import List

from config import AV_api_key, AV_db_file
from classes import DataFetcher, DatabaseAccessor


def main(symbols: List[str]) -> None:
    fetcher = DataFetcher(db_name=AV_db_file, api_key=AV_api_key)
    accessor = DatabaseAccessor(db_name=AV_db_file)

    for sym in symbols:
        data = fetcher.fetch_etf_profile(sym)
        if data:
            accessor.store_etf_holdings(sym, data)
            holdings = accessor.get_etf_holdings(sym)
            print(f"Holdings for {sym}:")
            print(holdings)
        else:
            print(f"Failed to fetch profile for {sym}")


if __name__ == "__main__":
    main(["SPY", "IWV"])
