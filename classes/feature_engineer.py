#!/usr/bin/env python3
"""Feature engineering utilities for financial data."""

from __future__ import annotations

from typing import List

import pandas as pd
import numpy as np

from .database_accessor import DatabaseAccessor


class FeatureEngineer:
    """Compute financial ratios and statistical features."""

    def __init__(self, accessor: DatabaseAccessor) -> None:
        self.accessor = accessor

    # ------------------------------------------------------------------
    # financial ratios
    # ------------------------------------------------------------------
    def compute_financial_ratios(
        self,
        tickers: List[str],
        period: str = "annual",
    ) -> pd.DataFrame:
        """Return a DataFrame of common financial ratios."""
        data = self.accessor.get_fundamentals(tickers, period)
        income = data.get("income_statement", pd.DataFrame())
        balance = data.get("balance_sheet", pd.DataFrame())
        overview = data.get("overview", pd.DataFrame())

        if income.empty or balance.empty:
            return pd.DataFrame()

        df = income.merge(
            balance,
            on=["ticker", "fiscal_date_ending", "period"],
            suffixes=("_inc", "_bal"),
        )
        if not overview.empty:
            df = df.merge(overview, on="ticker", how="left")

        def _num(col: str) -> pd.Series:
            return pd.to_numeric(df.get(col), errors="coerce")

        df_ratio = pd.DataFrame({
            "ticker": df["ticker"],
            "fiscal_date_ending": df["fiscal_date_ending"],
        })

        current_assets = _num("totalCurrentAssets")
        current_liab = _num("totalCurrentLiabilities")
        inventory = _num("inventory")
        total_debt = _num("totalDebt")
        total_assets = _num("totalAssets")
        total_equity = _num("totalShareholderEquity")
        cogs = _num("costOfGoodsAndServicesSold")
        receivables = _num("currentNetReceivables")
        revenue = _num("totalRevenue")
        net_income = _num("netIncome")
        dividends = _num("dividendPerShare")
        price = _num("close") if "close" in df.columns else _num("price")
        eps = _num("eps") if "eps" in df.columns else _num("EPS")
        book_value_ps = _num("bookValue") if "bookValue" in df.columns else _num("bookValuePerShare")

        df_ratio["current_ratio"] = current_assets / current_liab
        df_ratio["quick_ratio"] = (current_assets - inventory) / current_liab
        df_ratio["debt_to_equity"] = total_debt / total_equity
        df_ratio["equity_multiplier"] = total_assets / total_equity
        df_ratio["inventory_turnover"] = cogs / inventory
        df_ratio["receivables_turnover"] = revenue / receivables
        df_ratio["asset_turnover"] = revenue / total_assets
        df_ratio["profit_margin"] = net_income / revenue
        df_ratio["return_on_assets"] = net_income / total_assets
        df_ratio["return_on_equity"] = net_income / total_equity

        df_ratio["price_to_earnings"] = price / eps
        df_ratio["price_to_book"] = price / book_value_ps
        df_ratio["earnings_yield"] = eps / price
        df_ratio["dividend_yield"] = dividends / price
        if total_equity.notna().any():
            market_equity = _num("marketCapitalization")
            df_ratio["market_to_book"] = market_equity / total_equity
        return df_ratio

    # ------------------------------------------------------------------
    # statistical features
    # ------------------------------------------------------------------
    def rolling_volatility(
        self, prices: pd.DataFrame, window: int = 20, price_col: str = "close"
    ) -> pd.DataFrame:
        """Calculate rolling volatility of returns."""
        df = prices.copy()
        df["return"] = df.groupby("ticker")[price_col].pct_change()
        df["volatility"] = (
            df.groupby("ticker")["return"].rolling(window).std().reset_index(level=0, drop=True)
        )
        return df.drop(columns="return")

    def cross_sectional_zscore(self, df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Compute z-scores across the universe for ``cols`` by date."""
        zdf = df.copy()
        for c in cols:
            zdf[c] = (zdf[c] - zdf[c].mean()) / zdf[c].std(ddof=0)
        return zdf

    def min_max_scale(self, df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Normalize selected columns to the 0-1 range."""
        scaled = df.copy()
        for c in cols:
            col = scaled[c]
            scaled[c] = (col - col.min()) / (col.max() - col.min())
        return scaled

    def add_price_based_ratios(self, overview: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        """Return ``overview`` DataFrame with selected ratio columns numeric."""
        df = overview.copy()
        df = df.set_index("ticker") if "ticker" in df.columns else df
        out = pd.DataFrame(index=df.index)
        for k in keys:
            out[k] = pd.to_numeric(df.get(k), errors="coerce")
        return out
