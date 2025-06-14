#!/usr/bin/env python3
"""Simple stock screener utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import pandas as pd


@dataclass
class Screener:
    """Screen stocks using fundamental metrics.

    Parameters
    ----------
    overview : pd.DataFrame
        DataFrame containing fundamental overview data. Expected columns
        include ``avgVolume``, ``marketCap``, ``PERatio``, ``EPS`` and
        ``PEGRatio``.
    other_data : dict[str, pd.DataFrame], optional
        Additional fundamental or price data to attach to the screener for
        future use.
    """

    overview: pd.DataFrame
    other_data: Dict[str, pd.DataFrame] = field(default_factory=dict)

    def screen(
        self,
        *,
        min_volume: float = 0,
        min_market_cap: float = 0,
        pe_bounds: Optional[Tuple[Optional[float], Optional[float]]] = None,
        eps_bounds: Optional[Tuple[Optional[float], Optional[float]]] = None,
        peg_bounds: Optional[Tuple[Optional[float], Optional[float]]] = None,
    ) -> pd.DataFrame:
        """Return ``overview`` rows that satisfy the given filters."""

        df = self.overview.copy()

        if "avgVolume" in df.columns:
            df = df[df["avgVolume"] > min_volume]
        if "marketCap" in df.columns:
            df = df[df["marketCap"] > min_market_cap]

        if pe_bounds and "PERatio" in df.columns:
            low, high = pe_bounds
            if low is not None:
                df = df[df["PERatio"] >= low]
            if high is not None:
                df = df[df["PERatio"] <= high]

        if eps_bounds and "EPS" in df.columns:
            low, high = eps_bounds
            if low is not None:
                df = df[df["EPS"] >= low]
            if high is not None:
                df = df[df["EPS"] <= high]

        if peg_bounds and "PEGRatio" in df.columns:
            low, high = peg_bounds
            if low is not None:
                df = df[df["PEGRatio"] >= low]
            if high is not None:
                df = df[df["PEGRatio"] <= high]

        return df

    # ------------------------------------------------------------------
    # placeholder methods for future expansion
    # ------------------------------------------------------------------
    def add_data(self, name: str, df: pd.DataFrame) -> None:
        """Attach an additional DataFrame under ``name``."""
        self.other_data[name] = df

    def custom_factor(self) -> pd.Series:
        """Compute a custom factor. (Example placeholder method)"""
        return pd.Series(dtype=float)

    def rank_universe(self) -> pd.DataFrame:
        """Rank stocks on multiple metrics. (Example placeholder method)"""
        return self.overview

