#!/usr/bin/env python3
"""Backward compatibility wrapper for :class:`DataFetcher`."""

from __future__ import annotations

from .data_fetcher import DataFetcher

# ``AlphaVantageData`` used to be a separate class. It now simply aliases
# :class:`DataFetcher` so existing imports continue to work.
AlphaVantageData = DataFetcher
