#!/usr/bin/env python3
"""Date helpers used throughout the package."""

from __future__ import annotations

from datetime import date as date_type, datetime
from typing import Iterable


def month_end_series(index: Iterable[datetime | str]) -> "pd.DatetimeIndex":
    """Round each element of ``index`` to month end.

    Parameters
    ----------
    index : Iterable of datetimes or strings
        Values convertible to ``pandas.DatetimeIndex``.

    Returns
    -------
    pandas.DatetimeIndex
        Dates normalized to the last calendar day of the month.
    """
    import pandas as pd  # imported lazily for import safety

    idx = pd.DatetimeIndex(index)
    return idx.to_period("M").to_timestamp("M")


def to_yyyymm(dt: datetime | date_type | str) -> int:
    """Return ``YYYYMM`` integer representation of ``dt``."""
    if not isinstance(dt, (datetime, date_type)):
        dt = datetime.fromisoformat(str(dt))
    return dt.year * 100 + dt.month
