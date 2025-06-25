#!/usr/bin/env python3
"""Cluster stocks on fundamental ratios and track forward returns."""

from __future__ import annotations

import os
import logging
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sqlalchemy import create_engine

from classes.data_accessor import DataAccessor
from classes.feature_engineer import FeatureEngineer
from classes.screener import Screener
from utils.logging_utils import get_logger
from utils.date_utils import month_end_series


CONFIG = {
    "DB_URL": os.getenv("PG_CONN"),
    "TBL_PRICE": "price",
    "TBL_OVERVIEW": "overview",
    "TBL_FUNDAMENTALS": "fundamentals",
    "RATIO_KEYS": ["PERatio", "PriceToBookRatio", "EVToEBITDA"],
    "CLUSTERS": 4,
    "LOOKBACK_MONTHS": 12,
    "GAP_MONTHS": 3,
    "PERF_MONTHS": 12,
    "SCREEN_PARAMS": {
        "min_avg_vol": 1_000_000,
        "min_mktcap": 1_000_000_000,
        "pe_range": (0, 40),
    },
    "OUTPUT_TBL": "cluster_performance",
}

logger = get_logger(__name__)


def postgres_engine():
    """Return a SQLAlchemy engine for Postgres."""
    url = CONFIG["DB_URL"]
    if not url:
        raise ValueError("PG_CONN environment variable not set")
    return create_engine(url, pool_pre_ping=True)


def load_data(engine):
    """Load required tables into DataFrames."""
    accessor = DataAccessor("")
    accessor.engine = engine
    price = pd.read_sql_table(CONFIG["TBL_PRICE"], engine)
    overview = pd.read_sql_table(CONFIG["TBL_OVERVIEW"], engine)
    fundamentals = pd.read_sql_table(CONFIG["TBL_FUNDAMENTALS"], engine)
    return {"price": price, "overview": overview, "fundamentals": fundamentals}


def _winsorize(df: pd.DataFrame, cols: list[str], p: float = 0.01) -> pd.DataFrame:
    clipped = df.copy()
    for c in cols:
        lower = clipped[c].quantile(p)
        upper = clipped[c].quantile(1 - p)
        clipped[c] = clipped[c].clip(lower, upper)
    return clipped


def build_dataset(dfdict: dict[str, pd.DataFrame], as_of_date: pd.Timestamp) -> pd.DataFrame:
    lookback_start = as_of_date - relativedelta(months=CONFIG["LOOKBACK_MONTHS"])
    price = dfdict["price"]
    _ = price[(price["date"] > lookback_start) & (price["date"] <= as_of_date)]  # window reserved for future use

    fe = FeatureEngineer(DataAccessor(""))
    fe.accessor = None  # placeholder - FeatureEngineer may expect an accessor
    overview = dfdict["overview"]
    ratio_df = fe.add_price_based_ratios(overview, CONFIG["RATIO_KEYS"])

    ratio_df = _winsorize(ratio_df.dropna(), CONFIG["RATIO_KEYS"])
    return ratio_df


def run_kmeans(df_ratios: pd.DataFrame) -> pd.Series:
    standardized = (df_ratios - df_ratios.mean()) / df_ratios.std(ddof=0)
    model = KMeans(n_clusters=CONFIG["CLUSTERS"], random_state=42)
    labels = model.fit_predict(standardized)
    return pd.Series(labels, index=df_ratios.index)


def compute_forward_returns(df_price: pd.DataFrame, as_of_date: pd.Timestamp) -> pd.Series:
    start = as_of_date + relativedelta(months=CONFIG["GAP_MONTHS"])
    end = start + relativedelta(months=CONFIG["PERF_MONTHS"])

    df = df_price.set_index("date")
    returns = {}
    for ticker, grp in df.groupby("ticker"):
        try:
            start_price = grp.loc[start, "close"]
            end_price = grp.loc[end, "close"]
            returns[ticker] = (end_price / start_price) - 1
        except KeyError:
            returns[ticker] = np.nan
    return pd.Series(returns)


def store_snapshot(engine, snapshot_df: pd.DataFrame) -> None:
    tbl = CONFIG["OUTPUT_TBL"]
    with engine.begin() as conn:
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {tbl} (
            snapshot_date DATE,
            ticker TEXT,
            cluster_id INTEGER,
            fwd_ret DOUBLE PRECISION,
            ratios JSONB
        )"""
        )
    snapshot_df.to_sql(tbl, engine, if_exists="append", index=False)


def main(start: str | None = None, end: str | None = None) -> None:
    engine = postgres_engine()
    data = load_data(engine)
    screener = Screener(data["overview"])

    price_dates = month_end_series(data["price"]["date"])
    min_date = price_dates.min() + relativedelta(months=18)
    max_date = price_dates.max() - relativedelta(months=CONFIG["PERF_MONTHS"])

    if start:
        min_date = pd.to_datetime(start) + relativedelta(months=0)
    if end:
        max_date = pd.to_datetime(end)

    iter_dates = pd.date_range(min_date, max_date, freq="M")

    for as_of in iter_dates:
        df_ratios = build_dataset(data, as_of)
        df_screen = screener.screen(
            min_volume=CONFIG["SCREEN_PARAMS"]["min_avg_vol"],
            min_market_cap=CONFIG["SCREEN_PARAMS"]["min_mktcap"],
            pe_bounds=CONFIG["SCREEN_PARAMS"]["pe_range"],
        )
        df_screen = df_ratios.loc[df_screen.index.intersection(df_ratios.index)]
        labels = run_kmeans(df_screen)
        fwd_ret = compute_forward_returns(data["price"], as_of)
        snapshot = pd.DataFrame({
            "snapshot_date": as_of,
            "ticker": labels.index,
            "cluster_id": labels.values,
            "fwd_ret": fwd_ret.reindex(labels.index).values,
            "ratios": df_screen.loc[labels.index].to_dict("records"),
        })
        store_snapshot(engine, snapshot)
        logger.info(f"{as_of:%Y-%m-%d}: stored {len(snapshot)} rows")

    # --- TODO: plug in alternative clustering model here ---
    # --- TODO: add risk-adjusted return metrics ---


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="YYYY-MM override")
    parser.add_argument("--end", help="YYYY-MM override")
    args = parser.parse_args()
    main(args.start, args.end)
