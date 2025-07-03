#!/usr/bin/env python3
"""Run K-Means clustering on stock ratios and store results."""

from __future__ import annotations

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from dateutil.relativedelta import relativedelta

from classes import FeatureEngineer, Screener, PortfolioCalculations
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

# ---------------------------------------------------------------------
# database connection and data loading
# ---------------------------------------------------------------------
engine = create_engine(CONFIG["DB_URL"], pool_pre_ping=True)
price = pd.read_sql_table(CONFIG["TBL_PRICE"], engine)
overview = pd.read_sql_table(CONFIG["TBL_OVERVIEW"], engine)
_ = pd.read_sql_table(CONFIG["TBL_FUNDAMENTALS"], engine)  # placeholder

screener = Screener(overview)
fe = FeatureEngineer(None)
calc = PortfolioCalculations()

price_dates = month_end_series(price["date"])
min_date = price_dates.min() + relativedelta(months=18)
max_date = price_dates.max() - relativedelta(months=CONFIG["PERF_MONTHS"])

start = os.getenv("START_DATE")
end = os.getenv("END_DATE")
if start:
    min_date = pd.to_datetime(start)
if end:
    max_date = pd.to_datetime(end)

iter_dates = pd.date_range(min_date, max_date, freq="M")

for as_of in iter_dates:
    lookback_start = as_of - relativedelta(months=CONFIG["LOOKBACK_MONTHS"])
    _window = price[(price["date"] > lookback_start) & (price["date"] <= as_of)]

    ratio_df = fe.add_price_based_ratios(overview, CONFIG["RATIO_KEYS"])
    ratio_df = fe.winsorize(ratio_df.dropna(), CONFIG["RATIO_KEYS"])

    screened = screener.screen(
        min_volume=CONFIG["SCREEN_PARAMS"]["min_avg_vol"],
        min_market_cap=CONFIG["SCREEN_PARAMS"]["min_mktcap"],
        pe_bounds=CONFIG["SCREEN_PARAMS"]["pe_range"],
    )
    df_screen = ratio_df.loc[screened.index.intersection(ratio_df.index)]

    standardized = (df_screen - df_screen.mean()) / df_screen.std(ddof=0)
    labels = KMeans(n_clusters=CONFIG["CLUSTERS"], random_state=42).fit_predict(standardized)

    fwd_ret = calc.compute_forward_returns(price, as_of, CONFIG["GAP_MONTHS"], CONFIG["PERF_MONTHS"])

    snapshot = pd.DataFrame({
        "snapshot_date": as_of,
        "ticker": df_screen.index,
        "cluster_id": labels,
        "fwd_ret": fwd_ret.reindex(df_screen.index).values,
        "ratios": df_screen.loc[df_screen.index].to_dict("records"),
    })

    with engine.begin() as conn:
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {CONFIG['OUTPUT_TBL']} (
            snapshot_date DATE,
            ticker TEXT,
            cluster_id INTEGER,
            fwd_ret DOUBLE PRECISION,
            ratios JSONB
        )"""
        )
    snapshot.to_sql(CONFIG["OUTPUT_TBL"], engine, if_exists="append", index=False)
    logger.info(f"{as_of:%Y-%m-%d}: stored {len(snapshot)} rows")
