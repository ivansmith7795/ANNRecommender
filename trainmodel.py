from itertools import chain
import pandas as pd
import numpy as np
import datetime
import time
import os
from pathlib import Path
from alpha_vantage.timeseries import TimeSeries
import sys

FEATURE_COLUMNS = [
    "first_half_log_return_mean",
    "first_half_log_return_std",
    "second_half_log_return_mean",
    "second_half_log_return_std",
    "first_second_half_log_return_diff",
    "log_return_mean",
    "log_return_std",
    "log_return_min",
    "log_return_max",
    "month_log_return",
    "pct_bull",
]
NUM_FEATURES = len(FEATURE_COLUMNS)  # we have 11 features in total

# prepare our list of surviving constituents by the end of every month
df_constituents_end_of_month = (
    df_constituents.resample("1M").agg("last").fillna(method="ffill")
)

# for storing index to ann items and for rebuilding ann
ticker_month_index = []
next_month_performances = []
ann_items = []

portfolio_performance = []
dates = []
for dt, tickers in tqdm(
    df_constituents_end_of_month.tickers.iteritems(),
    total=len(df_constituents_end_of_month),
):
    t = AnnoyIndex(NUM_FEATURES, metric="euclidean")  # euclidean distance
    for i, vector in enumerate(ann_items):
        t.add_item(i, vector)
    i = len(ann_items)
    surviving_tickers = []
    for ticker in tickers.split(","):
        if ticker not in dict_dfs:
            continue
        if dt.isoformat() not in dict_dfs[ticker].index:
            continue
        vector = dict_dfs[ticker].loc[dt.isoformat(), FEATURE_COLUMNS].values
        t.add_item(i, vector)
        ticker_month_index.append((ticker, dt.isoformat()))
        ann_items.append(vector)
        surviving_tickers.append(ticker)
        next_month_performances.append(
            dict_dfs[ticker].loc[dt.isoformat(), "next_month_log_return"]
        )
        i += 1
    if dt.year < 2000:
        continue # continue to build up our historic dataset
        
    t.build(200)  # build our 200 tree ANN!

    # a vector for storing the weighting of stocks for the next month
    weighting = np.zeros_like(surviving_tickers, dtype=float)
    # for recording our portfolio performance
    actual_performance = np.zeros_like(surviving_tickers, dtype=float)
    
    # the intuition here is to store the average performance of all the nearest neighbours
    # for each of the constituents, and then normalise the average performance to obtain
    # our final weighting vector. Note that we will not consider stocks whose average 
    # nearest neighbour performance is below zero.
    for i, ticker in enumerate(surviving_tickers):
        id = len(ann_items) - len(surviving_tickers) + i
        neighbours_ids = t.get_nns_by_item(id, 201)[1:]
        neighbours = [ticker_month_index[j] for j in neighbours_ids]
        recommendation = np.nanmean([next_month_performances[j] for j in neighbours_ids])
        if recommendation > 0:
            weighting[i] = recommendation
            actual_performance[i] = next_month_performances[id]

    # if all the weightings are zero, cash is king!
    if all(weighting == 0):
        portfolio_performance.append(0)
    else:
        weighting /= weighting.sum()
        weighting[np.isnan(weighting)] = 0
        actual_performance[np.isnan(actual_performance)] = 0
        portfolio_performance.append(
            np.log((weighting * np.exp(actual_performance)).sum())
        )
    dates.append(dt)