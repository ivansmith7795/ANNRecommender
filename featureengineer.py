from itertools import chain
import pandas as pd
import numpy as np
import datetime
import time
import os
from pathlib import Path
from alpha_vantage.timeseries import TimeSeries
import sys

df_constituents = pd.read_csv(
    "https://raw.githubusercontent.com/fja05680/sp500/master/S%26P%20500%20Historical%20Components%20%26%20Changes(01-21-2021).csv",
    parse_dates=True,
    index_col=0,
).sort_index(ascending=False)

ALPHA_VANTAGE_DIR_PATH = Path("alphadata").absolute()

def generate_monthly_stats(df):
    #print(df)
    log_return = df["Close"].apply(np.log).diff()
    half_way_point = len(df) // 2

    return {
        "Open": df["Open"].iloc[0],
        "High": df["High"].max(),
        "Low": df["Low"].min(),
        "Close": df["Close"].iloc[-1],
        "Volume": df["Volume"].sum(),
        "first_half_log_return_mean": log_return.iloc[:half_way_point].mean(),
        "first_half_log_return_std": log_return.iloc[:half_way_point].std(),
        "second_half_log_return_mean": log_return.iloc[half_way_point:].mean(),
        "second_half_log_return_std": log_return.iloc[half_way_point:].std(),
        "first_second_half_log_return_diff": (
            log_return.iloc[half_way_point:].sum()
            - log_return.iloc[:half_way_point].sum()
        ),
        "log_return_mean": log_return.mean(),
        "log_return_std": log_return.std(),
        "log_return_min": log_return.min(),
        "log_return_max": log_return.max(),
        "month_log_return": np.log(df["Close"].iloc[-1] / df["Open"].iloc[0]),
        "pct_bull": (log_return > 0).mean()
    }

tickers = set(",".join(df_constituents.tickers.values).split(","))
slippage = .005 # 0.5% slippage per trade
dict_dfs = dict()
for t in tickers:
    # this stock is not available on alpha vantage
    if not (ALPHA_VANTAGE_DIR_PATH / f"{t}.csv").is_file():
        continue
    temp = (
        pd.read_csv(ALPHA_VANTAGE_DIR_PATH / f"{t}.csv", index_col=0, parse_dates=True)
        .groupby(pd.Grouper(freq="1M"))
        .apply(generate_monthly_stats)
    )
    print(temp)
    temp["next_month_log_return"] = np.log(
        np.exp(temp["month_log_return"].shift(-1)) * (1 - slippage) / (1 + slippage)
    )
    dict_dfs[t] = temp