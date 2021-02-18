from itertools import chain
import pandas as pd
import numpy as np
import datetime
import time
import os
from pathlib import Path
from alpha_vantage.timeseries import TimeSeries
import sys

benchmark_index = read_yfinance("GSPC")
benchmark_index = benchmark_index[
    (benchmark_index.index >= "2000") & (benchmark_index.index <= "2021")
]
benchmark_index = benchmark_index.resample("1M")["Close"].agg("last")
benchmark_index = benchmark_index.loc[[d.isoformat() for d in dates[:-1]]]
benchmark_index /= benchmark_index.iloc[0]

fig, ax = plt.subplots(figsize=(16, 9))
sns.lineplot(
    x=dates[1:-1],
    y=np.exp(np.cumsum(portfolio_performance)[:-2]) - 1,
    ax=ax,
    label="ANN Performance",
)
sns.lineplot(
    x=benchmark_index.index[1:],
    y=benchmark_index.values[1:] - 1,
    ax=ax,
    label="S&P 500 Performance",
)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.set_xlabel("Date")
ax.set_ylabel("Cumulative Percentage Return")
ax.set_title("Performance Comparison")
plt.show()