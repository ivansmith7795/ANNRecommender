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
SECRET = "RGBKTDQQ7C5071TJ"

def get_alpha_vantage(key, ticker, save_dir):
    """Given a key to Alpha Vantage and a valid ticker, this function will
    query alpha vantage and save the dataset into a csv in a predefined
    directory using ticker as the filename.
    """
    ts = TimeSeries(key=key, output_format="pandas", indexing_type="date")

    if isinstance(save_dir, str):
        save_dir = Path(save_dir)

    try:
        data, meta_data = ts.get_daily_adjusted(symbol=ticker, outputsize="full")

        # adjusting the prices
        data = data.rename(
            columns={
                "1. open": "Open",
                "2. high": "High",
                "3. low": "Low",
                "4. close": "Close",
                "5. adjusted close": "Adjusted Close",
                "6. volume": "Volume",
                "7. dividend amount": "Dividend",
                "8. split coefficient": "Split Coefficient",
            }
        )
        data["Unadjusted Open"] = data["Open"]
        data["Open"] = data["Close"] * data["Adjusted Close"] / data["Open"]
        data["High"] = data["High"] * data["Open"] / data["Unadjusted Open"]
        data["Low"] = data["Low"] * data["Open"] / data["Unadjusted Open"]
        data["Close"] = data["Adjusted Close"]
        data[["Open", "High", "Low", "Close", "Volume"]].round(4).to_csv(
            save_dir / f"{ticker}.csv"
        )

        print(f"{ticker} has been downloaded to {save_dir}/{ticker}.csv")
        return True
    except Exception as e:
        print(str(e))
        print(f"{ticker} Not found.")


current = 0
master_time = time.perf_counter()
for l in df_constituents.tickers.values:
    for ticker in l.split(","):
        if os.path.exists(ALPHA_VANTAGE_DIR_PATH / f"{ticker}.csv"):
            continue

        # clean ticker symbol if the ticker cannot be found
        corrected_tickers = [ticker]
        if "." in ticker:
            corrected_tickers += [ticker.replace(".", "-"), ticker.replace(".", "")]
        for corrected_ticker in corrected_tickers:
            if current == 75:
                while time.perf_counter() - master_time < 60:
                    time.sleep(1)
                master_time = time.perf_counter()
                current = 0

            t = time.perf_counter()
            current += 1
            if get_alpha_vantage(SECRET, corrected_ticker, ALPHA_VANTAGE_DIR_PATH):
                break
