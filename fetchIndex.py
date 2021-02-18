import yfinance as yf
import time
from pathlib import Path

YFINANCE_DATA_DIR_PATH = Path("yfinance")

t = time.perf_counter()
ticker = yf.Ticker("^GSPC")
ticker.history(
    period="max",
    auto_adjust=True,
).to_csv(YFINANCE_DATA_DIR_PATH / "GSPC.csv")
s = time.perf_counter() - t
print(f"{t} Time elapsed: {s:.2f}")