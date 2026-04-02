import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta

PAIRS = [
    "BTCUSDT",
]

TIMEFRAMES = {
    "D": 1440,
    "60": 60,
    "15": 15,
    "5": 5,
}

INITIAL_HISTORY_DAYS = 90
BASE_URL = "https://api.bybit.com"
DATA_DIR = "data"


def fetch_klines(symbol, interval, start_ms, end_ms, limit=1000):
    url = f"{BASE_URL}/v5/market/kline"
    params = {
        "category": "spot",
        "symbol": symbol,
        "interval": interval,
        "start": start_ms,
        "end": end_ms,
        "limit": limit,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data["retCode"] != 0:
        print(f"  API error: {data['retMsg']}")
        return []
    return data["result"]["list"]


def fetch_all_klines(symbol, interval, start_date, end_date):
    interval_ms = TIMEFRAMES[interval] * 60 * 1000
    start_ms = int(start_date.timestamp() * 1000)
    end_ms = int(end_date.timestamp() * 1000)
    all_data = []
    current_end = end_ms
    while current_end > start_ms:
        try:
            rows = fetch_klines(symbol, interval, start_ms, current_end)
            if not rows:
                break
            all_data.extend(rows)
            oldest_ts = int(rows[-1][0])
            if oldest_ts <= start_ms:
                break
            current_end = oldest_ts - 1
            print(f"  Fetched {len(rows)} candles, total: {len(all_data)}")
            time.sleep(0.15)
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(1)
            continue
    return all_data


def raw_to_dataframe(raw_data):
    columns = ["open_time", "open", "high", "low", "close", "volume", "turnover"]
    df = pd.DataFrame(raw_data, columns=columns)
    df["open_time"] = pd.to_datetime(df["open_time"].astype(int), unit="ms")
    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = df[col].astype(float)
