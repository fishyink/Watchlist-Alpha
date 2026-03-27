#!/usr/bin/env python3
"""
Fetch top 300 cryptocurrencies by market cap from CoinGecko and save to config.
Run weekly to keep the list updated. Pass 2 deep backtest uses this cached list.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from src.market_cap import fetch_top_n_coingecko

CACHE_FILE = Path(__file__).resolve().parent / "config" / "top300_coingecko.json"


def main() -> None:
    print("Fetching top 300 from CoinGecko...")
    symbols = fetch_top_n_coingecko(300)
    symbols_list = sorted(symbols)
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({"symbols": symbols_list, "count": len(symbols_list)}, f, indent=2)
    print(f"Saved {len(symbols_list)} symbols to {CACHE_FILE}")
    sys.exit(0)


if __name__ == "__main__":
    main()
