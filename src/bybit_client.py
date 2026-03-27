"""
Fetch Bybit USDT perpetual trading pairs for TradingView scanner.
"""
import requests
from typing import Iterator, Optional


BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"


def fetch_bybit_usdt_perp_pairs(max_pairs: Optional[int] = None) -> list:
    """
    Fetch all USDT perpetual pairs from Bybit, formatted for TradingView (BYBIT:XXXUSDT).
    """
    pairs = []
    cursor = None  # type: Optional[str]

    while True:
        params: dict = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(BYBIT_INSTRUMENTS_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {data.get('retMsg', 'Unknown')}")

        result = data.get("result", {})
        items = result.get("list", [])

        for item in items:
            if item.get("status") != "Trading":
                continue
            symbol = item.get("symbol", "")
            if symbol and symbol.endswith("USDT"):
                # TradingView uses .P suffix for perpetual contracts
                pairs.append(f"BYBIT:{symbol}.P")
                if max_pairs and len(pairs) >= max_pairs:
                    return pairs

        cursor = result.get("nextPageCursor")
        if not cursor or not items:
            break

    return pairs


def get_test_pairs() -> list[str]:
    """Return 4 pairs for test mode: BTC, ETH, SOL, DOGE. Use .P for perpetual."""
    return [
        "BYBIT:BTCUSDT.P",
        "BYBIT:ETHUSDT.P",
        "BYBIT:SOLUSDT.P",
        "BYBIT:DOGEUSDT.P",
    ]
