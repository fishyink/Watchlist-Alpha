"""
Market cap rankings for Pass 2 filter (top 300).
- CoinMarketCap: optional, requires API key (coinmarketcap.com/api)
- CoinGecko: zero setup, no API key needed (fallback)
"""
from __future__ import annotations

import json
import os
import re
import logging
from pathlib import Path
from typing import Set, Optional
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

CMC_LISTINGS_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
GECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


def _bybit_symbol_to_cmc_symbol(bybit_symbol: str) -> str:
    """Convert BYBIT:1000PEPEUSDT.P -> PEPE, BYBIT:BTCUSDT.P -> BTC."""
    if not bybit_symbol:
        return ""
    # Strip BYBIT: prefix and .P suffix
    s = bybit_symbol.strip().upper()
    if ":" in s:
        s = s.split(":")[-1]
    s = s.replace(".P", "").replace("USDT", "")
    # Strip leading digits (e.g. 1000PEPE -> PEPE, 1000000BABYDOGE -> BABYDOGE)
    s = re.sub(r"^\d+", "", s)
    return s


def fetch_top_n_by_market_cap(api_key: str, n: int = 300) -> Set[str]:
    """
    Fetch top N cryptocurrencies by market cap from CoinMarketCap.
    Returns set of uppercase symbols (e.g. {"BTC", "ETH", "SOL"}).
    """
    symbols = set()
    start = 1
    limit = 100  # CMC allows max 100 per call

    while len(symbols) < n:
        try:
            resp = __import__("urllib.request").request.urlopen(
                __import__("urllib.request").Request(
                    f"{CMC_LISTINGS_URL}?start={start}&limit={limit}",
                    headers={"X-CMC_PRO_API_KEY": api_key, "Accept": "application/json"},
                ),
                timeout=15,
            )
        except Exception as e:
            logger.warning("CoinMarketCap API error: %s", e)
            break

        import json
        data = json.loads(resp.read().decode())
        items = data.get("data", [])
        if not items:
            break

        for item in items:
            sym = (item.get("symbol") or "").upper()
            if sym:
                symbols.add(sym)
            if len(symbols) >= n:
                break

        if len(items) < limit:
            break
        start += limit

    return symbols


def fetch_top_n_coingecko(n: int = 300) -> Set[str]:
    """
    Fetch top N cryptocurrencies by market cap from CoinGecko.
    No API key required. Returns set of uppercase symbols.
    """
    symbols = set()
    page = 1
    per_page = 250  # CoinGecko max per request
    while len(symbols) < n:
        try:
            url = f"{GECKO_MARKETS_URL}?vs_currency=usd&order=market_cap_desc&per_page={per_page}&page={page}"
            with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=15) as resp:
                items = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("CoinGecko API error: %s", e)
            break
        if not items:
            break
        for item in items:
            sym = (item.get("symbol") or "").upper()
            if sym:
                symbols.add(sym)
            if len(symbols) >= n:
                break
        if len(items) < per_page:
            break
        page += 1
    return symbols


# Cached CoinGecko list (updated weekly via update_market_cap_list.py)
_COINGECKO_CACHE = None


def _get_coingecko_cache_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "top300_coingecko.json"


def get_top_symbols(api_key: Optional[str] = None, top_n: int = 300) -> Set[str]:
    """
    Get top N symbols by market cap.
    Uses CoinMarketCap if API key provided. Otherwise CoinGecko: reads from
    config/top300_coingecko.json if present (run update_market_cap_list.py weekly),
    else fetches from API.
    """
    key = api_key or os.environ.get("COINMARKETCAP_API_KEY")
    if key:
        return fetch_top_n_by_market_cap(key, top_n)
    # CoinGecko: prefer cached file (updated weekly)
    cache_path = _get_coingecko_cache_path()
    if cache_path.exists():
        try:
            with open(cache_path, encoding="utf-8") as f:
                data = json.load(f)
            symbols = set((data.get("symbols") or [])[:top_n])
            if symbols:
                logger.info("Using cached CoinGecko top %d from %s", len(symbols), cache_path.name)
                return symbols
        except Exception as e:
            logger.warning("Could not load cache %s: %s", cache_path, e)
    # Fallback: fetch from CoinGecko API
    logger.info("Using CoinGecko API for top %d (no cache)", top_n)
    return fetch_top_n_coingecko(top_n)


def is_in_top_n(bybit_symbol: str, top_symbols: Set[str]) -> bool:
    """Check if Bybit pair's base is in top N by market cap."""
    if not top_symbols:
        return True  # No filter if we couldn't fetch
    cmc_sym = _bybit_symbol_to_cmc_symbol(bybit_symbol)
    return cmc_sym in top_symbols


def filter_bybit_pairs_by_market_cap(
    pairs: list[str], top_n: int, cmc_api_key: Optional[str] = None
) -> list[str]:
    """
    Keep only pairs whose base asset is in the top ``top_n`` by market cap.
    Uses CoinMarketCap if ``cmc_api_key`` is set, else CoinGecko (cache or API).
    """
    if top_n <= 0 or not pairs:
        return pairs
    top_symbols = get_top_symbols(api_key=cmc_api_key, top_n=top_n)
    if not top_symbols:
        logger.warning("Market cap list empty — skipping Phase 1 market cap filter")
        return pairs
    out = [p for p in pairs if is_in_top_n(p, top_symbols)]
    logger.info(
        "Phase 1 market cap top %d: %d Bybit USDT pairs -> %d after filter",
        top_n,
        len(pairs),
        len(out),
    )
    return out
