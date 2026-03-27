"""
Pass 2 filter: read Pass 1 Excel results and apply criteria to get pairs for deep backtest.
Criteria: # Trades >= min (default 50), Net Profit > 0, Win Rate >= 45%, Market cap rank <= 300.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from openpyxl import load_workbook

from .excel_writer import get_data_rows
from .market_cap import get_top_symbols, is_in_top_n

logger = logging.getLogger(__name__)

# Column indices in Excel (0-based): 0=Symbol, 1=Net Profit, 2=Net Profit %, ..., 9=Win Rate %, 10=# Trades
IDX_SYMBOL = 0
IDX_NET_PROFIT = 1
IDX_TRADES = 10
IDX_WIN_RATE = 9

MIN_TRADES = 50  # minimum # Trades (inclusive); override via config pass2.min_trades
MIN_WIN_RATE = 45
TOP_MARKET_CAP = 300


def min_trades_from_config(cfg: dict | None) -> int:
    """Read pass2.min_trades from config dict; fall back to MIN_TRADES."""
    if not cfg:
        return MIN_TRADES
    p2 = cfg.get("pass2")
    if not isinstance(p2, dict) or p2.get("min_trades") is None:
        return MIN_TRADES
    try:
        return max(0, int(p2["min_trades"]))
    except (TypeError, ValueError):
        return MIN_TRADES


def filter_pass1_results(
    xlsx_path: Path,
    min_trades: int = MIN_TRADES,
    min_win_rate: float = MIN_WIN_RATE,
    top_market_cap: int = TOP_MARKET_CAP,
    api_key: Optional[str] = None,
) -> list[str]:
    """
    Read Pass 1 Excel and return list of BYBIT:XXXUSDT.P symbols that pass all criteria.
    - # Trades >= min_trades (default 50)
    - Net Profit > 0
    - Win Rate % >= min_win_rate (default 45)
    - Market cap rank <= top_market_cap (default 300, from CoinMarketCap)
    """
    if not xlsx_path.exists():
        logger.warning("Pass 1 file not found: %s", xlsx_path)
        return []

    wb = load_workbook(xlsx_path, data_only=True)
    ws = wb.active
    rows = get_data_rows(ws)
    wb.close()

    if not rows:
        return []

    try:
        top_symbols = get_top_symbols(api_key=api_key, top_n=top_market_cap)
    except Exception as e:
        logger.warning("Market cap fetch failed, skipping rank filter: %s", e)
        top_symbols = set()

    passed = []
    for row in rows:
        if len(row) <= IDX_TRADES:
            continue
        symbol = row[IDX_SYMBOL] if row else ""
        if not symbol or not isinstance(symbol, str) or "USDT" not in symbol.upper():
            continue

        # Ensure BYBIT: prefix
        if not symbol.startswith("BYBIT:"):
            symbol = f"BYBIT:{symbol}" if ":" not in symbol else symbol

        # # Trades >= min_trades
        trades = row[IDX_TRADES]
        try:
            if trades is None or trades == "":
                t = 0
            else:
                t = int(float(trades))
        except (TypeError, ValueError):
            t = 0
        if t < min_trades:
            continue

        # Net Profit > 0
        net_profit = row[IDX_NET_PROFIT]
        try:
            np = float(net_profit) if net_profit is not None else 0
        except (TypeError, ValueError):
            np = 0
        if np <= 0:
            continue

        # Win Rate >= 45
        win_rate = row[IDX_WIN_RATE]
        try:
            wr = float(win_rate) if win_rate is not None else 0
        except (TypeError, ValueError):
            wr = 0
        if wr < min_win_rate:
            continue

        # Market cap rank <= 300
        if top_symbols and not is_in_top_n(symbol, top_symbols):
            continue

        passed.append(symbol)

    return passed
