#!/usr/bin/env python3
"""
Pass 2 (deep backtest): Filter Pass 1 results, re-scan filtered pairs with Entire history.
Output: separate *_deep_scan_*.xlsx and *_deep_scan_*.html files.

Usage:
  python run_deep_scan.py              # run Pass 2 for all strategies
  python run_deep_scan.py --strategy 3 # Pass 2 for strategy 3 only

Requires Pass 1 results in output/ (run run_scan.py --full first).
Filter thresholds: config pass2 (e.g. min_trades); market cap via CoinGecko or CoinMarketCap (coinmarketcap_api_key).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml

_project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_project_root))

from src.excel_writer import read_pass1_workbook_progress
from src.main import run_scan
from src.pass2_filter import filter_pass1_results, min_trades_from_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def find_latest_pass1_xlsx(output_dir: Path, strategy_index: int) -> Path | None:
    """Most complete Pass 1 Excel for strategy (max symbol rows), then newest mtime."""
    prefix = f"strategy_{strategy_index:02d}_"
    matches = []
    for p in output_dir.glob(f"{prefix}*_scan_*.xlsx"):
        if "deep_scan" in p.stem:
            continue
        matches.append(p)
    if not matches:
        return None
    best: Path | None = None
    best_n = -1
    best_mtime = 0.0
    for p in matches:
        try:
            _, ordered = read_pass1_workbook_progress(p)
            n = len(ordered)
        except Exception:
            n = 0
        try:
            mt = p.stat().st_mtime
        except OSError:
            mt = 0.0
        if n > best_n or (n == best_n and mt > best_mtime):
            best_n = n
            best_mtime = mt
            best = p
    return best


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pass 2: Deep backtest on filtered pairs (Entire history, separate output)"
    )
    parser.add_argument("--config", "-c", default="config/config.yaml", help="Config file")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--strategy", "-S", type=int, metavar="N", help="Run only strategy N (1-based)")
    parser.add_argument("--limit", "-l", type=int, metavar="N", help="Limit to N pairs (for testing)")
    parser.add_argument("--re-login", action="store_true", help="Force login pause")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    cfg = load_config(config_path)
    strategies = cfg.get("strategies", [])
    if not strategies:
        print("No strategies in config.")
        sys.exit(1)

    output_dir = Path(cfg.get("output_dir", "output"))
    provider = cfg.get("market_cap_provider", "coingecko")
    cmc_key = cfg.get("coinmarketcap_api_key") or os.environ.get("COINMARKETCAP_API_KEY") if provider == "coinmarketcap" else None
    if provider == "coingecko" or not cmc_key:
        print("Using CoinGecko for top 300 market cap (no API key)")

    if args.strategy is not None:
        idx = args.strategy - 1
        if 0 <= idx < len(strategies):
            strategies = [strategies[idx]]
        else:
            print(f"Invalid --strategy {args.strategy}. Must be 1-{len(strategies)}.")
            sys.exit(1)

    all_paths = []
    for strat_idx, strat in enumerate(strategies):
        strat_num = args.strategy if args.strategy is not None else (strat_idx + 1)
        url = strat.get("url", "").strip()
        if not url:
            continue

        xlsx = find_latest_pass1_xlsx(output_dir, strat_num)
        if not xlsx:
            logger.warning("No Pass 1 file for strategy %d, skipping", strat_num)
            continue

        passed = filter_pass1_results(xlsx, min_trades=min_trades_from_config(cfg), api_key=cmc_key)
        if not passed:
            logger.info("Strategy %d: no pairs passed filters, skipping", strat_num)
            continue

        # Deduplicate: Pass 1 Excel can have same symbol multiple times (original + pairs iteration)
        passed = list(dict.fromkeys(passed))

        if args.limit:
            passed = passed[: args.limit]
            logger.info("Strategy %d: limited to %d pairs (--limit)", strat_num, len(passed))

        logger.info("Strategy %d: %d pairs passed filters, running deep scan (Entire history)", strat_num, len(passed))
        try:
            _ps = cfg.get("pair_stall_timeout_sec")
            pair_stall_timeout_sec = 420.0 if _ps is None or _ps == "" else max(0.0, float(_ps))
        except (TypeError, ValueError):
            pair_stall_timeout_sec = 420.0
        try:
            _cr = cfg.get("chart_soft_refresh_every_n_pairs")
            chart_soft_refresh_every_n_pairs = 0 if _cr is None or _cr == "" else max(0, int(_cr))
        except (TypeError, ValueError):
            chart_soft_refresh_every_n_pairs = 0
        paths = run_scan(
            strategies=[strat],
            output_dir=output_dir,
            strategy_index_override=strat_num,
            test_mode=False,
            max_pairs=None,
            pairs_override=passed,
            headless=args.headless or cfg.get("headless", False),
            delay_between_symbols_sec=float(cfg.get("delay_between_symbols_sec", 5)),
            wait_after_symbol_change_sec=float(cfg.get("wait_after_symbol_change_sec", 10)),
            delay_between_strategies_sec=float(cfg.get("delay_between_strategies_sec", 5)),
            backtest_date_range="entire_history",
            pause_for_manual_login=cfg.get("pause_for_manual_login", False),
            login_wait_seconds=int(cfg.get("login_wait_seconds", 90)),
            storage_state_path=cfg.get("storage_state_path"),
            browser_channel=cfg.get("browser_channel"),
            output_suffix="deep_scan",
            pair_stall_timeout_sec=pair_stall_timeout_sec,
            chart_soft_refresh_every_n_pairs=chart_soft_refresh_every_n_pairs,
        )
        all_paths.extend(paths)

    print(f"\nDone. Output: {len(all_paths)} file(s)")
    for p in all_paths:
        print(f"  - {p}")


if __name__ == "__main__":
    main()
