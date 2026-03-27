#!/usr/bin/env python3
"""
CLI entry point for TradingView Bybit strategy scanner.
Usage:
  python run_scan.py                    # uses config.yaml, test mode (4 pairs)
  python run_scan.py --full              # full run (all pairs)
  python run_scan.py --full -l 5 -S 2 --no-pause  # 5 pairs, strategy 2, skip login wait
  python run_scan.py --test              # explicit test mode
  python run_scan.py --config other.yaml # custom config file
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

import yaml

# Add project root to path for imports
import sys
_project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_project_root))

from src.main import run_scan
from src.paths import resolve_storage_state_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def load_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingView strategy scanner for Bybit pairs")
    parser.add_argument("--config", "-c", default="config/config.yaml", help="Config file path")
    parser.add_argument("--test", "-t", action="store_true", help="Test mode (4 pairs only)")
    parser.add_argument("--full", "-f", action="store_true", help="Full run (all pairs)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--limit", "-l", type=int, help="Max pairs to scan (e.g. 3 for quick test)")
    parser.add_argument("--strategy", "-S", type=int, metavar="N", help="Run only strategy N (1-based, e.g. 3 for LINK chart)")
    parser.add_argument("--re-login", action="store_true", help="Ignore saved session, show login pause (fixes stale login)")
    parser.add_argument(
        "--no-pause",
        action="store_true",
        help="Skip login wait (use with saved session); good for quick tests",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        print("Create config/config.yaml with strategies list. See README.")
        sys.exit(1)

    cfg = load_config(config_path)
    strategies = cfg.get("strategies", [])
    if not strategies:
        print("No strategies in config. Add entries under 'strategies' with 'url' (and optional 'name').")
        sys.exit(1)

    test_mode = args.test or (not args.full and cfg.get("test_mode", True))
    max_pairs = args.limit if args.limit is not None else cfg.get("max_pairs")
    output_dir = Path(cfg.get("output_dir", "output"))
    delay_symbols = float(cfg.get("delay_between_symbols_sec", 5))
    wait_after_symbol = float(cfg.get("wait_after_symbol_change_sec", delay_symbols))
    delay_strategies = float(cfg.get("delay_between_strategies_sec", 10))
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
    headless = args.headless or cfg.get("headless", False)
    backtest_date_range = cfg.get("backtest_date_range", "range_from_chart")
    pause_for_manual_login = (
        False if args.no_pause else cfg.get("pause_for_manual_login", False)
    )
    login_wait_seconds = int(cfg.get("login_wait_seconds", 120))
    storage_state_path = cfg.get("storage_state_path")
    browser_channel = cfg.get("browser_channel") or None

    phase1_mc = cfg.get("phase1_market_cap_top_n")
    if phase1_mc is not None:
        try:
            phase1_mc = int(phase1_mc)
        except (TypeError, ValueError):
            phase1_mc = None
    market_cap_provider = str(cfg.get("market_cap_provider", "coingecko") or "coingecko")
    cmc_key = (cfg.get("coinmarketcap_api_key") or "").strip() or os.environ.get(
        "COINMARKETCAP_API_KEY"
    )

    def _int_or_none(val):
        if val is None or val == "":
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    phase1_fast_mode = bool(cfg.get("phase1_fast_mode", True))
    chart_initial_wait_ms = _int_or_none(cfg.get("chart_initial_wait_ms"))
    backtest_ready_poll_ms = _int_or_none(cfg.get("backtest_ready_poll_ms"))
    backtest_ready_stability_ms = _int_or_none(cfg.get("backtest_ready_stability_ms"))

    debug_shot_full_page = bool(cfg.get("debug_screenshots_full_page", False))
    prefer_url_sym = bool(cfg.get("prefer_url_symbol_change", True))
    _us = _int_or_none(cfg.get("url_symbol_settle_ms"))
    url_settle_ms = _us if _us is not None else 2800

    debug_shot_root: Path | None = None
    if cfg.get("debug_screenshots"):
        base = Path(cfg.get("debug_screenshots_dir", "output/scan_screenshots"))
        debug_shot_root = base / datetime.now().strftime("%Y%m%d_%H%M%S")

    # Force fresh login: delete saved session so login pause will run
    if args.re_login and storage_state_path:
        state_file = resolve_storage_state_path(storage_state_path)
        if state_file and state_file.exists():
            state_file.unlink()
            print("Cleared saved session (--re-login). You will be prompted to log in.")

    # --full alone = all pairs; --full --limit N = cap after market-cap filter
    if args.full:
        max_pairs = args.limit if args.limit is not None else None

    if args.strategy is not None:
        idx = args.strategy - 1
        if 0 <= idx < len(strategies):
            strategies = [strategies[idx]]
            print(f"Running only strategy {args.strategy}: {strategies[0].get('url', '')[:50]}...")
        else:
            print(f"Invalid --strategy {args.strategy}. Must be 1-{len(strategies)}.")
            sys.exit(1)

    strategy_index_override = args.strategy if args.strategy is not None else None
    pair_desc = "4 (test)"
    if not test_mode:
        if phase1_mc and phase1_mc > 0:
            pair_desc = f"Bybit & top {phase1_mc} by market cap"
            if market_cap_provider.lower() == "coinmarketcap" and cmc_key:
                pair_desc += " (CoinMarketCap)"
            else:
                pair_desc += " (CoinGecko)"
        else:
            pair_desc = "all Bybit USDT perps"
        if max_pairs:
            pair_desc += f", max {max_pairs}"
    print(
        f"Strategies: {len(strategies)}, Pairs: {pair_desc}, Backtest: {backtest_date_range}"
    )
    print("Starting scan...")
    if debug_shot_root:
        print(f"Debug screenshots -> {debug_shot_root}")
    paths = run_scan(
        strategies=strategies,
        output_dir=output_dir,
        test_mode=test_mode,
        max_pairs=max_pairs,
        strategy_index_override=strategy_index_override,
        headless=headless,
        delay_between_symbols_sec=delay_symbols,
        wait_after_symbol_change_sec=wait_after_symbol,
        delay_between_strategies_sec=delay_strategies,
        backtest_date_range=backtest_date_range,
        pause_for_manual_login=pause_for_manual_login,
        login_wait_seconds=login_wait_seconds,
        storage_state_path=storage_state_path,
        browser_channel=browser_channel,
        phase1_market_cap_top_n=phase1_mc,
        market_cap_provider=market_cap_provider,
        coinmarketcap_api_key=cmc_key,
        phase1_fast_mode=phase1_fast_mode,
        chart_initial_wait_ms=chart_initial_wait_ms,
        backtest_ready_poll_ms=backtest_ready_poll_ms,
        backtest_ready_stability_ms=backtest_ready_stability_ms,
        debug_screenshot_dir=debug_shot_root,
        debug_screenshots_full_page=debug_shot_full_page,
        prefer_url_symbol_change=prefer_url_sym,
        url_symbol_settle_ms=url_settle_ms,
        pair_stall_timeout_sec=pair_stall_timeout_sec,
        chart_soft_refresh_every_n_pairs=chart_soft_refresh_every_n_pairs,
    )
    print(f"\nDone. Output: {len(paths)} file(s)")
    for p in paths:
        print(f"  - {p}")


if __name__ == "__main__":
    main()
