"""
Main orchestrator for TradingView strategy scanner.
Runs multiple strategies, each against Bybit USDT pairs, writing one Excel file per strategy.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from playwright.sync_api import sync_playwright

from .bybit_client import fetch_bybit_usdt_perp_pairs, get_test_pairs
from .market_cap import filter_bybit_pairs_by_market_cap
from .excel_writer import (
    create_workbook,
    open_workbook_for_append,
    get_completed_symbols,
    append_result_row,
    get_next_data_row,
    get_data_rows,
    sort_data_by_net_profit,
)
from .html_writer import write_html_report
from .paths import resolve_storage_state_path
from .scraper import (
    TradingViewScraper,
    PairStallTimeoutError,
    extract_original_pair_from_url,
    METRIC_KEYS,
)

logger = logging.getLogger(__name__)


def _canonical_pair_label(symbol: str) -> str:
    """
    Normalize for dedup: BYBIT:BTCUSDT.P, BTCUSDT.P, BYBIT:BTCUSDT -> BTCUSDT.
    """
    s = (symbol or "").strip().upper().replace(" ", "")
    if ":" in s:
        s = s.split(":", 1)[1]
    if s.endswith(".P"):
        s = s[:-2]
    return s


def _chart_url_with_interval(url: str, interval_minutes: int | None) -> str:
    """
    Append ?interval=N (minutes) to the saved chart URL.

    Your layout already has the correct timeframe; this is not "setting the chart wrong twice".
    TradingView often opens the Change interval dialog when Playwright changes symbols in bulk —
    keeping interval on the URL aligns TV's internal state and usually prevents that popup.
    Omit interval in config to use the raw saved URL only.
    """
    if interval_minutes is None:
        return url
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params["interval"] = [str(int(interval_minutes))]
    new_query = urlencode(params, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def run_scan(
    strategies: list[dict[str, str]],
    output_dir: Path,
    test_mode: bool = True,
    max_pairs=None,
    pairs_override: Optional[list[str]] = None,
    strategy_index_override: Optional[int] = None,
    existing_xlsx_path: Optional[Path] = None,
    headless: bool = False,
    delay_between_symbols_sec: float = 5.0,
    wait_after_symbol_change_sec: Optional[float] = None,
    delay_between_strategies_sec: float = 10.0,
    backtest_date_range: str = "range_from_chart",
    pause_for_manual_login: bool = False,
    login_wait_seconds: int = 120,
    storage_state_path: Optional[str] = None,
    browser_channel: Optional[str] = None,
    output_suffix: str = "scan",
    phase1_market_cap_top_n: Optional[int] = None,
    market_cap_provider: str = "coingecko",
    coinmarketcap_api_key: Optional[str] = None,
    phase1_fast_mode: bool = True,
    chart_initial_wait_ms: Optional[int] = None,
    backtest_ready_poll_ms: Optional[int] = None,
    backtest_ready_stability_ms: Optional[int] = None,
    debug_screenshot_dir: Optional[Path] = None,
    debug_screenshots_full_page: bool = False,
    prefer_url_symbol_change: bool = True,
    url_symbol_settle_ms: int = 2800,
    on_pair_complete: Optional[
        Callable[[str, int, int, Optional[Path], Optional[dict[str, Any]]], None]
    ] = None,
    control: Optional[dict[str, bool]] = None,
    pair_stall_timeout_sec: float = 420.0,
    chart_soft_refresh_every_n_pairs: int = 0,
) -> list[Path]:
    """
    Run full scan: for each strategy, create Excel (or append to existing if
    existing_xlsx_path is set), iterate pairs, save after each.
    Returns list of output file paths.
    When existing_xlsx_path is set, pairs_override must contain the remaining
    pairs to process (resume support).
    on_pair_complete(symbol, pair_idx, total, filepath, metrics) after each pair (metrics may be None).
    control: if provided, check control.get("stop") to break early.
    pair_stall_timeout_sec: if > 0, each pair (symbol change + metrics) must finish within this many
        seconds or we reload the chart URL once and retry; 0 disables (default 420 ≈ 7 min).
    chart_soft_refresh_every_n_pairs: if > 0, after every N completed pair attempts (same strategy),
        reload the chart URL + date range to clear TV/Chrome buildup (helps long headless runs). 0 = off.
    """
    if pairs_override is not None:
        pairs = pairs_override
    elif test_mode:
        pairs = get_test_pairs()
    else:
        # Market cap filter needs full Bybit list first, then optional max_pairs slice
        cmc_key = None
        if (market_cap_provider or "").lower() == "coinmarketcap":
            cmc_key = (coinmarketcap_api_key or "").strip() or None
        if phase1_market_cap_top_n and phase1_market_cap_top_n > 0:
            pairs = fetch_bybit_usdt_perp_pairs(None)
            pairs = filter_bybit_pairs_by_market_cap(
                pairs, phase1_market_cap_top_n, cmc_api_key=cmc_key
            )
            if max_pairs:
                pairs = pairs[:max_pairs]
        else:
            pairs = fetch_bybit_usdt_perp_pairs(max_pairs)

    # Stable unique order (original list can repeat; Bybit order preserved)
    pairs = list(dict.fromkeys(pairs))

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []

    is_quick_phase1 = (backtest_date_range or "").strip() == "range_from_chart"
    use_fast = bool(phase1_fast_mode and is_quick_phase1)
    _cw = chart_initial_wait_ms if chart_initial_wait_ms is not None else (3800 if use_fast else 5200)
    _poll = backtest_ready_poll_ms if backtest_ready_poll_ms is not None else (450 if use_fast else 800)
    _stab = backtest_ready_stability_ms if backtest_ready_stability_ms is not None else (800 if use_fast else 1500)

    with sync_playwright() as p:
        launch_opts = {"headless": headless}
        if browser_channel:
            launch_opts["channel"] = browser_channel
            logger.info("Using browser: %s", browser_channel)
        # Reduce automation detection (helps with Google sign-in)
        launch_opts["args"] = ["--disable-blink-features=AutomationControlled"]
        logger.info("Launching Chromium headless=%s (UI toggle / config)", headless)
        browser = p.chromium.launch(**launch_opts)

        # Load saved session if available (cookies from last login)
        state_path = resolve_storage_state_path(storage_state_path)
        has_saved_session = bool(state_path and state_path.exists())

        context_options = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        if has_saved_session:
            context_options["storage_state"] = str(state_path)
            logger.info("Loaded saved TradingView session from %s", state_path)
        elif state_path is not None:
            logger.warning(
                "No session file at %s — use Step 1 (Log into TradingView) or run once non-headless with pause.",
                state_path,
            )

        context = browser.new_context(**context_options)

        first_page = None
        needs_login_pause = pause_for_manual_login and not headless and not has_saved_session
        if needs_login_pause:
            first_page = context.new_page()
            first_page.goto("https://www.tradingview.com", wait_until="domcontentloaded", timeout=60_000)
            first_page.wait_for_timeout(3000)  # Let page stabilize
            print("\n" + "="*60)
            print(">>> LOG IN to TradingView in the browser window.")
            print(f">>> You have {login_wait_seconds} seconds - the scan will start automatically.")
            print("="*60 + "\n")
            for remaining in range(login_wait_seconds, 0, -15):
                print(f"  ... {remaining} seconds left to log in ...")
                time.sleep(min(15, remaining))

            # Extra wait for redirects/OAuth to finish
            time.sleep(5)
            if state_path:
                state_path.parent.mkdir(parents=True, exist_ok=True)
                context.storage_state(path=str(state_path))
                logger.info("Saved TradingView session to %s", state_path)

        for strat_idx, strat in enumerate(strategies):
            url = strat.get("url", "").strip()
            name = strat.get("name", "").strip() or None
            if not url:
                logger.warning("Skipping strategy %d: empty URL", strat_idx + 1)
                continue
            interval_raw = strat.get("interval")
            interval_minutes: int | None = None
            if interval_raw is not None and str(interval_raw).strip() != "":
                try:
                    interval_minutes = int(interval_raw)
                except (TypeError, ValueError):
                    interval_minutes = None
            chart_url = _chart_url_with_interval(url, interval_minutes)

            logger.info("Strategy %d/%d: %s", strat_idx + 1, len(strategies), chart_url[:60] + "...")

            # Reuse login page for first strategy (keeps session); new page for rest
            if first_page is not None and strat_idx == 0:
                page = first_page
                first_page = None  # Use only once
            else:
                page = context.new_page()
            # Caps locator/evaluate waits so a wedged tab errors out instead of blocking forever (some TV states).
            page.set_default_timeout(60_000)
            si = strategy_index_override if strategy_index_override is not None else strat_idx + 1
            delay = wait_after_symbol_change_sec if wait_after_symbol_change_sec is not None else delay_between_symbols_sec
            strat_shot_dir: Optional[Path] = None
            if debug_screenshot_dir is not None:
                strat_shot_dir = debug_screenshot_dir / f"strategy_{si:02d}"
                strat_shot_dir.mkdir(parents=True, exist_ok=True)

            scraper = TradingViewScraper(
                page,
                delay_after_symbol_sec=delay,
                backtest_date_range=backtest_date_range,
                debug_screenshot_dir=strat_shot_dir,
                chart_load_wait_ms=_cw,
                backtest_poll_ms=_poll,
                backtest_stability_ms=_stab,
                debug_screenshots_full_page=debug_screenshots_full_page,
                prefer_url_symbol_change=prefer_url_symbol_change,
                url_symbol_settle_ms=url_symbol_settle_ms,
            )

            try:
                extra_wait = 10 if backtest_date_range == "entire_history" else 0
                scraper.navigate_and_wait(chart_url, extra_wait_sec=extra_wait)
                scraper.set_backtest_date_range()

                stall_sec = max(0.0, float(pair_stall_timeout_sec))

                def _pair_work(sym: str, change_first: bool) -> dict[str, Any]:
                    """Run change_symbol (if needed) + extract_metrics; reload chart once on stall."""
                    def attempt() -> dict[str, Any]:
                        if change_first:
                            scraper.change_symbol(sym)
                        return scraper.extract_metrics()

                    if stall_sec <= 0:
                        return attempt()
                    scraper.set_pair_deadline(time.time() + stall_sec)
                    try:
                        try:
                            return attempt()
                        except PairStallTimeoutError:
                            scraper.set_pair_deadline(None)
                            logger.warning(
                                "Stall timeout (~%ds) for %s — reloading chart, retry once",
                                int(stall_sec),
                                sym,
                            )
                            scraper.navigate_and_wait(chart_url, extra_wait_sec=extra_wait)
                            scraper.set_backtest_date_range()
                            scraper.set_pair_deadline(time.time() + stall_sec)
                            return attempt()
                    finally:
                        scraper.set_pair_deadline(None)

                if existing_xlsx_path is not None and existing_xlsx_path.exists():
                    # Resume: append to existing workbook
                    wb, filepath = open_workbook_for_append(existing_xlsx_path)
                    ws = wb.active
                    original = (ws.cell(row=2, column=2).value or "").strip() or "Unknown"
                    display_url = (ws.cell(row=2, column=1).value or "").strip() or url
                    if pairs_override is not None:
                        pairs_to_process = pairs_override
                    else:
                        completed = get_completed_symbols(ws)
                        pairs_to_process = [p for p in pairs if _canonical_pair_label(p) not in completed]
                    orig_canon = None  # Skip original-extraction; already in file
                else:
                    # Fresh run: create new workbook
                    original = extract_original_pair_from_url(url) or scraper.get_original_pair_from_chart()
                    original = original or "Unknown"
                    display_url = strat.get("export_link") or url
                    wb, filepath = create_workbook(
                        output_dir=output_dir,
                        strategy_index=si,
                        strategy_url=display_url,
                        original_pair=original,
                        strategy_name=name,
                        output_suffix=output_suffix,
                    )
                    ws = wb.active
                    pairs_to_process = pairs
                    orig_canon = _canonical_pair_label(
                        original if original.startswith("BYBIT:") else f"BYBIT:{original}"
                    ) if original and original != "Unknown" else None

                results.append(filepath)
                if not (existing_xlsx_path is not None and existing_xlsx_path.exists()):
                    wb.save(filepath)

                # Save session after first chart load - captures fully authenticated state
                if state_path and strat_idx == 0:
                    state_path.parent.mkdir(parents=True, exist_ok=True)
                    context.storage_state(path=str(state_path))
                    logger.info("Saved session after chart load (login persisted)")

                # Extract metrics for original pair first (chart is already on that symbol) - skip when resuming or pairs_override
                if existing_xlsx_path is None and pairs_override is None and original and original != "Unknown":
                    orig_symbol = original if original.startswith("BYBIT:") else f"BYBIT:{original}"
                    try:
                        metrics = _pair_work(orig_symbol, change_first=False)
                        row_num = get_next_data_row(ws)
                        append_result_row(ws, row_num, orig_symbol, metrics)
                        wb.save(filepath)
                        logger.info("  0/%d %s (original) OK", len(pairs) + 1, orig_symbol)
                        scraper._debug_screenshot("row0_original")
                        if on_pair_complete:
                            on_pair_complete(
                                orig_symbol,
                                1,
                                len(pairs_to_process),
                                filepath,
                                metrics,
                            )
                    except Exception as e:
                        logger.warning("  Original pair %s FAIL: %s", orig_symbol, e)
                        row_num = get_next_data_row(ws)
                        append_result_row(ws, row_num, orig_symbol, {"error": str(e)})
                        wb.save(filepath)
                        if on_pair_complete:
                            on_pair_complete(
                                orig_symbol,
                                1,
                                len(pairs_to_process),
                                filepath,
                                {"error": str(e)},
                            )

                prev_canon: Optional[str] = None
                proactive_every = max(0, int(chart_soft_refresh_every_n_pairs or 0))
                proactive_count = 0
                for pair_idx, symbol in enumerate(pairs_to_process):
                    sym_canon = _canonical_pair_label(symbol)
                    if orig_canon is not None and sym_canon == orig_canon:
                        logger.info(
                            "  skip %s (already row 0 / chart original)",
                            symbol,
                        )
                        continue
                    if prev_canon is not None and sym_canon == prev_canon:
                        logger.warning("  skip consecutive duplicate %s", symbol)
                        continue
                    prev_canon = sym_canon
                    metrics_cb: Optional[dict[str, Any]] = None
                    logger.info(
                        "  Pair %d/%d: starting %s",
                        pair_idx + 1,
                        len(pairs_to_process),
                        symbol,
                    )
                    try:
                        metrics = _pair_work(symbol, change_first=True)
                        row_num = get_next_data_row(ws)
                        append_result_row(ws, row_num, symbol, metrics)
                        wb.save(filepath)
                        logger.info("  %d/%d %s OK", pair_idx + 1, len(pairs_to_process), symbol)
                        metrics_cb = metrics
                    except Exception as e:
                        logger.warning("  %d/%d %s FAIL: %s", pair_idx + 1, len(pairs_to_process), symbol, e)
                        row_num = get_next_data_row(ws)
                        append_result_row(ws, row_num, symbol, {"error": str(e)})
                        wb.save(filepath)
                        metrics_cb = {"error": str(e)}
                    if on_pair_complete:
                        on_pair_complete(
                            symbol,
                            pair_idx + 1,
                            len(pairs_to_process),
                            filepath,
                            metrics_cb,
                        )
                    proactive_count += 1
                    if (
                        proactive_every > 0
                        and proactive_count % proactive_every == 0
                        and not (control and control.get("stop"))
                    ):
                        try:
                            logger.info(
                                "Proactive chart reload after %d pair(s) (chart_soft_refresh_every_n_pairs=%d)",
                                proactive_count,
                                proactive_every,
                            )
                            scraper.navigate_and_wait(chart_url, extra_wait_sec=extra_wait)
                            scraper.set_backtest_date_range()
                        except Exception as ex:
                            logger.warning("Proactive chart reload failed (continuing): %s", ex)
                    if control and control.get("stop"):
                        logger.info("  Stop requested, breaking")
                        break

                # Sort by Net Profit descending (highest at top)
                sort_data_by_net_profit(ws)
                wb.save(filepath)

                # Write HTML report (dark dashboard style, sortable columns)
                data_rows = get_data_rows(ws)
                if data_rows:
                    html_path = write_html_report(
                        output_dir=output_dir,
                        strategy_index=si,
                        strategy_url=display_url,
                        original_pair=original,
                        strategy_name=name,
                        rows=data_rows,
                        xlsx_path=filepath,
                    )
                    results.append(html_path)
                    logger.info("  HTML report: %s", html_path.name)
            finally:
                page.close()

            if delay_between_strategies_sec > 0 and strat_idx < len(strategies) - 1:
                time.sleep(delay_between_strategies_sec)

        browser.close()

    return results
