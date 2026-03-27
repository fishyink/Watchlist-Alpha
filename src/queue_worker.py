"""
Background queue worker: processes queue items, runs Phase 1 (and Phase 2 if deep_backtest).
Supports resume on restart via run_state.
Re-fetches the queue after each job so new items added while running are picked up.
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from .bybit_client import fetch_bybit_usdt_perp_pairs
from .db import (
    add_run,
    clear_run_state,
    delete_queue_item,
    get_queue_items,
    get_run_state,
    parse_completed_pairs,
    serialize_completed_pairs,
    upsert_run_state,
)
from .market_cap import filter_bybit_pairs_by_market_cap
from .excel_writer import (
    read_pass1_workbook_progress,
    read_scan_preview_rows,
    row_values_for_metrics,
    sort_preview_rows_by_net_pct,
)
from .main import run_scan
from .pass2_filter import filter_pass1_results, min_trades_from_config

logger = logging.getLogger(__name__)

from .paths import get_app_root

DEFAULT_CONFIG_PATH = get_app_root() / "config" / "config.yaml"


def _canonical_pair_label(symbol: str) -> str:
    """Normalize symbol for dedup."""
    s = (symbol or "").strip().upper().replace(" ", "")
    if ":" in s:
        s = s.split(":", 1)[1]
    if s.endswith(".P"):
        s = s[:-2]
    return s


def _find_latest_pass1_xlsx(output_dir: Path, strategy_index: int) -> Optional[Path]:
    """
    Best Pass 1 workbook for this queue/strategy id (strategy_NN_*_scan_*.xlsx, no deep_scan).

    Prefer the file with the **most symbol rows** (complete run), then newest mtime.
    Using mtime alone breaks resume: a restarted run creates a new almost-empty .xlsx that
    is "newer" than the finished 500+ row file.
    """
    prefix = f"strategy_{strategy_index:02d}_"
    matches = [p for p in output_dir.glob(f"{prefix}*_scan_*.xlsx") if "deep_scan" not in p.stem]
    if not matches:
        return None
    best: Optional[Path] = None
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


def _load_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _cfg_output_dir(cfg: dict) -> Path:
    """Resolve output_dir relative to app root so paths stay valid after restart (cwd may change)."""
    p = Path(cfg.get("output_dir", "output"))
    if not p.is_absolute():
        p = get_app_root() / p
    p.mkdir(parents=True, exist_ok=True)
    return p.resolve()


def _queue_auto_remove_on_success(cfg: dict) -> bool:
    q = cfg.get("queue")
    if isinstance(q, dict):
        return bool(q.get("auto_remove_on_success", False))
    return False


def _maybe_auto_remove_queue_item(
    cfg: dict,
    qid: int,
    db_path: Optional[Path],
    on_progress: Optional[Callable[[dict[str, Any]], None]],
) -> None:
    """If enabled in config, delete queue row after a fully successful job (outputs remain on disk)."""
    if not _queue_auto_remove_on_success(cfg):
        return
    try:
        delete_queue_item(qid, db_path)
        logger.info("Removed queue item %s from list (queue.auto_remove_on_success)", qid)
        if on_progress:
            on_progress({"queue_auto_refresh": True})
    except Exception as e:
        logger.warning("Could not auto-remove queue item %s: %s", qid, e)


def _scan_timing_kwargs(cfg: dict) -> dict[str, Any]:
    """Map config.yaml timing / URL / debug keys into run_scan(...) kwargs (same behavior as run_scan.py CLI)."""

    def _intn(key: str) -> Optional[int]:
        v = cfg.get(key)
        if v is None or v == "":
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    delay_sym = float(cfg.get("delay_between_symbols_sec", 5))
    wait_raw = cfg.get("wait_after_symbol_change_sec")
    if wait_raw is None or wait_raw == "":
        wait_after = delay_sym
    else:
        try:
            wait_after = float(wait_raw)
        except (TypeError, ValueError):
            wait_after = delay_sym

    out: dict[str, Any] = {
        "delay_between_symbols_sec": delay_sym,
        "wait_after_symbol_change_sec": wait_after,
        "delay_between_strategies_sec": float(cfg.get("delay_between_strategies_sec", 5)),
        "phase1_fast_mode": bool(cfg.get("phase1_fast_mode", True)),
        "chart_initial_wait_ms": _intn("chart_initial_wait_ms"),
        "backtest_ready_poll_ms": _intn("backtest_ready_poll_ms"),
        "backtest_ready_stability_ms": _intn("backtest_ready_stability_ms"),
        "prefer_url_symbol_change": bool(cfg.get("prefer_url_symbol_change", True)),
        "debug_screenshots_full_page": bool(cfg.get("debug_screenshots_full_page", False)),
    }
    us = _intn("url_symbol_settle_ms")
    out["url_symbol_settle_ms"] = us if us is not None else 2800

    ps = cfg.get("pair_stall_timeout_sec")
    if ps is None or ps == "":
        out["pair_stall_timeout_sec"] = 420.0
    else:
        try:
            out["pair_stall_timeout_sec"] = max(0.0, float(ps))
        except (TypeError, ValueError):
            out["pair_stall_timeout_sec"] = 420.0

    cr_raw = cfg.get("chart_soft_refresh_every_n_pairs")
    if cr_raw is not None and cr_raw != "":
        try:
            out["chart_soft_refresh_every_n_pairs"] = max(0, int(cr_raw))
        except (TypeError, ValueError):
            pass

    if bool(cfg.get("debug_screenshots", False)):
        raw = cfg.get("debug_screenshots_dir", "output/scan_screenshots")
        ddir = Path(raw)
        if not ddir.is_absolute():
            ddir = get_app_root() / ddir
        out["debug_screenshot_dir"] = ddir
    else:
        out["debug_screenshot_dir"] = None

    return out


def _resolve_stored_xlsx_path(path_str: Optional[str]) -> Optional[Path]:
    """Find workbook from DB path; handle relative paths saved under an old cwd."""
    if not path_str or not str(path_str).strip():
        return None
    raw = Path(str(path_str).strip())
    candidates: list[Path] = [raw]
    if not raw.is_absolute():
        candidates.append(get_app_root() / raw)
    for c in candidates:
        try:
            r = c.resolve()
        except OSError:
            continue
        if r.is_file():
            return r
    return None


def _get_pairs_for_phase1(
    phase1_pairs: str,
    cfg: dict,
) -> list[str]:
    """Get pair list for Phase 1: 'all' or 'top300'."""
    if phase1_pairs == "all":
        pairs = fetch_bybit_usdt_perp_pairs(None)
        cmc_key = None
        if (cfg.get("market_cap_provider") or "").lower() == "coinmarketcap":
            cmc_key = (cfg.get("coinmarketcap_api_key") or "").strip() or os.environ.get("COINMARKETCAP_API_KEY")
        if cmc_key:
            pairs = filter_bybit_pairs_by_market_cap(pairs, 10000, cmc_api_key=cmc_key)
    else:
        phase1_mc = cfg.get("phase1_market_cap_top_n") or 300
        try:
            phase1_mc = int(phase1_mc)
        except (TypeError, ValueError):
            phase1_mc = 300
        provider = cfg.get("market_cap_provider", "coingecko")
        cmc_key = None
        if provider == "coinmarketcap":
            cmc_key = (cfg.get("coinmarketcap_api_key") or "").strip() or os.environ.get("COINMARKETCAP_API_KEY")
        pairs = fetch_bybit_usdt_perp_pairs(None)
        pairs = filter_bybit_pairs_by_market_cap(pairs, phase1_mc, cmc_api_key=cmc_key)
    return list(dict.fromkeys(pairs))


def run_queue_worker(
    config_path: Path = DEFAULT_CONFIG_PATH,
    db_path: Optional[Path] = None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    control: Optional[dict[str, bool]] = None,
    headless_override: Optional[bool] = None,
) -> None:
    """
    Process queue items in order. Uses run_state for resume.
    on_progress({queue_item_id, name, phase, status, current, total, output_xlsx_path, queue_index,
    queue_total, jobs_done_session, ...}).
    control: {"stop", "pause"} - worker checks after each pair.
    """
    if not config_path.exists():
        logger.error("Config not found: %s", config_path)
        return
    cfg = _load_config(config_path)
    output_dir = _cfg_output_dir(cfg)
    cmc_key = None
    if (cfg.get("market_cap_provider") or "").lower() == "coinmarketcap":
        cmc_key = (cfg.get("coinmarketcap_api_key") or "").strip() or os.environ.get("COINMARKETCAP_API_KEY")

    control = control or {}
    headless = headless_override if headless_override is not None else cfg.get("headless", False)
    # Jobs fully completed (or skipped) in this worker session — queue is re-fetched so adds mid-run run next.
    finished_this_run: set[int] = set()

    def _emit_prog(extra: dict[str, Any]) -> None:
        if on_progress:
            merged = dict(extra)
            merged["jobs_done_session"] = len(finished_this_run)
            on_progress(merged)

    while True:
        if control.get("stop"):
            logger.info("Stop requested, exiting worker")
            return

        items = get_queue_items(db_path)
        if not items:
            logger.info("Queue is empty")
            return

        queue_total = len(items)
        item = None
        queue_index = 0
        for idx, it in enumerate(items, start=1):
            if it["id"] not in finished_this_run:
                item = it
                queue_index = idx
                break
        if item is None:
            logger.info("All queued jobs finished this run")
            break

        qid = item["id"]
        url = (item.get("url") or "").strip()
        if not url:
            logger.warning("Skipping queue item %s: empty URL", qid)
            finished_this_run.add(qid)
            continue

        name = (item.get("name") or "").strip() or f"Chart {qid}"
        deep_backtest = bool(item.get("deep_backtest"))
        phase1_pairs = item.get("phase1_pairs") or "top300"
        export_link = (item.get("export_link") or "").strip() or None

        strategy_index = qid
        strategy = {
            "url": url,
            "name": name,
            "export_link": export_link,
            "interval": None,
        }

        # Check for resumable run
        run_state = get_run_state(qid, db_path)
        existing_xlsx: Optional[Path] = None
        pairs_override: Optional[list[str]] = None

        completed_list = []

        if run_state and run_state.get("status") in ("running", "paused"):
            existing_xlsx = _resolve_stored_xlsx_path(run_state.get("output_xlsx_path"))
            if existing_xlsx is not None:
                completed_json = run_state.get("completed_pairs_json")
                completed_list = parse_completed_pairs(completed_json)
                completed = set(_canonical_pair_label(s) for s in completed_list)
                all_pairs = _get_pairs_for_phase1(phase1_pairs, cfg)
                remaining = [p for p in all_pairs if _canonical_pair_label(p) not in completed]
                if remaining:
                    pairs_override = remaining
                    logger.info("Resuming %s from %s: %d pairs remaining", name, existing_xlsx.name, len(remaining))
                else:
                    logger.info("Resume: %s Phase 1 already complete", name)
                    upsert_run_state(
                        qid, "completed", "phase1",
                        output_xlsx_path=str(existing_xlsx),
                        current_pair_index=len(all_pairs),
                        total_pairs=len(all_pairs),
                        db_path=db_path,
                    )
                    add_run(qid, "phase1", "completed", output_xlsx_path=str(existing_xlsx), pairs_count=len(all_pairs), db_path=db_path)
                    _emit_prog({"queue_item_id": qid, "name": name, "phase": "phase1", "status": "completed", "current": len(all_pairs), "total": len(all_pairs), "output_xlsx_path": str(existing_xlsx), "queue_index": queue_index, "queue_total": queue_total})
                    phase2_ok = True
                    if deep_backtest:
                        phase2_ok = _run_phase2(cfg, qid, strategy_index, output_dir, cmc_key, _emit_prog, control, db_path, queue_index, queue_total, headless)
                    finished_this_run.add(qid)
                    if phase2_ok:
                        _maybe_auto_remove_queue_item(cfg, qid, db_path, on_progress)
                    continue
            else:
                logger.warning(
                    "Run state for queue item %s has no usable workbook (path=%r); clearing state — starting fresh.",
                    qid,
                    run_state.get("output_xlsx_path"),
                )
                clear_run_state(qid, db_path)

        # Phase 1
        phase1_mc = None if phase1_pairs == "all" else int(cfg.get("phase1_market_cap_top_n") or 300)
        all_pairs_phase1 = _get_pairs_for_phase1(phase1_pairs, cfg)

        # App closed after Phase 1 finished: DB run_state is "completed" so get_run_state() is empty,
        # but Pass 1 *.xlsx on disk still has all pairs — skip re-scanning from scratch.
        if existing_xlsx is None and pairs_override is None:
            disk_xlsx = _find_latest_pass1_xlsx(output_dir, strategy_index)
            if disk_xlsx is not None and disk_xlsx.is_file():
                try:
                    done_set, ordered_syms = read_pass1_workbook_progress(disk_xlsx)
                    remaining_disk = [p for p in all_pairs_phase1 if _canonical_pair_label(p) not in done_set]
                    if not remaining_disk:
                        logger.info(
                            "Queue item %s (%s): Pass 1 already complete on disk (%s, %d pairs) — skipping Phase 1",
                            qid,
                            name,
                            disk_xlsx.name,
                            len(all_pairs_phase1),
                        )
                        try:
                            disk_abs = str(disk_xlsx.resolve())
                        except OSError:
                            disk_abs = str(disk_xlsx)
                        # Do not call upsert_run_state/add_run here: status is often already "completed"
                        # in DB after a normal finish, and upsert would INSERT a duplicate row.
                        _emit_prog(
                            {
                                "queue_item_id": qid,
                                "name": name,
                                "phase": "phase1",
                                "status": "completed",
                                "current": len(all_pairs_phase1),
                                "total": len(all_pairs_phase1),
                                "output_xlsx_path": disk_abs,
                                "queue_index": queue_index,
                                "queue_total": queue_total,
                            }
                        )
                        phase2_ok = True
                        if deep_backtest:
                            phase2_ok = _run_phase2(
                                cfg,
                                qid,
                                strategy_index,
                                output_dir,
                                cmc_key,
                                _emit_prog,
                                control,
                                db_path,
                                queue_index,
                                queue_total,
                                headless,
                            )
                        finished_this_run.add(qid)
                        if phase2_ok:
                            _maybe_auto_remove_queue_item(cfg, qid, db_path, on_progress)
                        continue
                    logger.info(
                        "Queue item %s (%s): resuming Phase 1 from disk %s (%d pairs remaining)",
                        qid,
                        name,
                        disk_xlsx.name,
                        len(remaining_disk),
                    )
                    existing_xlsx = disk_xlsx
                    pairs_override = remaining_disk
                    completed_list = list(ordered_syms)
                except Exception as ex:
                    logger.warning("On-disk Pass 1 resume check failed for queue %s: %s", qid, ex)

        total_phase1 = len(pairs_override) if pairs_override else len(all_pairs_phase1)

        # In-memory rows for UI live table (avoids reading .xlsx while the worker is saving).
        live_rows: list[list[Any]] = []
        if existing_xlsx is not None:
            try:
                live_rows.extend(read_scan_preview_rows(existing_xlsx.resolve(), max_rows=600))
            except Exception:
                pass

        def _emit_phase1(extra: dict[str, Any]) -> None:
            payload = dict(extra)
            payload["live_preview_rows"] = sort_preview_rows_by_net_pct([list(r) for r in live_rows])
            _emit_prog(payload)

        def _on_pair(
            symbol: str,
            idx: int,
            total: int,
            filepath: Optional[Path] = None,
            metrics: Optional[dict[str, Any]] = None,
        ) -> None:
            completed_list.append(symbol)
            live_rows.append(row_values_for_metrics(symbol, metrics))
            if len(live_rows) > 400:
                del live_rows[: len(live_rows) - 400]
            if existing_xlsx is not None:
                xlsx_abs = existing_xlsx.resolve()
            elif filepath is not None:
                try:
                    xlsx_abs = filepath.resolve()
                except OSError:
                    xlsx_abs = filepath
            else:
                xlsx_abs = None
            xlsx_out = str(xlsx_abs) if xlsx_abs is not None else None
            upsert_run_state(
                qid,
                "running",
                "phase1",
                output_xlsx_path=xlsx_out,
                completed_pairs_json=serialize_completed_pairs(completed_list),
                current_pair_index=idx,
                total_pairs=total,
                db_path=db_path,
            )
            _emit_phase1(
                {
                    "queue_item_id": qid,
                    "name": name,
                    "phase": "phase1",
                    "status": "running",
                    "current": idx,
                    "total": total,
                    "output_xlsx_path": xlsx_out,
                    "queue_index": queue_index,
                    "queue_total": queue_total,
                }
            )

        try:
            # IMPORTANT: upsert must not wipe output_xlsx_path / completed_pairs_json when resuming
            # after Stop (paused). Passing None for those fields overwrites the row and restarts from 0.
            if existing_xlsx is not None:
                upsert_run_state(
                    qid,
                    "running",
                    "phase1",
                    output_xlsx_path=str(existing_xlsx),
                    completed_pairs_json=serialize_completed_pairs(completed_list),
                    current_pair_index=len(completed_list),
                    total_pairs=total_phase1,
                    db_path=db_path,
                )
            else:
                upsert_run_state(qid, "running", "phase1", db_path=db_path)
            _emit_phase1(
                {
                    "queue_item_id": qid,
                    "name": name,
                    "phase": "phase1",
                    "status": "running",
                    "current": 0,
                    "total": 0,
                    "queue_index": queue_index,
                    "queue_total": queue_total,
                }
            )

            paths = run_scan(
                strategies=[strategy],
                output_dir=output_dir,
                test_mode=False,
                max_pairs=None,
                pairs_override=pairs_override,
                strategy_index_override=strategy_index,
                existing_xlsx_path=existing_xlsx,
                headless=headless,
                **_scan_timing_kwargs(cfg),
                backtest_date_range="range_from_chart",
                pause_for_manual_login=False if headless else cfg.get("pause_for_manual_login", False),
                login_wait_seconds=int(cfg.get("login_wait_seconds", 90)),
                storage_state_path=cfg.get("storage_state_path"),
                browser_channel=cfg.get("browser_channel"),
                output_suffix="scan",
                phase1_market_cap_top_n=phase1_mc,
                market_cap_provider=cfg.get("market_cap_provider", "coingecko"),
                coinmarketcap_api_key=cmc_key,
                on_pair_complete=_on_pair,
                control=control,
            )

            xlsx_path = next((p for p in paths if p.suffix == ".xlsx"), None)
            if xlsx_path is not None:
                try:
                    xlsx_path = xlsx_path.resolve()
                except OSError:
                    pass
            if control.get("stop"):
                upsert_run_state(qid, "paused", "phase1", output_xlsx_path=str(xlsx_path) if xlsx_path else None, completed_pairs_json=serialize_completed_pairs(completed_list), current_pair_index=len(completed_list), total_pairs=total_phase1, db_path=db_path)
                _emit_phase1(
                    {
                        "queue_item_id": qid,
                        "name": name,
                        "phase": "phase1",
                        "status": "paused",
                        "current": len(completed_list),
                        "total": len(completed_list) if pairs_override else 0,
                        "output_xlsx_path": str(xlsx_path) if xlsx_path else None,
                        "queue_index": queue_index,
                        "queue_total": queue_total,
                    }
                )
                return

            upsert_run_state(qid, "completed", "phase1", output_xlsx_path=str(xlsx_path) if xlsx_path else None, current_pair_index=len(completed_list) if pairs_override else len(_get_pairs_for_phase1(phase1_pairs, cfg)), total_pairs=len(_get_pairs_for_phase1(phase1_pairs, cfg)), db_path=db_path)
            add_run(qid, "phase1", "completed", output_xlsx_path=str(xlsx_path) if xlsx_path else None, pairs_count=len(completed_list) if pairs_override else len(all_pairs_phase1), db_path=db_path)
            _emit_phase1(
                {
                    "queue_item_id": qid,
                    "name": name,
                    "phase": "phase1",
                    "status": "completed",
                    "output_xlsx_path": str(xlsx_path) if xlsx_path else None,
                    "queue_index": queue_index,
                    "queue_total": queue_total,
                }
            )

            phase2_ok = True
            if deep_backtest:
                phase2_ok = _run_phase2(cfg, qid, strategy_index, output_dir, cmc_key, _emit_prog, control, db_path, queue_index, queue_total, headless)

            finished_this_run.add(qid)
            if phase2_ok:
                _maybe_auto_remove_queue_item(cfg, qid, db_path, on_progress)

        except Exception as e:
            logger.exception("Phase 1 failed for %s: %s", name, e)
            upsert_run_state(qid, "failed", "phase1", error_message=str(e), db_path=db_path)
            add_run(qid, "phase1", "failed", db_path=db_path)
            _emit_phase1(
                {
                    "queue_item_id": qid,
                    "name": name,
                    "phase": "phase1",
                    "status": "failed",
                    "error": str(e),
                    "queue_index": queue_index,
                    "queue_total": queue_total,
                }
            )
            finished_this_run.add(qid)


def _run_phase2(
    cfg: dict,
    queue_item_id: int,
    strategy_index: int,
    output_dir: Path,
    cmc_key: Optional[str],
    on_progress: Optional[Callable[[dict[str, Any]], None]],
    control: dict[str, bool],
    db_path: Optional[Path],
    queue_index: int = 0,
    queue_total: int = 0,
    headless: bool = False,
) -> bool:
    """Run Phase 2 (deep backtest) after Phase 1. Returns False if Phase 2 failed; True if skipped or completed."""
    xlsx = _find_latest_pass1_xlsx(output_dir, strategy_index)
    if not xlsx:
        logger.warning("No Pass 1 file for strategy %d, skipping Phase 2", strategy_index)
        return True
    passed = filter_pass1_results(xlsx, min_trades=min_trades_from_config(cfg), api_key=cmc_key)
    if not passed:
        logger.info("Strategy %d: no pairs passed filters, skipping Phase 2", strategy_index)
        return True
    passed = list(dict.fromkeys(passed))
    name = ""
    for item in get_queue_items(db_path):
        if item["id"] == queue_item_id:
            name = (item.get("name") or "").strip() or f"Chart {queue_item_id}"
            break
    strategy = {"url": "", "name": name, "export_link": None, "interval": None}
    for item in get_queue_items(db_path):
        if item["id"] == queue_item_id:
            strategy = {"url": item.get("url", ""), "name": name, "export_link": item.get("export_link"), "interval": None}
            break

    phase2_total = len(passed)
    phase2_current: list[int] = [0]
    live_rows_p2: list[list[Any]] = []

    def _emit_phase2(extra: dict[str, Any]) -> None:
        if not on_progress:
            return
        payload = dict(extra)
        payload["live_preview_rows"] = sort_preview_rows_by_net_pct([list(r) for r in live_rows_p2])
        on_progress(payload)

    def _on_phase2_pair(
        sym: str,
        idx: int,
        total: int,
        filepath: Optional[Path] = None,
        metrics: Optional[dict[str, Any]] = None,
    ) -> None:
        phase2_current[0] = idx
        live_rows_p2.append(row_values_for_metrics(sym, metrics))
        if len(live_rows_p2) > 600:
            del live_rows_p2[: len(live_rows_p2) - 600]
        _emit_phase2(
            {
                "queue_item_id": queue_item_id,
                "name": name,
                "phase": "phase2",
                "status": "running",
                "current": idx,
                "total": total,
                "queue_index": queue_index,
                "queue_total": queue_total,
            }
        )

    try:
        _emit_phase2(
            {
                "queue_item_id": queue_item_id,
                "name": name,
                "phase": "phase2",
                "status": "running",
                "current": 0,
                "total": phase2_total,
                "queue_index": queue_index,
                "queue_total": queue_total,
            }
        )
        paths = run_scan(
            strategies=[strategy],
            output_dir=output_dir,
            strategy_index_override=strategy_index,
            test_mode=False,
            pairs_override=passed,
            **_scan_timing_kwargs(cfg),
            backtest_date_range="entire_history",
            headless=headless,
            pause_for_manual_login=False if headless else cfg.get("pause_for_manual_login", False),
            login_wait_seconds=int(cfg.get("login_wait_seconds", 90)),
            storage_state_path=cfg.get("storage_state_path"),
            browser_channel=cfg.get("browser_channel"),
            output_suffix="deep_scan",
            control=control,
            on_pair_complete=_on_phase2_pair,
        )
        _emit_phase2(
            {
                "queue_item_id": queue_item_id,
                "name": name,
                "phase": "phase2",
                "status": "completed",
                "output_xlsx_path": str(
                    next((p for p in paths if p.suffix == ".xlsx"), paths[0] if paths else None)
                ),
                "queue_index": queue_index,
                "queue_total": queue_total,
            }
        )
        add_run(queue_item_id, "phase2", "completed", output_xlsx_path=str(paths[0]) if paths else None, pairs_count=len(passed), db_path=db_path)
        return True
    except Exception as e:
        logger.exception("Phase 2 failed: %s", e)
        add_run(queue_item_id, "phase2", "failed", db_path=db_path)
        _emit_phase2(
            {
                "queue_item_id": queue_item_id,
                "name": name,
                "phase": "phase2",
                "status": "failed",
                "error": str(e),
                "queue_index": queue_index,
                "queue_total": queue_total,
            }
        )
        return False


def run_worker_thread(
    config_path: Path = DEFAULT_CONFIG_PATH,
    db_path: Optional[Path] = None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    control: Optional[dict[str, bool]] = None,
    headless_override: Optional[bool] = None,
) -> threading.Thread:
    """Start the queue worker in a background thread. Returns the thread."""
    control = control or {}
    thread = threading.Thread(
        target=run_queue_worker,
        kwargs={"config_path": config_path, "db_path": db_path, "on_progress": on_progress, "control": control, "headless_override": headless_override},
        daemon=True,
    )
    thread.start()
    return thread
