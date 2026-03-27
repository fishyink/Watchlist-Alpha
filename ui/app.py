"""
Flet desktop app: Watchlist (Trade-Harbour).
Tabs: Queue, Runs, Results.
"""
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
import threading
import time
import yaml
from datetime import datetime
from pathlib import Path
from typing import Optional

# Ensure project root is on path
from src.paths import ensure_config, get_app_root

PROJECT_ROOT = get_app_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import flet as ft

from src.db import (
    add_queue_item,
    delete_queue_item,
    get_queue_items,
    get_runs,
    update_queue_item,
    DEFAULT_DB_PATH,
)
from src.branding import (
    APP_DISPLAY_NAME,
    BRAND_TAGLINE,
    BRAND_TITLE,
    TRADE_HARBOUR_LINK_TEXT,
    TRADE_HARBOUR_URL,
)
from src.queue_import import apply_queue_defaults, parse_queue_import_file
from src.queue_worker import run_worker_thread
from src.tv_login import do_tradingview_login
from src.excel_writer import read_scan_preview_rows, sort_preview_rows_by_net_pct

from ui.output_parse import count_pairs_in_output_file, parse_output_path, queue_job_label

_logger = logging.getLogger(__name__)

# --- Design tokens (dark, high-contrast trading terminal aesthetic) ---
C_BG = "#070a0f"
C_SURFACE = "#0f1419"
C_SURFACE_ELEVATED = "#151c24"
C_BORDER = "#1e2a3a"
C_MUTED = "#7a8fa3"
C_TEXT = "#e8f0f7"
C_ACCENT = "#00d4aa"
C_ACCENT_DIM = "#00a884"
C_WARN = "#f0b429"
C_HTML_BADGE = "#6366f1"
# Match html_writer.py brand-banner / link colors
C_BRAND_BANNER_BG = "#0b121e"
C_LINK = "#58a6ff"
C_TAGLINE = "#8ba3b8"
C_POWERED_BY = "#8b949e"

# Container content alignment — older Flet builds lack Alignment.CENTER_LEFT / center_left
_ALIGN_CENTER_LEFT = ft.Alignment(-1.0, 0.0)
_ALIGN_CENTER = ft.Alignment(0.0, 0.0)

# Shared state for worker progress (thread-safe: worker writes, UI reads)
_worker_progress: dict = {}
_worker_control: dict = {"stop": False, "pause": False}
_worker_thread = None
# Reset when worker starts or when (queue_item_id, phase) changes — used for ETA
# baseline_cur: progress["current"] at first UI tick for this key (resume can start at 187, not 0)
_run_eta_state: dict = {"key": None, "t0": None, "baseline_cur": 0}
# Detect hung worker: same (qid, phase, current) for too long while "running"
_stall_watch: dict = {"key": None, "cur": None, "since": None}


def _format_duration_sec(sec: float) -> str:
    if sec < 0 or sec > 86400 * 14:
        return "—"
    if sec < 90:
        return f"~{int(sec)}s"
    if sec < 3600:
        return f"~{int(sec // 60)}m"
    h, r = int(sec // 3600), int((sec % 3600) // 60)
    return f"~{h}h {r}m"


def _touch_eta_state(progress: dict) -> None:
    qid = progress.get("queue_item_id")
    ph = progress.get("phase")
    key = (qid, ph)
    if _run_eta_state.get("key") != key:
        _run_eta_state["key"] = key
        _run_eta_state["t0"] = time.time()
        _run_eta_state["baseline_cur"] = int(progress.get("current") or 0)


def _compute_step_eta_line(progress: dict) -> str:
    """Rough ETA for current phase step from pair throughput."""
    status = progress.get("status") or ""
    if status != "running":
        return ""
    cur = int(progress.get("current") or 0)
    tot = int(progress.get("total") or 0)
    if tot <= 0:
        return "This step: warming up…"
    _touch_eta_state(progress)
    elapsed = max(0.0, time.time() - float(_run_eta_state["t0"] or time.time()))
    base = int(_run_eta_state.get("baseline_cur") or 0)
    delta = cur - base
    if cur <= 0:
        return "This step: estimating pace…"
    if delta <= 0:
        if elapsed < 90:
            return "This step: estimating pace… (resumed mid-run — ETA after next pair)"
        return "This step: no progress — if this persists, TV may be stuck; try Stop or non-headless."
    rate = elapsed / float(delta)
    rem = max(0, tot - cur)
    return f"This step ETA: {_format_duration_sec(rem * rate)} (rough)"


def _compute_batch_eta_line(progress: dict) -> str:
    """Very rough ETA for remaining jobs in the current queue list + rest of this step."""
    status = progress.get("status") or ""
    if status != "running":
        return ""
    cur = int(progress.get("current") or 0)
    tot = int(progress.get("total") or 0)
    qidx = int(progress.get("queue_index") or 0)
    qtot = int(progress.get("queue_total") or 0)
    after_this = max(0, qtot - qidx)
    if tot <= 0 or cur <= 0:
        return ""
    _touch_eta_state(progress)
    elapsed = max(0.0, time.time() - float(_run_eta_state["t0"] or time.time()))
    base = int(_run_eta_state.get("baseline_cur") or 0)
    delta = cur - base
    if delta <= 0:
        return ""
    pair_rate = elapsed / float(delta)
    rem_pairs = max(0, tot - cur)
    rem_current = rem_pairs * pair_rate
    # Assume later jobs are similar size to this step's total (crude).
    future_jobs = after_this * (pair_rate * float(tot))
    total_guess = rem_current + future_jobs
    if total_guess <= 0:
        return ""
    return f"All listed jobs (very rough): {_format_duration_sec(total_guess)}"


def _avg_time_per_pair_line(progress: dict) -> str:
    """Rolling average since UI started this step (delta pairs), so resume mid-run isn't ~0s."""
    status = (progress.get("status") or "").strip()
    if status not in ("running", "paused"):
        return ""
    cur = int(progress.get("current") or 0)
    tot = int(progress.get("total") or 0)
    if cur <= 0:
        return "Avg time per pair: — (after the first pair finishes)"
    _touch_eta_state(progress)
    t0 = float(_run_eta_state["t0"] or time.time())
    elapsed = max(0.0, time.time() - t0)
    base = int(_run_eta_state.get("baseline_cur") or 0)
    delta = cur - base
    tot_s = str(tot) if tot > 0 else "?"
    if delta <= 0:
        return (
            f"Avg time per pair (this step): —  ·  resumed at {cur}/{tot_s} — "
            f"pace shows after the next pair completes"
        )
    sec_per = elapsed / float(delta)
    if sec_per < 120:
        pace = f"~{sec_per:.1f}s"
    else:
        pace = _format_duration_sec(sec_per)
    return f"Avg time per pair (this step): {pace}  ·  based on {cur}/{tot_s} pairs completed ({delta} since UI tab refresh)"


def _reset_eta_state() -> None:
    _run_eta_state["key"] = None
    _run_eta_state["t0"] = None
    _run_eta_state["baseline_cur"] = 0
    _stall_watch["key"] = None
    _stall_watch["cur"] = None
    _stall_watch["since"] = None


def _stall_warning_line(progress: dict) -> str:
    """If pair counter stops increasing while running, warn (headless TV hang, login wall, etc.)."""
    if not progress or (progress.get("status") or "") != "running":
        _stall_watch["key"] = None
        return ""
    qid = progress.get("queue_item_id")
    ph = progress.get("phase")
    cur = int(progress.get("current") or 0)
    key = (qid, ph)
    now = time.time()
    if _stall_watch.get("key") != key:
        _stall_watch["key"] = key
        _stall_watch["cur"] = cur
        _stall_watch["since"] = now
        return ""
    if cur != _stall_watch.get("cur"):
        _stall_watch["cur"] = cur
        _stall_watch["since"] = now
        return ""
    stuck = now - float(_stall_watch.get("since") or now)
    if stuck < 180:
        return ""
    m = int(stuck // 60)
    return (
        f"⚠ No progress for {m}m at {cur} pairs — scanner may be stuck (TradingView loading, login, or symbol change). "
        f"Try Stop → turn headless OFF → Start to see the browser, or re-login from the header."
    )


def _open_path(path: Path) -> None:
    if not path.exists():
        return
    if sys.platform == "win32":
        os.startfile(str(path))
    elif sys.platform == "darwin":
        subprocess.run(["open", str(path)], check=False)
    else:
        subprocess.run(["xdg-open", str(path)], check=False)


def _open_output_prefer_html(path: Path) -> None:
    """Open the HTML report if present for this stem; otherwise .xlsx or the file clicked."""
    stem = path.stem
    parent = path.parent
    html_path = parent / f"{stem}.html"
    htm_path = parent / f"{stem}.htm"
    xlsx_path = parent / f"{stem}.xlsx"
    if html_path.exists():
        _open_path(html_path)
    elif htm_path.exists():
        _open_path(htm_path)
    elif xlsx_path.exists():
        _open_path(xlsx_path)
    elif path.exists():
        _open_path(path)


def _open_containing_folder(path: Path) -> None:
    if not path.exists():
        return
    if sys.platform == "win32":
        subprocess.run(["explorer", "/select,", str(path.resolve())], check=False)
    elif sys.platform == "darwin":
        # Reveal file in Finder
        subprocess.run(["open", "-R", str(path.resolve())], check=False)
    else:
        subprocess.run(["xdg-open", str(path.parent)], check=False)


def _delete_scan_outputs(primary: Path) -> tuple[bool, str]:
    """Delete this output and paired .xlsx/.html with the same stem (same scan)."""
    stem = primary.stem
    parent = primary.parent
    targets = [parent / f"{stem}.xlsx", parent / f"{stem}.html"]
    try:
        removed = False
        for p in targets:
            if p.exists():
                p.unlink()
                removed = True
        if not removed:
            return False, "File not found"
        return True, ""
    except OSError as ex:
        return False, str(ex)


def _copy_text(page: ft.Page, text: str) -> None:
    try:
        if hasattr(page, "set_clipboard") and callable(getattr(page, "set_clipboard")):
            page.set_clipboard(text)
        else:
            import tkinter as tk

            r = tk.Tk()
            r.withdraw()
            r.clipboard_clear()
            r.clipboard_append(text)
            r.update()
            r.destroy()
        page.snack_bar = ft.SnackBar(ft.Text("Copied to clipboard"), bgcolor=C_SURFACE_ELEVATED, open=True)
    except Exception:
        page.snack_bar = ft.SnackBar(ft.Text("Could not copy"), open=True)
    page.update()


def _open_trade_harbour(page: ft.Page) -> None:
    try:
        if hasattr(page, "launch_url") and callable(getattr(page, "launch_url")):
            page.launch_url(TRADE_HARBOUR_URL)
        else:
            import webbrowser

            webbrowser.open(TRADE_HARBOUR_URL)
    except Exception:
        import webbrowser

        webbrowser.open(TRADE_HARBOUR_URL)


def _badge(text: str, *, color: str = C_ACCENT_DIM, fg: str = C_TEXT) -> ft.Container:
    return ft.Container(
        content=ft.Text(text, size=11, weight=ft.FontWeight.W_600, color=fg),
        padding=ft.padding.symmetric(horizontal=10, vertical=4),
        bgcolor=color,
        border_radius=6,
    )


def main(page: ft.Page) -> None:
    page.title = APP_DISPLAY_NAME
    page.window.min_width = 960
    page.window.min_height = 640
    page.padding = 0
    page.theme_mode = ft.ThemeMode.DARK
    page.bgcolor = C_BG
    page.theme = ft.Theme(
        color_scheme_seed=C_ACCENT,
        use_material3=True,
    )
    page.dark_theme = page.theme

    ensure_config()
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Desktop window: maximized = fill screen (taskbar visible); fullscreen = borderless; normal = default size
    _wstart = "maximized"
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                _cfg_win = yaml.safe_load(f) or {}
            _wstart = str((_cfg_win.get("ui") or {}).get("window_start") or "maximized").strip().lower()
        except Exception:
            pass
    if _wstart == "fullscreen":
        page.window.full_screen = True
    elif _wstart == "maximized":
        page.window.maximized = True

    # --- Queue tab ---
    # No expand here: queue tab outer Column scrolls so the list is never clipped below the add form.
    queue_list = ft.Column(spacing=10)
    queue_count_label = ft.Text("Queued jobs (0)", size=16, weight=ft.FontWeight.BOLD, color=C_ACCENT)
    queue_running_banner = ft.Text("", size=13, weight=ft.FontWeight.W_500, color=C_ACCENT, visible=False)

    add_url = ft.TextField(
        label="TradingView URL",
        value="https://www.tradingview.com/chart/",
        expand=True,
        border_color=C_BORDER,
        focused_border_color=C_ACCENT,
        bgcolor=C_SURFACE,
    )
    add_name = ft.TextField(
        label="Name (optional)",
        width=200,
        border_color=C_BORDER,
        focused_border_color=C_ACCENT,
        bgcolor=C_SURFACE,
    )
    add_export = ft.TextField(
        label="dtech link (optional)",
        expand=True,
        border_color=C_BORDER,
        focused_border_color=C_ACCENT,
        bgcolor=C_SURFACE,
    )
    add_deep = ft.Checkbox(label="Deep backtest after Phase 1", value=False)
    add_pairs = ft.Dropdown(
        label="Phase 1 pairs",
        value="all",
        width=180,
        options=[
            ft.dropdown.Option("top300", "Top 300 market cap"),
            ft.dropdown.Option("all", "All pairs (~500)"),
        ],
        border_color=C_BORDER,
        focused_border_color=C_ACCENT,
        bgcolor=C_SURFACE,
    )

    def build_queue_item_row(item: dict, *, running_qid: int | None = None) -> ft.Control:
        qid = item["id"]
        url = (item.get("url") or "").strip()
        name = (item.get("name") or "").strip() or f"Chart {qid}"
        export_link = (item.get("export_link") or "").strip() or ""
        deep = bool(item.get("deep_backtest"))
        phase1 = item.get("phase1_pairs") or "top300"
        is_running = running_qid is not None and qid == running_qid

        run_badge = ft.Container(
            content=ft.Text("RUNNING", size=10, weight=ft.FontWeight.BOLD, color=C_BG),
            bgcolor=C_ACCENT,
            padding=ft.padding.symmetric(horizontal=8, vertical=5),
            border_radius=4,
            visible=is_running,
        )
        badge_slot = ft.Container(width=88 if is_running else 0, content=run_badge, alignment=_ALIGN_CENTER)

        tf_name = ft.TextField(
            label="Name",
            value=name,
            width=160,
            dense=True,
            border_color=C_BORDER,
            focused_border_color=C_ACCENT,
            bgcolor=C_SURFACE,
            on_change=lambda e, i=qid: _save_queue_name(i, e.control.value),
        )
        tf_url = ft.TextField(
            label="TradingView URL",
            value=url,
            expand=True,
            dense=True,
            border_color=C_BORDER,
            focused_border_color=C_ACCENT,
            bgcolor=C_SURFACE,
            on_change=lambda e, i=qid: _save_queue_url(i, e.control.value),
        )
        tf_export = ft.TextField(
            label="dtech link (optional)",
            value=export_link,
            hint_text="Optional export / dtech URL",
            expand=True,
            dense=True,
            border_color=C_BORDER,
            focused_border_color=C_ACCENT,
            bgcolor=C_SURFACE,
            on_change=lambda e, i=qid: _save_queue_export(i, e.control.value),
        )
        cb_deep = ft.Checkbox(label="Deep backtest", value=deep, on_change=lambda e, i=qid: _save_queue_deep(i, e.control.value))
        # Not dense: gives the floating label + menu room so it is not clipped inside the card
        dd_pairs = ft.Dropdown(
            label="Phase 1 pairs",
            value=phase1,
            width=220,
            border_color=C_BORDER,
            focused_border_color=C_ACCENT,
            bgcolor=C_SURFACE,
            options=[ft.dropdown.Option("top300", "Top 300 market cap"), ft.dropdown.Option("all", "All pairs (~500)")],
            on_change=lambda e, i=qid: _save_queue_pairs(i, e.control.value),
        )

        def delete_click(e):
            delete_queue_item(qid)
            refresh_queue_tab()

        delete_btn = ft.IconButton(
            ft.Icons.DELETE_OUTLINE,
            on_click=delete_click,
            tooltip="Remove from queue",
            icon_color=C_MUTED,
            style=ft.ButtonStyle(padding=10),
        )

        border_col = C_ACCENT if is_running else C_BORDER
        border_w = 2 if is_running else 1

        # Three rows: identity + URL + delete | full-width dtech | options (no crowding / clipping)
        row_header = ft.Row(
            [
                badge_slot,
                tf_name,
                tf_url,
                delete_btn,
            ],
            spacing=12,
            vertical_alignment=ft.CrossAxisAlignment.START,
        )
        row_dtech = ft.Row([tf_export], spacing=0, vertical_alignment=ft.CrossAxisAlignment.START)
        row_options = ft.Row(
            [
                cb_deep,
                dd_pairs,
            ],
            spacing=20,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        )

        return ft.Container(
            bgcolor=C_SURFACE_ELEVATED,
            border_radius=12,
            border=ft.border.all(border_w, border_col),
            clip_behavior=ft.ClipBehavior.NONE,
            padding=ft.padding.symmetric(horizontal=16, vertical=14),
            margin=ft.margin.only(bottom=4),
            content=ft.Column(
                [
                    row_header,
                    ft.Container(height=4),
                    row_dtech,
                    ft.Container(height=6),
                    row_options,
                    ft.Container(height=4),
                ],
                spacing=0,
                tight=True,
            ),
        )

    def _save_queue_url(qid: int, val: str):
        update_queue_item(qid, url=val)

    def _save_queue_name(qid: int, val: str):
        update_queue_item(qid, name=val)

    def _save_queue_export(qid: int, val: str):
        update_queue_item(qid, export_link=val or None)

    def _save_queue_deep(qid: int, val: bool):
        update_queue_item(qid, deep_backtest=val)

    def _save_queue_pairs(qid: int, val: str):
        update_queue_item(qid, phase1_pairs=val)

    def refresh_queue_tab():
        global _worker_thread
        items = get_queue_items()
        queue_count_label.value = f"Queued jobs ({len(items)})"
        running_qid = None
        if _worker_thread and _worker_thread.is_alive() and (_worker_progress.get("status") == "running"):
            running_qid = _worker_progress.get("queue_item_id")
        queue_list.controls.clear()
        for item in items:
            queue_list.controls.append(build_queue_item_row(item, running_qid=running_qid))
        page.update()

    def add_to_queue_click(e):
        url = (add_url.value or "").strip()
        if not url:
            page.snack_bar = ft.SnackBar(ft.Text("Enter a TradingView chart URL."), open=True)
            page.update()
            return
        ph = (add_pairs.value or "all").strip()
        if ph not in ("all", "top300"):
            ph = "all"
        add_queue_item(
            url,
            name=(add_name.value or "").strip(),
            export_link=(add_export.value or "").strip() or None,
            deep_backtest=bool(add_deep.value),
            phase1_pairs=ph,
        )
        add_name.value = ""
        add_export.value = ""
        page.snack_bar = ft.SnackBar(ft.Text("Added to queue"), bgcolor=C_SURFACE_ELEVATED, open=True)
        refresh_queue_tab()

    def import_config_click(e):
        if not config_path.exists():
            return
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        ph = (add_pairs.value or "all").strip()
        if ph not in ("all", "top300"):
            ph = "all"
        for s in cfg.get("strategies", []):
            url = (s.get("url") or "").strip()
            if not url:
                continue
            add_queue_item(
                url,
                name=(s.get("name") or "").strip() or None,
                export_link=(s.get("export_link") or "").strip() or None,
                deep_backtest=bool(add_deep.value),
                phase1_pairs=ph,
                db_path=DEFAULT_DB_PATH,
            )
        refresh_queue_tab()

    MAX_QUEUE_IMPORT = 400

    def on_queue_import_result(e: ft.FilePickerResultEvent):
        if not e.files:
            return
        f0 = e.files[0]
        fpath = getattr(f0, "path", None) or str(getattr(f0, "name", "") or "")
        if not fpath:
            return
        try:
            entries = parse_queue_import_file(fpath)
        except OSError as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Could not read file: {ex}"), open=True)
            page.update()
            return
        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Invalid file: {ex}"), open=True)
            page.update()
            return
        if not entries:
            page.snack_bar = ft.SnackBar(ft.Text("No valid chart URLs found."), open=True)
            page.update()
            return
        ph = (add_pairs.value or "all").strip()
        if ph not in ("all", "top300"):
            ph = "all"
        entries = apply_queue_defaults(
            entries,
            default_deep=bool(add_deep.value),
            default_phase1_pairs=ph,
        )
        truncated = False
        if len(entries) > MAX_QUEUE_IMPORT:
            entries = entries[:MAX_QUEUE_IMPORT]
            truncated = True
        for row in entries:
            add_queue_item(
                row["url"],
                name=(row.get("name") or "").strip(),
                export_link=row.get("export_link"),
                deep_backtest=bool(row.get("deep_backtest")),
                phase1_pairs=row.get("phase1_pairs") or ph,
                db_path=DEFAULT_DB_PATH,
            )
        msg = f"Imported {len(entries)} job(s) from file."
        if truncated:
            msg += f" (max {MAX_QUEUE_IMPORT} per import)"
        page.snack_bar = ft.SnackBar(ft.Text(msg), bgcolor=C_SURFACE_ELEVATED, open=True)
        refresh_queue_tab()

    queue_import_picker = ft.FilePicker(on_result=on_queue_import_result)

    def pick_queue_import_file(_):
        queue_import_picker.pick_files(
            dialog_title="Import queue — .txt or .csv",
            allowed_extensions=["txt", "csv", "tsv"],
            file_type=ft.FilePickerFileType.CUSTOM,
        )

    add_to_queue_card = ft.Card(
        elevation=0,
        color=C_SURFACE_ELEVATED,
        content=ft.Container(
            content=ft.Column(
                [
                    ft.Text("Add to queue", size=16, weight=ft.FontWeight.W_600, color=C_TEXT),
                    ft.Text(
                        "Fill in a chart, then Add to queue. You can add more anytime — even while a run is going; new jobs are picked up after the current one finishes. "
                        "Bulk: Import from file — .txt (one URL per line, or name|url); .csv with url column; or two-column Sheets export (TV chart URL, dtech link, no header). "
                        "Deep backtest / Phase 1 pairs follow the controls above (default: all pairs, no deep) unless a CSV column overrides them.",
                        size=12,
                        color=C_MUTED,
                    ),
                    ft.Container(height=8),
                    add_url,
                    ft.Row([add_name, add_export], spacing=12),
                    ft.Row(
                        [
                            add_deep,
                            add_pairs,
                            ft.ElevatedButton(
                                "Add to queue",
                                icon=ft.Icons.ADD,
                                on_click=add_to_queue_click,
                                style=ft.ButtonStyle(bgcolor={ft.ControlState.DEFAULT: C_ACCENT}, color={ft.ControlState.DEFAULT: C_BG}),
                            ),
                            ft.OutlinedButton("Import from config", icon=ft.Icons.UPLOAD, on_click=import_config_click),
                            ft.OutlinedButton("Import from file…", icon=ft.Icons.FILE_OPEN, on_click=pick_queue_import_file),
                        ],
                        spacing=12,
                        wrap=True,
                    ),
                ],
                spacing=8,
            ),
            padding=16,
            border=ft.border.all(1, C_BORDER),
            border_radius=12,
        ),
    )

    queue_tab = ft.Container(
        expand=True,
        content=ft.Column(
            [
                ft.Column(
                    [
                        ft.Text("Scan queue", size=22, weight=ft.FontWeight.BOLD, color=C_TEXT),
                        ft.Text(
                            "Start a run from the Runs tab. With queue.auto_remove_on_success (default true in config), each row disappears when that job finishes OK — outputs remain in output/. Set false to keep finished rows until you trash them.",
                            size=13,
                            color=C_MUTED,
                        ),
                    ],
                    spacing=4,
                ),
                ft.Container(height=12),
                add_to_queue_card,
                ft.Container(height=16),
                queue_count_label,
                queue_running_banner,
                ft.Text("Queued jobs list — compact rows; RUNNING highlights the active job.", size=12, weight=ft.FontWeight.W_500, color=C_MUTED),
                ft.Container(height=6),
                queue_list,
                ft.Container(height=16),
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
    )

    # --- Runs tab ---
    runs_list = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
    runs_status = ft.Text("Idle", size=14, weight=ft.FontWeight.W_500, color=C_MUTED)
    progress_bar = ft.ProgressBar(value=0, bar_height=10)
    progress_text = ft.Text("0 / 0 pairs completed", size=14, color=C_TEXT)
    progress_chart_name = ft.Text("--", size=14, color=C_TEXT)
    progress_phase_status = ft.Text("--", size=14, color=C_MUTED)
    progress_error = ft.Text("", color=ft.Colors.RED, size=12)
    runs_stat_line1 = ft.Text("", size=12, color=C_MUTED)
    runs_stat_line2 = ft.Text("", size=12, color=C_MUTED)
    runs_stat_line3 = ft.Text("", size=12, color=C_MUTED)
    runs_stat_line4 = ft.Text("", size=12, color=C_WARN, weight=ft.FontWeight.W_500)
    progress_card = ft.Card(
        elevation=0,
        color=C_SURFACE_ELEVATED,
        visible=False,
        content=ft.Container(
            expand=True,
            content=ft.Column(
                [
                    ft.Text("Current run", weight=ft.FontWeight.BOLD, size=16, color=C_ACCENT),
                    runs_stat_line1,
                    runs_stat_line2,
                    runs_stat_line3,
                    runs_stat_line4,
                    progress_chart_name,
                    progress_phase_status,
                    ft.Container(content=progress_bar, expand=True, height=14),
                    progress_text,
                    progress_error,
                ],
                spacing=6,
                expand=True,
            ),
            padding=20,
            border=ft.border.all(1, C_BORDER),
            border_radius=12,
        ),
    )

    # Live spreadsheet preview (Runs tab): updates while worker is running; HTML is built at step end.
    live_preview_path_holder: dict = {"path": None}
    # Row indices match DATA_COLUMNS: 0 Sym, 1 Net $, 2 Net %, 6 Max DD %, …
    LIVE_PREVIEW_COL_IDX = (0, 1, 2, 6, 7, 8, 9, 10, 11)
    LIVE_PREVIEW_HDRS = ("Symbol", "Net $", "Net %", "Max DD %", "Sharpe", "Sortino", "Win %", "#", "PF")
    # expand flex so the table uses full card width (not clipped — "—" is empty data, see subtitle)
    LIVE_PREVIEW_FLEX = (4, 2, 2, 2, 2, 2, 2, 1, 2)
    # Cap rows in the scroll area (sorted by Net %). Worker keeps a slightly larger buffer in memory.
    LIVE_PREVIEW_MAX_ROWS = 500

    def _live_hdr_cell(text: str, flex: int) -> ft.Container:
        return ft.Container(
            expand=flex,
            content=ft.Text(text, size=11, weight=ft.FontWeight.W_600, color=C_ACCENT),
            padding=ft.padding.only(right=6),
        )

    def _live_data_cell(text: str, flex: int, color: str) -> ft.Container:
        return ft.Container(
            expand=flex,
            content=ft.Text(
                text,
                size=11,
                color=color,
                max_lines=1,
                overflow=ft.TextOverflow.ELLIPSIS,
            ),
            padding=ft.padding.only(right=6),
            alignment=_ALIGN_CENTER_LEFT,
        )

    def _fmt_live_cell(v) -> str:
        if v is None or v == "":
            return "—"
        if isinstance(v, float):
            if v != v:
                return "—"
            s = f"{v:.2f}".rstrip("0").rstrip(".")
            return s or "0"
        if isinstance(v, int):
            return str(v)
        s = str(v).strip()
        return s if s else "—"

    def _live_cell_color(col_idx: int, val) -> str:
        """Green/red for Net $ and Net % columns."""
        if col_idx not in (1, 2):
            return C_TEXT
        try:
            if val is None or str(val).strip() == "":
                return C_TEXT
            fv = float(val)
            if fv > 0:
                return "#3fb950"
            if fv < 0:
                return "#f85149"
        except (TypeError, ValueError):
            pass
        return C_TEXT

    live_results_body = ft.Column(spacing=2, tight=True)

    def _open_live_xlsx_click(e):
        p = live_preview_path_holder.get("path")
        if p:
            pp = Path(p)
            if not pp.is_absolute():
                pp = (PROJECT_ROOT / pp).resolve()
            else:
                pp = pp.resolve()
            _open_path(pp)

    btn_open_live_xlsx = ft.OutlinedButton(
        "Open spreadsheet",
        icon=ft.Icons.TABLE_CHART,
        on_click=_open_live_xlsx_click,
        style=ft.ButtonStyle(color={ft.ControlState.DEFAULT: C_ACCENT}),
    )

    live_pair_pace_text = ft.Text("", size=12, weight=ft.FontWeight.W_500, color=C_ACCENT)

    live_results_card = ft.Card(
        elevation=0,
        visible=False,
        color=C_SURFACE_ELEVATED,
        content=ft.Container(
            padding=16,
            border=ft.border.all(1, C_BORDER),
            border_radius=12,
            expand=True,
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Column(
                                [
                                    ft.Text("Live results preview", weight=ft.FontWeight.BOLD, size=15, color=C_TEXT),
                                    ft.Text(
                                        "Live from the scanner (same numbers as the .xlsx). "
                                        "“—” means no value parsed yet — not a width limit. "
                                        f"HTML is written when the step completes. Table shows up to {LIVE_PREVIEW_MAX_ROWS} rows (best Net % first).",
                                        size=11,
                                        color=C_MUTED,
                                    ),
                                ],
                                expand=True,
                                spacing=4,
                            ),
                            btn_open_live_xlsx,
                        ],
                        vertical_alignment=ft.CrossAxisAlignment.START,
                    ),
                    live_pair_pace_text,
                    ft.Container(height=8),
                    ft.Row(
                        [_live_hdr_cell(h, f) for h, f in zip(LIVE_PREVIEW_HDRS, LIVE_PREVIEW_FLEX)],
                        spacing=0,
                        expand=True,
                    ),
                    ft.Container(height=6),
                    ft.Container(
                        expand=True,
                        height=280,
                        border=ft.border.all(1, C_BORDER),
                        border_radius=8,
                        padding=ft.padding.symmetric(horizontal=10, vertical=8),
                        content=ft.Column([live_results_body], expand=True, scroll=ft.ScrollMode.AUTO),
                    ),
                ],
                spacing=0,
                tight=True,
                expand=True,
            ),
        ),
    )

    # Only the run-history list is rebuilt on refresh. Never clear() progress_card / live_results_card from
    # runs_list — Flet drops server-side control ids (__uid) when removed; re-append causes
    # AssertionError in build_update_commands and kills _poll_progress (UI looks frozen).
    runs_history_column = ft.Column(spacing=0)
    runs_list.controls = [
        ft.Container(content=progress_card, expand=True),
        live_results_card,
        runs_history_column,
        ft.Container(expand=True),
    ]

    def _update_live_run_table(progress: dict):
        live_preview_path_holder["path"] = None
        alive = _worker_thread and _worker_thread.is_alive()
        st = (progress or {}).get("status")
        show = bool(alive and progress and st in ("running", "paused"))
        if not show:
            live_results_card.visible = False
            live_results_body.controls.clear()
            live_pair_pace_text.value = ""
            return
        live_results_card.visible = True
        live_pair_pace_text.value = _avg_time_per_pair_line(progress)
        ox = progress.get("output_xlsx_path") or ""
        if not str(ox).strip():
            live_results_body.controls.clear()
            live_results_body.controls.append(ft.Text("Waiting for spreadsheet…", size=12, color=C_MUTED))
            btn_open_live_xlsx.disabled = True
            return
        live_preview_path_holder["path"] = str(ox)
        pth = Path(str(ox))
        if not pth.is_absolute():
            pth = (PROJECT_ROOT / pth).resolve()
        else:
            pth = pth.resolve()
        if not pth.is_file():
            live_results_body.controls.clear()
            live_results_body.controls.append(ft.Text("Spreadsheet not created yet…", size=12, color=C_MUTED))
            btn_open_live_xlsx.disabled = True
            return
        btn_open_live_xlsx.disabled = False
        mem = progress.get("live_preview_rows")
        if isinstance(mem, list) and len(mem) > 0:
            rows = sort_preview_rows_by_net_pct([list(r) for r in mem])[:LIVE_PREVIEW_MAX_ROWS]
        else:
            rows = read_scan_preview_rows(pth, max_rows=LIVE_PREVIEW_MAX_ROWS)
        live_results_body.controls.clear()
        if not rows:
            live_results_body.controls.append(
                ft.Text("No rows yet, or file is locked — retrying on next refresh.", size=12, color=C_MUTED)
            )
            return
        for row in rows:
            cells = []
            for col_idx, flex in zip(LIVE_PREVIEW_COL_IDX, LIVE_PREVIEW_FLEX):
                val = row[col_idx] if col_idx < len(row) else None
                txt = _fmt_live_cell(val)
                color = _live_cell_color(col_idx, val)
                cells.append(_live_data_cell(txt, flex, color))
            live_results_body.controls.append(ft.Row(cells, spacing=0, expand=True))

    btn_start = ft.ElevatedButton("Start", icon=ft.Icons.PLAY_ARROW, on_click=lambda e: _start_worker())
    btn_stop = ft.ElevatedButton("Stop", icon=ft.Icons.STOP, on_click=lambda e: _stop_worker())
    btn_tv_login = ft.OutlinedButton(
        "Step 1 — Log into TradingView",
        icon=ft.Icons.LOGIN,
        on_click=lambda e: _do_login(),
    )
    login_hint_90s = ft.Text(
        "You have 90 seconds to log in after the browser opens.",
        size=11,
        color=C_MUTED,
    )
    headless_switch = ft.Switch(label="Run headless (no browser window)", value=True)
    login_status = ft.Text("", size=11, color=C_MUTED)

    def _load_login_status():
        cfg = {}
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
        path = cfg.get("storage_state_path") or "config/tv_session.json"
        p = PROJECT_ROOT / path
        return "Session saved" if p.exists() else "Not logged in"

    def _do_login():
        btn_tv_login.disabled = True
        login_status.value = "Opening Chrome… you have 90 seconds to log in."
        page.update()

        def _run_login():
            cfg = {}
            if config_path.exists():
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f) or {}
            path = str(PROJECT_ROOT / (cfg.get("storage_state_path") or "config/tv_session.json"))
            ok = do_tradingview_login(path, login_wait_seconds=90, browser_channel=cfg.get("browser_channel") or "chrome")
            btn_tv_login.disabled = False
            login_status.value = "Session saved — you can run headless from the Runs tab." if ok else "Login timed out — try Step 1 again."
            page.update()

        t = threading.Thread(target=_run_login, daemon=True)
        t.start()

    def refresh_runs_tab(e=None):
        global _worker_thread
        progress = _worker_progress.copy()
        current = progress.get("current", 0) or 0
        total = progress.get("total", 0) or 0

        if progress:
            progress_card.visible = True
            progress_bar.value = (current / total) if total > 0 else 0
            if total > 0:
                pct = int(100 * current / total)
                progress_text.value = f"{current} / {total} pairs completed ({pct}%)"
            else:
                progress_text.value = f"{current} / {total} pairs completed"
            name = progress.get("name", "?")
            phase = progress.get("phase", "?")
            status = progress.get("status", "?")
            qidx = int(progress.get("queue_index") or 0)
            qtotal = int(progress.get("queue_total") or 0)
            queue_label = f" (Chart {qidx} of {qtotal})" if qtotal > 0 else ""
            progress_chart_name.value = f"{name}{queue_label}"
            progress_phase_status.value = f"{phase} — {status}"
            progress_error.value = progress.get("error", "") or ""

            jdone = int(progress.get("jobs_done_session") or 0)
            after_this = max(0, qtotal - qidx)
            runs_stat_line1.value = (
                f"Queue list: job {qidx} of {qtotal} · {after_this} job(s) listed after this one · "
                f"finished earlier this run: {jdone}"
            )
            runs_stat_line2.value = _compute_step_eta_line(progress)
            runs_stat_line3.value = _compute_batch_eta_line(progress)
            sw = _stall_warning_line(progress)
            runs_stat_line4.value = sw
            runs_stat_line4.visible = bool(sw)
        else:
            progress_card.visible = False
            runs_stat_line1.value = ""
            runs_stat_line2.value = ""
            runs_stat_line3.value = ""
            runs_stat_line4.value = ""
            runs_stat_line4.visible = False
            _stall_watch["key"] = None

        # Queue tab: show which job is active (updated every poll without rebuilding queue rows)
        if _worker_thread and _worker_thread.is_alive() and progress and progress.get("status") == "running":
            qn = progress.get("name") or "?"
            qid = progress.get("queue_item_id")
            auto_rm = False
            try:
                if config_path.exists():
                    with open(config_path, "r", encoding="utf-8") as f:
                        _cfg_banner = yaml.safe_load(f) or {}
                    auto_rm = bool((_cfg_banner.get("queue") or {}).get("auto_remove_on_success", False))
            except Exception:
                auto_rm = False
            if auto_rm:
                rm_hint = "This row is removed when the job completes successfully (scan files stay in output/)."
            else:
                rm_hint = "Finished jobs stay until you use the trash icon, or set queue.auto_remove_on_success: true in config.yaml."
            queue_running_banner.value = f"▶ Now running: {qn} (queue id {qid}). {rm_hint}"
            queue_running_banner.visible = True
        else:
            queue_running_banner.visible = False
            queue_running_banner.value = ""

        _update_live_run_table(progress)

        runs_history_column.controls.clear()
        for run in get_runs(limit=30):
            name = run.get("name") or run.get("url", "")[:50]
            phase = run.get("phase", "")
            status = run.get("status", "")
            path = run.get("output_xlsx_path") or run.get("output_html_path") or ""
            finished = run.get("finished_at", "")

            def open_file(e, p=path):
                if p:
                    _open_output_prefer_html(Path(p))

            row = ft.ListTile(
                title=ft.Text(f"{name} ({phase})", color=C_TEXT),
                subtitle=ft.Text(f"{status} — {finished[:19] if finished else ''}", color=C_MUTED, size=12),
                trailing=ft.IconButton(ft.Icons.OPEN_IN_NEW, on_click=open_file, tooltip="Open HTML if available", icon_color=C_ACCENT) if path else None,
            )
            runs_history_column.controls.append(
                ft.Card(
                    elevation=0,
                    color=C_SURFACE_ELEVATED,
                    content=ft.Container(content=row, padding=4, border=ft.border.all(1, C_BORDER), border_radius=10),
                )
            )

        if _worker_thread and not _worker_thread.is_alive():
            progress_card.visible = False
            runs_status.value = "Idle"
            btn_start.disabled = False

        if _worker_progress.pop("queue_auto_refresh", None):
            refresh_queue_tab()

        try:
            page.update()
        except AssertionError:
            _logger.debug("page.update skipped (Flet control tree)", exc_info=True)
        except RuntimeError as ex:
            if "event loop is closed" in str(ex).lower():
                _logger.debug("page.update skipped: %s", ex)
            else:
                raise

    async def _poll_progress():
        while _worker_thread and _worker_thread.is_alive():
            try:
                refresh_runs_tab()
            except AssertionError:
                _logger.warning("Runs tab refresh failed (Flet); stopping progress poll.", exc_info=True)
                break
            except RuntimeError as ex:
                if "event loop is closed" in str(ex).lower():
                    break
                raise
            await asyncio.sleep(1)
        try:
            refresh_runs_tab()
        except Exception:
            pass

    def _start_worker():
        global _worker_thread, _worker_control
        if headless_switch.value:
            if _load_login_status() == "Not logged in":
                page.snack_bar = ft.SnackBar(ft.Text("Log in first (click Login to TradingView) before running headless."), open=True)
                page.update()
                return
        _worker_control["stop"] = False
        _worker_progress.clear()
        _reset_eta_state()

        def on_progress(p: dict):
            _worker_progress.update(p)

        _worker_thread = run_worker_thread(
            config_path=config_path,
            db_path=DEFAULT_DB_PATH,
            on_progress=on_progress,
            control=_worker_control,
            headless_override=headless_switch.value,
        )
        runs_status.value = "Running..."
        btn_start.disabled = True
        page.run_task(_poll_progress)
        page.update()

    def _stop_worker():
        _worker_control["stop"] = True
        runs_status.value = "Stopping..."
        page.snack_bar = ft.SnackBar(
            ft.Text("Wait until status is Idle before closing the app — that saves resume state to disk."),
            bgcolor=C_SURFACE_ELEVATED,
            open=True,
        )
        page.update()


    # --- Results tab ---
    # Parent tab Column scrolls (see results_tab_content); avoid nested expand+scroll = blank area
    results_list = ft.Column(spacing=10)
    results_search = ft.TextField(
        hint_text="Search pair, job id, filename…",
        prefix_icon=ft.Icons.SEARCH,
        border_color=C_BORDER,
        focused_border_color=C_ACCENT,
        cursor_color=C_ACCENT,
        bgcolor=C_SURFACE,
        expand=True,
        on_change=lambda e: refresh_results_tab(),
    )
    results_sort = ft.Dropdown(
        width=200,
        value="newest",
        options=[
            ft.dropdown.Option("newest", "Newest modified"),
            ft.dropdown.Option("oldest", "Oldest modified"),
            ft.dropdown.Option("strategy", "Job id (A→Z)"),
            ft.dropdown.Option("pair", "Pair (A→Z)"),
        ],
        border_color=C_BORDER,
        focused_border_color=C_ACCENT,
        bgcolor=C_SURFACE,
        on_change=lambda e: refresh_results_tab(),
    )
    results_count_label = ft.Text("", size=13, color=C_MUTED)

    def _open_alert_dialog(dlg: ft.AlertDialog) -> None:
        """Flet 0.24+ expects dialogs via page.open(), not only page.dialog assignment."""
        page.open(dlg)

    def _close_alert_dialog(dlg: ft.AlertDialog) -> None:
        try:
            ft.Page.close(dlg)
        except Exception:
            try:
                dlg.open = False
            except Exception:
                pass
        page.update()

    def _results_table_header() -> ft.Control:
        def h(text: str, width: int | None = None, expand: int = 0, tip: str | None = None):
            return ft.Container(
                content=ft.Text(text, size=12, weight=ft.FontWeight.W_600, color=C_MUTED),
                width=width,
                expand=expand or False,
                padding=ft.padding.symmetric(horizontal=8, vertical=10),
                alignment=_ALIGN_CENTER_LEFT,
                tooltip=tip,
            )

        return ft.Container(
            content=ft.Row(
                [
                    h("Job", 80, tip="Queue id in filenames (strategy_NN_…), not config.yaml order"),
                    h("Pair", 108, tip="Chart’s base pair when the scan ran"),
                    h("Scan", 84),
                    h("Pairs", 52),
                    h("Run", expand=1),
                    h("Modified", expand=1),
                    h("Open / actions", 268, tip="HTML report and XLSX spreadsheet for the same scan"),
                ],
                spacing=0,
            ),
            bgcolor=C_SURFACE,
            border=ft.border.only(
                top=ft.BorderSide(1, C_BORDER),
                left=ft.BorderSide(1, C_BORDER),
                right=ft.BorderSide(1, C_BORDER),
            ),
            border_radius=ft.border_radius.only(top_left=8, top_right=8),
        )

    def _in_progress_output_stems() -> set[str]:
        """Hide current job's .xlsx/.html from Results until the worker step completes (HTML is written at end)."""
        if not _worker_thread or not _worker_thread.is_alive():
            return set()
        prog = _worker_progress
        if prog.get("status") not in ("running", "paused"):
            return set()
        ox = prog.get("output_xlsx_path") or ""
        if not str(ox).strip():
            return set()
        try:
            return {Path(str(ox)).stem}
        except Exception:
            return set()

    def _group_result_paths_by_stem(files: list[Path]) -> list[tuple[Optional[Path], Optional[Path]]]:
        """One entry per scan: (.xlsx or None, .html or None), same stem."""
        by_stem: dict[str, dict[str, Path]] = {}
        for p in files:
            st = p.stem
            if st not in by_stem:
                by_stem[st] = {}
            suf = p.suffix.lower()
            if suf == ".xlsx":
                by_stem[st]["xlsx"] = p
            elif suf == ".html":
                by_stem[st]["html"] = p
        return [(d.get("xlsx"), d.get("html")) for d in by_stem.values()]

    def _sort_result_groups(
        groups: list[tuple[Optional[Path], Optional[Path]]], sort_mode: str
    ) -> list[tuple[Optional[Path], Optional[Path]]]:
        def meta(g: tuple[Optional[Path], Optional[Path]]) -> Path:
            x, h = g
            return x or h  # type: ignore[return-value]

        def mtimes(g: tuple[Optional[Path], Optional[Path]]) -> list[float]:
            return [p.stat().st_mtime for p in (g[0], g[1]) if p is not None]

        mode = (sort_mode or "newest").strip()

        def key_newest(g):
            ts = mtimes(g)
            return (-max(ts), meta(g).name) if ts else (0.0, "")

        def key_oldest(g):
            ts = mtimes(g)
            return (min(ts), meta(g).name) if ts else (1e20, "")

        def key_strategy(g):
            p = meta(g)
            pr = parse_output_path(p)
            return (int(pr.strategy_num) if pr and pr.strategy_num.isdigit() else 999, p.name)

        def key_pair(g):
            p = meta(g)
            pr = parse_output_path(p)
            return (pr.pair_display.lower() if pr else p.name, p.name)

        if mode == "oldest":
            return sorted(groups, key=key_oldest)
        if mode == "strategy":
            return sorted(groups, key=key_strategy)
        if mode == "pair":
            return sorted(groups, key=key_pair)
        return sorted(groups, key=key_newest)

    def _build_merged_result_row(
        xlsx_p: Optional[Path],
        html_p: Optional[Path],
        refresh_cb,
    ) -> ft.Control:
        primary = xlsx_p or html_p
        assert primary is not None

        parsed = parse_output_path(primary)
        mt_vals = [p.stat().st_mtime for p in (xlsx_p, html_p) if p is not None]
        mtime_max = max(mt_vals) if mt_vals else None
        mtime_short = datetime.fromtimestamp(mtime_max).strftime("%d %b %y %H:%M") if mtime_max else "—"
        copy_target = xlsx_p or html_p
        full_path = str(copy_target.resolve()) if copy_target and copy_target.exists() else ""

        if parsed:
            strat = queue_job_label(parsed.strategy_num)
            pair = parsed.pair_display
            scan = "Deep" if "Deep" in parsed.scan_kind else "Pass 1"
            run_s = parsed.run_at_label.replace(" · ", " ") if parsed.run_at else "—"
        else:
            strat = "—"
            pair = "—"
            scan = "—"
            run_s = "—"

        count_path = xlsx_p if xlsx_p is not None else html_p
        n_pairs = count_pairs_in_output_file(count_path) if count_path else None
        if n_pairs is None:
            pairs_txt = "—"
            pairs_color = C_MUTED
            pairs_weight = None
            pairs_tip = "Could not read row count (open XLSX when available)"
        elif n_pairs == 0:
            pairs_txt = "0"
            pairs_color = "#f85149"
            pairs_weight = ft.FontWeight.W_600
            pairs_tip = "No pair rows — scan may have failed or found nothing"
        else:
            pairs_txt = str(n_pairs)
            pairs_color = C_TEXT
            pairs_weight = None
            pairs_tip = "Pairs with data rows in the spreadsheet"

        def cell(content: ft.Control | str, width: int | None = None, expand: int = 0, tip: str | None = None):
            if isinstance(content, str):
                content = ft.Text(content, size=12, color=C_TEXT, max_lines=1)
            return ft.Container(
                content=content,
                width=width,
                expand=expand or False,
                padding=ft.padding.symmetric(horizontal=8, vertical=8),
                alignment=_ALIGN_CENTER_LEFT,
                tooltip=tip,
            )

        def open_html_click(e):
            if html_p and html_p.exists():
                _open_path(html_p)

        def open_xlsx_click(e):
            if xlsx_p and xlsx_p.exists():
                _open_path(xlsx_p)

        def open_folder_click(e):
            _open_containing_folder(primary)

        def copy_path_click(e):
            if full_path:
                _copy_text(page, full_path)

        def delete_click(e):
            stem = primary.stem
            dlg_holder: list = []

            def on_confirm(ev):
                ok, err = _delete_scan_outputs(primary)
                _close_alert_dialog(dlg_holder[0])
                if ok:
                    page.snack_bar = ft.SnackBar(ft.Text("Output deleted"), bgcolor=C_SURFACE_ELEVATED, open=True)
                    refresh_cb()
                else:
                    page.snack_bar = ft.SnackBar(ft.Text(f"Delete failed: {err}"), open=True)
                    page.update()

            def on_cancel(ev):
                _close_alert_dialog(dlg_holder[0])

            dlg = ft.AlertDialog(
                modal=True,
                title=ft.Text("Delete output?", color=C_TEXT),
                content=ft.Column(
                    [
                        ft.Text(primary.name, size=13, color=C_TEXT, selectable=True),
                        ft.Text(
                            f"Removes {stem}.xlsx and {stem}.html if they exist (same scan).",
                            size=11,
                            color=C_MUTED,
                        ),
                    ],
                    spacing=8,
                ),
                actions=[
                    ft.TextButton("Cancel", on_click=on_cancel),
                    ft.TextButton("Delete", on_click=on_confirm, style=ft.ButtonStyle(color=ft.Colors.RED)),
                ],
                bgcolor=C_SURFACE_ELEVATED,
            )
            dlg_holder.append(dlg)
            _open_alert_dialog(dlg)

        fname_tip = primary.name
        job_tip = (
            f"Queue id {parsed.strategy_num} in filename — not config.yaml strategy order" if parsed else None
        )

        link_style = ft.ButtonStyle(color=C_ACCENT, padding=ft.padding.symmetric(horizontal=6, vertical=4))
        btn_html = ft.TextButton(
            "HTML",
            on_click=open_html_click,
            disabled=not (html_p and html_p.exists()),
            tooltip="Open HTML report",
            style=link_style,
        )
        btn_xlsx = ft.TextButton(
            "XLSX",
            on_click=open_xlsx_click,
            disabled=not (xlsx_p and xlsx_p.exists()),
            tooltip="Open Excel spreadsheet",
            style=link_style,
        )

        actions = ft.Row(
            [
                btn_html,
                btn_xlsx,
                ft.IconButton(ft.Icons.FOLDER_OPEN, icon_color=C_MUTED, tooltip="Show in folder", on_click=open_folder_click),
                ft.IconButton(ft.Icons.CONTENT_COPY, icon_color=C_MUTED, tooltip="Copy path (XLSX if present, else HTML)", on_click=copy_path_click),
                ft.IconButton(ft.Icons.DELETE_OUTLINE, icon_color="#f85149", tooltip="Delete scan (both files)", on_click=delete_click),
            ],
            spacing=0,
            wrap=False,
        )

        if pairs_weight:
            pairs_ctrl = ft.Text(pairs_txt, size=12, color=pairs_color, max_lines=1, weight=pairs_weight)
        else:
            pairs_ctrl = ft.Text(pairs_txt, size=12, color=pairs_color, max_lines=1)

        return ft.Container(
            content=ft.Row(
                [
                    cell(strat, 80, tip=job_tip),
                    cell(pair, 108, tip=fname_tip),
                    cell(scan, 84),
                    cell(pairs_ctrl, 52, tip=pairs_tip),
                    cell(run_s, expand=1, tip=fname_tip),
                    cell(mtime_short, expand=1, tip="Latest change on HTML or XLSX"),
                    cell(actions, 268),
                ],
                spacing=0,
            ),
            border=ft.border.only(left=ft.BorderSide(1, C_BORDER), right=ft.BorderSide(1, C_BORDER), bottom=ft.BorderSide(1, C_BORDER)),
            bgcolor=C_SURFACE_ELEVATED,
        )

    def _collect_result_paths() -> list[Path]:
        """All .xlsx / .html in output_dir matching search (no sort — grouping sorts later)."""
        all_files = list(output_dir.glob("*.xlsx")) + list(output_dir.glob("*.html"))
        q = (results_search.value or "").strip().lower()
        if q:
            all_files = [p for p in all_files if q in p.name.lower() or q in str(p).lower()]

        busy = _in_progress_output_stems()
        if busy:
            all_files = [p for p in all_files if p.stem not in busy]
        return all_files

    def _primary_paths_per_scan_stem(files: list[Path]) -> list[Path]:
        """One path per stem so we do not double-delete when both .xlsx and .html are listed."""
        by_stem: dict[str, Path] = {}
        for p in files:
            st = p.stem
            if st not in by_stem:
                by_stem[st] = p
            elif p.suffix.lower() == ".xlsx" and by_stem[st].suffix.lower() != ".xlsx":
                by_stem[st] = p
        return list(by_stem.values())

    def refresh_results_tab():
        results_list.controls.clear()
        all_files = _collect_result_paths()
        groups = _group_result_paths_by_stem(all_files)
        sort_mode = (results_sort.value or "newest").strip()
        groups = _sort_result_groups(groups, sort_mode)

        total_matches = len(groups)
        shown_groups = groups[:80]
        if total_matches > 80:
            results_count_label.value = f"Showing {len(shown_groups)} of {total_matches} scans (max 80 per view)"
        elif total_matches == 0:
            results_count_label.value = "No matching files"
        else:
            results_count_label.value = f"Showing {total_matches} scan(s)"

        if not shown_groups:
            results_list.controls.append(
                ft.Container(
                    content=ft.Column(
                        [
                            ft.Icon(ft.Icons.INSERT_DRIVE_FILE_OUTLINED, size=48, color=C_MUTED),
                            ft.Text("No matching outputs", size=16, weight=ft.FontWeight.W_600, color=C_TEXT),
                            ft.Text(
                                "Run a scan from the Runs tab, or clear the search filter. If a job is running, its outputs are hidden here until it completes.",
                                size=13,
                                color=C_MUTED,
                            ),
                        ],
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        spacing=12,
                    ),
                    alignment=_ALIGN_CENTER,
                    padding=40,
                )
            )
        else:
            results_list.controls.append(_results_table_header())
            for i, (xlsx_p, html_p) in enumerate(shown_groups):
                try:
                    row = _build_merged_result_row(xlsx_p, html_p, refresh_results_tab)
                except Exception:
                    continue
                # Alternate row tint
                if i % 2 == 1 and isinstance(row, ft.Container):
                    row.bgcolor = C_SURFACE
                results_list.controls.append(row)
        results_list.controls.append(ft.Container(height=24))
        page.update()

    def delete_all_results_click(e):
        all_files = _collect_result_paths()
        primaries = _primary_paths_per_scan_stem(all_files)
        n = len(primaries)
        if n == 0:
            page.snack_bar = ft.SnackBar(ft.Text("No files to delete."), bgcolor=C_SURFACE_ELEVATED, open=True)
            page.update()
            return

        q_raw = (results_search.value or "").strip()
        search_note = f"Matches current search (“{q_raw}”). " if q_raw else ""
        n_scans = len(primaries)
        if n_scans > 80:
            search_note += f"Table lists 80 scans per view; this will delete all {n_scans} matching scan(s) (XLSX+HTML pairs). "

        dlg_holder: list = []

        def on_confirm(ev):
            _close_alert_dialog(dlg_holder[0])
            ok_n = 0
            err_n = 0
            last_err = ""
            for path in primaries:
                ok, err = _delete_scan_outputs(path)
                if ok:
                    ok_n += 1
                else:
                    err_n += 1
                    last_err = err or last_err
            refresh_results_tab()
            if err_n == 0:
                msg = f"Deleted {ok_n} scan output(s)."
            else:
                msg = f"Deleted {ok_n}, {err_n} failed. {last_err}".strip()
            page.snack_bar = ft.SnackBar(ft.Text(msg), bgcolor=C_SURFACE_ELEVATED, open=True)
            page.update()

        def on_cancel(ev):
            _close_alert_dialog(dlg_holder[0])

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Delete all results?", color=C_TEXT),
            content=ft.Column(
                [
                    ft.Text(
                        f"{search_note}Removes {n} scan output(s): each deletes paired .xlsx and .html with the same name.",
                        size=13,
                        color=C_TEXT,
                    ),
                    ft.Text("This cannot be undone.", size=12, color=C_WARN, weight=ft.FontWeight.W_600),
                ],
                spacing=10,
                tight=True,
            ),
            actions=[
                ft.TextButton("Cancel", on_click=on_cancel),
                ft.TextButton("Delete all", on_click=on_confirm, style=ft.ButtonStyle(color=ft.Colors.RED)),
            ],
            bgcolor=C_SURFACE_ELEVATED,
        )
        dlg_holder.append(dlg)
        _open_alert_dialog(dlg)

    btn_delete_all_results = ft.OutlinedButton(
        "Delete all",
        icon=ft.Icons.DELETE_SWEEP,
        on_click=delete_all_results_click,
        style=ft.ButtonStyle(color={ft.ControlState.DEFAULT: "#f85149"}),
    )

    # --- Tab container (outer expand=True so body fills space below header on all platforms) ---
    runs_tab_content = ft.Container(
        expand=True,
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Column(
                            [
                                ft.Text("Runs & progress", size=22, weight=ft.FontWeight.BOLD, color=C_TEXT),
                                ft.Text("Run the queue, log in to TradingView, and watch live progress.", size=13, color=C_MUTED),
                            ],
                            expand=True,
                        ),
                    ],
                ),
                ft.Container(height=12),
                ft.Row(
                    [
                        runs_status,
                        ft.Row([btn_start, btn_stop, ft.IconButton(ft.Icons.REFRESH, on_click=lambda e: refresh_runs_tab(), tooltip="Refresh", icon_color=C_ACCENT)], spacing=8),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Row(
                    [
                        headless_switch,
                        ft.Text("Log in any time via Step 1 in the header.", size=12, color=C_MUTED),
                    ],
                    alignment=ft.MainAxisAlignment.START,
                    spacing=16,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                ft.Container(height=8),
                ft.Container(content=runs_list, expand=True),
            ],
            expand=True,
        ),
    )

    results_tab_content = ft.Container(
        expand=True,
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Column(
                            [
                                ft.Text("Output library", size=22, weight=ft.FontWeight.BOLD, color=C_TEXT),
                                ft.Text(
                                    "Completed outputs only — the active job’s files stay on Runs (live table + spreadsheet) until that step finishes and HTML is written. "
                                    "Each row is one scan; use HTML and XLSX to open the report or spreadsheet. "
                                    "Column Job is the queue list id (filename strategy_NN_…), not your strategy order in config.yaml.",
                                    size=13,
                                    color=C_MUTED,
                                ),
                            ],
                            expand=True,
                        ),
                    ],
                ),
                ft.Container(height=12),
                ft.Row(
                    [
                        results_search,
                        results_sort,
                        ft.ElevatedButton("Refresh", icon=ft.Icons.REFRESH, on_click=lambda e: refresh_results_tab()),
                        btn_delete_all_results,
                    ],
                    spacing=12,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                ft.Container(height=8),
                results_count_label,
                ft.Container(height=8),
                results_list,
                ft.Container(height=8),
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
    )

    tab_contents = [queue_tab, runs_tab_content, results_tab_content]
    content_holder = ft.Container(content=queue_tab, expand=True, padding=ft.padding.only(top=8))

    def select_tab(idx: int):
        content_holder.content = tab_contents[idx]
        if idx == 0:
            refresh_queue_tab()
        elif idx == 1:
            if "Opening" not in (login_status.value or ""):
                login_status.value = _load_login_status()
            refresh_runs_tab()
        elif idx == 2:
            refresh_results_tab()
        for i, pill in enumerate(nav_pills):
            active = i == idx
            pill.bgcolor = C_ACCENT if active else C_SURFACE_ELEVATED
            pill.border = ft.border.all(1, C_ACCENT if active else C_BORDER)
            if isinstance(pill.content, ft.Text):
                pill.content.color = C_BG if active else C_MUTED
        page.update()

    nav_pills: list[ft.Container] = []
    for i, name in enumerate(["Queue", "Runs", "Results"]):
        lbl = ft.Text(name, size=13, weight=ft.FontWeight.W_600, color=C_MUTED)
        nav_pills.append(
            ft.Container(
                content=lbl,
                padding=ft.padding.symmetric(horizontal=22, vertical=11),
                border_radius=8,
                bgcolor=C_SURFACE_ELEVATED,
                border=ft.border.all(1, C_BORDER),
                on_click=lambda e, ix=i: select_tab(ix),
            )
        )

    # Brand strip — mirrors src/html_writer.py .brand-banner / .brand-logo
    brand_link = ft.Text(
        TRADE_HARBOUR_LINK_TEXT,
        size=13,
        weight=ft.FontWeight.W_500,
        color=C_LINK,
    )

    def _th_link_click(_):
        _open_trade_harbour(page)

    brand_link_row = ft.GestureDetector(
        content=brand_link,
        on_tap=_th_link_click,
    )

    brand_banner = ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Text("Powered By", size=13, color=C_POWERED_BY),
                        brand_link_row,
                    ],
                    spacing=12,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                ft.Container(
                    content=ft.Row(
                        [
                            ft.Text("⚓", size=26, color=C_LINK),
                            ft.Column(
                                [
                                    ft.Text(BRAND_TITLE, size=20, weight=ft.FontWeight.BOLD, color=C_TEXT),
                                    ft.Text(BRAND_TAGLINE, size=13, italic=True, color=C_TAGLINE),
                                ],
                                spacing=2,
                            ),
                        ],
                        spacing=12,
                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    ),
                    padding=ft.padding.only(top=10, left=0),
                    border=ft.border.only(top=ft.BorderSide(1, C_BORDER)),
                ),
            ],
            spacing=0,
        ),
        bgcolor=C_BRAND_BANNER_BG,
        padding=16,
        border_radius=10,
        border=ft.border.all(1, C_BORDER),
    )

    header_login_block = ft.Container(
        content=ft.Column(
            [
                btn_tv_login,
                login_hint_90s,
                login_status,
            ],
            spacing=2,
            tight=True,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        padding=ft.padding.symmetric(horizontal=12),
    )

    title_row = ft.Row(
        [
            ft.Column(
                [
                    ft.Text(APP_DISPLAY_NAME, size=26, weight=ft.FontWeight.BOLD, color=C_TEXT),
                    ft.Text("TradingView multi-pair scans · Bybit USDT", size=13, color=C_MUTED),
                ],
                spacing=4,
                expand=True,
                tight=True,
            ),
            header_login_block,
            ft.Container(
                content=ft.Row(nav_pills, spacing=8),
                bgcolor=C_SURFACE,
                padding=8,
                border_radius=12,
                border=ft.border.all(1, C_BORDER),
            ),
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    header = ft.Column(
        [
            brand_banner,
            ft.Container(height=16),
            title_row,
        ],
        spacing=0,
    )

    header_wrap = ft.Container(
        content=header,
        padding=ft.padding.only(bottom=16),
        border=ft.border.only(bottom=ft.BorderSide(1, C_BORDER)),
    )

    shell = ft.Column(
        expand=True,
        controls=[
            header_wrap,
            content_holder,
        ],
    )

    page.add(ft.Container(content=shell, padding=24, expand=True))
    page.overlay.append(queue_import_picker)

    # Initial load
    login_status.value = _load_login_status()
    refresh_queue_tab()
    select_tab(0)
    page.update()
