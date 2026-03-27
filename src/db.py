"""
SQLite schema and CRUD for queue_items, run_state, and runs.
Used by the Watchlist Scanner UI for queue management and resume support.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .paths import get_app_root

DEFAULT_DB_PATH = get_app_root() / "data" / "watchlist.db"


def get_conn(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get connection to the database, creating parent dir and schema if needed."""
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS queue_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            name TEXT,
            export_link TEXT,
            deep_backtest INTEGER NOT NULL DEFAULT 0,
            phase1_pairs TEXT NOT NULL DEFAULT 'top300',
            interval INTEGER,
            created_at TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS run_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_item_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            phase TEXT NOT NULL,
            output_xlsx_path TEXT,
            output_html_path TEXT,
            completed_pairs_json TEXT,
            current_pair_index INTEGER NOT NULL DEFAULT 0,
            total_pairs INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            started_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (queue_item_id) REFERENCES queue_items(id)
        );

        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_item_id INTEGER NOT NULL,
            phase TEXT NOT NULL,
            output_xlsx_path TEXT,
            output_html_path TEXT,
            pairs_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            finished_at TEXT,
            FOREIGN KEY (queue_item_id) REFERENCES queue_items(id)
        );

        CREATE INDEX IF NOT EXISTS ix_run_state_queue_item ON run_state(queue_item_id);
        CREATE INDEX IF NOT EXISTS ix_runs_queue_item ON runs(queue_item_id);
    """)


# --- Queue items ---

def add_queue_item(
    url: str,
    *,
    name: str = "",
    export_link: Optional[str] = None,
    deep_backtest: bool = False,
    phase1_pairs: str = "top300",
    interval: Optional[int] = None,
    db_path: Optional[Path] = None,
) -> int:
    """Add a queue item. Returns the new id."""
    conn = get_conn(db_path)
    try:
        max_order = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM queue_items"
        ).fetchone()[0]
        cursor = conn.execute(
            """INSERT INTO queue_items (url, name, export_link, deep_backtest, phase1_pairs, interval, created_at, sort_order)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                url.strip(),
                (name or "").strip(),
                (export_link or "").strip() or None,
                1 if deep_backtest else 0,
                phase1_pairs if phase1_pairs in ("all", "top300") else "top300",
                interval,
                datetime.utcnow().isoformat(),
                max_order,
            ),
        )
        conn.commit()
        return cursor.lastrowid or 0
    finally:
        conn.close()


def get_queue_items(db_path: Optional[Path] = None) -> list[dict[str, Any]]:
    """Get all queue items ordered by sort_order."""
    conn = get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM queue_items ORDER BY sort_order, id"
        ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def update_queue_item(
    item_id: int,
    *,
    url: Optional[str] = None,
    name: Optional[str] = None,
    export_link: Optional[str] = None,
    deep_backtest: Optional[bool] = None,
    phase1_pairs: Optional[str] = None,
    interval: Optional[int] = None,
    sort_order: Optional[int] = None,
    db_path: Optional[Path] = None,
) -> None:
    """Update a queue item. Only provided fields are updated."""
    conn = get_conn(db_path)
    try:
        updates = []
        args = []
        if url is not None:
            updates.append("url = ?")
            args.append(url.strip())
        if name is not None:
            updates.append("name = ?")
            args.append(name.strip())
        if export_link is not None:
            updates.append("export_link = ?")
            args.append(export_link.strip() or None)
        if deep_backtest is not None:
            updates.append("deep_backtest = ?")
            args.append(1 if deep_backtest else 0)
        if phase1_pairs is not None:
            updates.append("phase1_pairs = ?")
            args.append(phase1_pairs if phase1_pairs in ("all", "top300") else "top300")
        if interval is not None:
            updates.append("interval = ?")
            args.append(interval)
        if sort_order is not None:
            updates.append("sort_order = ?")
            args.append(sort_order)
        if not updates:
            return
        args.append(item_id)
        conn.execute(
            f"UPDATE queue_items SET {', '.join(updates)} WHERE id = ?",
            args,
        )
        conn.commit()
    finally:
        conn.close()


def delete_queue_item(item_id: int, db_path: Optional[Path] = None) -> None:
    """Delete a queue item and its run_state."""
    conn = get_conn(db_path)
    try:
        conn.execute("DELETE FROM run_state WHERE queue_item_id = ?", (item_id,))
        conn.execute("DELETE FROM runs WHERE queue_item_id = ?", (item_id,))
        conn.execute("DELETE FROM queue_items WHERE id = ?", (item_id,))
        conn.commit()
    finally:
        conn.close()


def reorder_queue_items(item_ids: list[int], db_path: Optional[Path] = None) -> None:
    """Set sort_order by the order of item_ids."""
    conn = get_conn(db_path)
    try:
        for i, qid in enumerate(item_ids):
            conn.execute("UPDATE queue_items SET sort_order = ? WHERE id = ?", (i, qid))
        conn.commit()
    finally:
        conn.close()


# --- Run state ---

def get_run_state(queue_item_id: int, db_path: Optional[Path] = None) -> Optional[dict[str, Any]]:
    """Get the latest incomplete run_state for a queue item."""
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            """SELECT * FROM run_state
               WHERE queue_item_id = ? AND status IN ('pending', 'running', 'paused')
               ORDER BY updated_at DESC LIMIT 1""",
            (queue_item_id,),
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def upsert_run_state(
    queue_item_id: int,
    status: str,
    phase: str,
    *,
    output_xlsx_path: Optional[str] = None,
    output_html_path: Optional[str] = None,
    completed_pairs_json: Optional[str] = None,
    current_pair_index: int = 0,
    total_pairs: int = 0,
    error_message: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> int:
    """Insert or update run_state. Returns run_state id."""
    conn = get_conn(db_path)
    now = datetime.utcnow().isoformat()
    try:
        existing = conn.execute(
            "SELECT id FROM run_state WHERE queue_item_id = ? AND status IN ('pending', 'running', 'paused') ORDER BY updated_at DESC LIMIT 1",
            (queue_item_id,),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE run_state SET
                    status = ?, phase = ?, output_xlsx_path = ?, output_html_path = ?,
                    completed_pairs_json = ?, current_pair_index = ?, total_pairs = ?,
                    error_message = ?, updated_at = ?
                    WHERE id = ?""",
                (
                    status,
                    phase,
                    output_xlsx_path,
                    output_html_path,
                    completed_pairs_json,
                    current_pair_index,
                    total_pairs,
                    error_message,
                    now,
                    existing["id"],
                ),
            )
            conn.commit()
            return existing["id"]
        else:
            cursor = conn.execute(
                """INSERT INTO run_state (queue_item_id, status, phase, output_xlsx_path, output_html_path,
                   completed_pairs_json, current_pair_index, total_pairs, error_message, started_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    queue_item_id,
                    status,
                    phase,
                    output_xlsx_path,
                    output_html_path,
                    completed_pairs_json,
                    current_pair_index,
                    total_pairs,
                    error_message,
                    now,
                    now,
                ),
            )
            conn.commit()
            return cursor.lastrowid or 0
    finally:
        conn.close()


def clear_run_state(queue_item_id: int, db_path: Optional[Path] = None) -> None:
    """Clear incomplete run_state for a queue item (when starting fresh)."""
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE run_state SET status = 'cancelled' WHERE queue_item_id = ? AND status IN ('pending', 'running', 'paused')",
            (queue_item_id,),
        )
        conn.commit()
    finally:
        conn.close()


# --- Runs (completed/failed history) ---

def add_run(
    queue_item_id: int,
    phase: str,
    status: str,
    *,
    output_xlsx_path: Optional[str] = None,
    output_html_path: Optional[str] = None,
    pairs_count: int = 0,
    db_path: Optional[Path] = None,
) -> int:
    """Record a completed or failed run. Returns run id."""
    conn = get_conn(db_path)
    now = datetime.utcnow().isoformat()
    try:
        cursor = conn.execute(
            """INSERT INTO runs (queue_item_id, phase, output_xlsx_path, output_html_path, pairs_count, status, finished_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (queue_item_id, phase, output_xlsx_path, output_html_path, pairs_count, status, now),
        )
        conn.commit()
        return cursor.lastrowid or 0
    finally:
        conn.close()


def get_runs(
    queue_item_id: Optional[int] = None,
    limit: int = 100,
    db_path: Optional[Path] = None,
) -> list[dict[str, Any]]:
    """Get runs, optionally filtered by queue_item_id. Joins queue item name/url for display."""
    conn = get_conn(db_path)
    try:
        if queue_item_id is not None:
            rows = conn.execute(
                """SELECT r.*, q.name, q.url FROM runs r
                   JOIN queue_items q ON r.queue_item_id = q.id
                   WHERE r.queue_item_id = ?
                   ORDER BY r.finished_at DESC LIMIT ?""",
                (queue_item_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT r.*, q.name, q.url FROM runs r
                   JOIN queue_items q ON r.queue_item_id = q.id
                   ORDER BY r.finished_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    if "deep_backtest" in d:
        d["deep_backtest"] = bool(d.get("deep_backtest"))
    return d


def parse_completed_pairs(json_str: Optional[str]) -> list[str]:
    """Parse completed_pairs_json to list of symbol strings."""
    if not json_str:
        return []
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return []


def serialize_completed_pairs(symbols: list[str]) -> str:
    """Serialize list of symbols to JSON string."""
    return json.dumps(symbols)
