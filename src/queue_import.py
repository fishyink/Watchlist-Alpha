"""
Parse bulk queue imports from .txt / .csv / .tsv for the desktop UI.

Supported formats:
1) One TradingView URL per line (blank lines and # comments ignored).
2) name|url or name<TAB>url per line.
3) Headerless two-column CSV from Sheets: chart URL, dtech/strategy URL (comma-separated).
4) CSV/TSV with header row including a url column (optional: name, export_link,
   deep_backtest, phase1_pairs). UTF-8; BOM allowed.
"""
from __future__ import annotations

import csv
import re
from io import StringIO
from typing import Any


def _parse_bool_cell(raw: str | None) -> bool | None:
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


def _normalize_csv_row(row: dict[str | Any, str | Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in row.items():
        if k is None:
            continue
        key = str(k).strip().lower().replace(" ", "_")
        out[key] = (v or "").strip() if v is not None else ""
    return out


def _parse_csv_or_tsv(text: str) -> list[dict[str, Any]] | None:
    """If text looks like a table with url column, return rows; else None."""
    sample = text.lstrip("\ufeff").strip()
    if not sample:
        return None
    first_nl = sample.split("\n", 1)[0]
    if "http" in first_nl.lower() and first_nl.strip().startswith("http"):
        return None
    if "url" not in first_nl.lower():
        return None
    delimiter = "\t" if first_nl.count("\t") >= first_nl.count(",") else ","
    try:
        reader = csv.DictReader(StringIO(sample), delimiter=delimiter)
        if not reader.fieldnames:
            return None
        fn = [f.strip().lower().replace(" ", "_") for f in reader.fieldnames if f]
        if "url" not in fn:
            return None
        out: list[dict[str, Any]] = []
        for raw in reader:
            row = _normalize_csv_row(raw)
            url = row.get("url", "").strip()
            if not url or not url.lower().startswith("http"):
                continue
            name = row.get("name", "").strip()
            export_link = row.get("export_link", row.get("export", row.get("dtech", ""))).strip() or None
            deep = _parse_bool_cell(row.get("deep_backtest", row.get("deep", "")))
            ph = row.get("phase1_pairs", row.get("phase1", row.get("pairs", ""))).strip().lower()
            if ph not in ("all", "top300"):
                ph = None
            out.append(
                {
                    "url": url,
                    "name": name,
                    "export_link": export_link,
                    "deep_backtest": deep,
                    "phase1_pairs": ph,
                }
            )
        return out if out else None
    except Exception:
        return None


_URL_START = re.compile(r"^https?://", re.I)


def _parse_headerless_two_url_csv(text: str) -> list[dict[str, Any]] | None:
    """
    Google Sheets export: two columns, no header — chart URL, strategy/export URL.
    Example: https://www.tradingview.com/chart/xxx/,https://daviddtech.com/strategy/...
    """
    raw = text.lstrip("\ufeff").strip()
    if not raw:
        return None
    first = raw.split("\n", 1)[0].strip()
    if not first.lower().startswith("http") or "," not in first:
        return None
    # Skip real CSV headers like url,name
    low = first.lower()
    if low.startswith("url,") or low.startswith("chart,") or low.startswith("tradingview,"):
        return None
    try:
        parts = next(csv.reader([first]))
    except Exception:
        return None
    if len(parts) < 2:
        return None
    a, b = parts[0].strip(), parts[1].strip()
    if not (_URL_START.match(a) and _URL_START.match(b)):
        return None
    if "tradingview.com" not in a.lower():
        return None

    out: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            cols = next(csv.reader([line]))
        except Exception:
            continue
        if len(cols) < 2:
            continue
        chart_u, export_u = cols[0].strip(), cols[1].strip()
        if not (_URL_START.match(chart_u) and _URL_START.match(export_u)):
            continue
        if "tradingview.com" not in chart_u.lower():
            continue
        out.append(
            {
                "url": chart_u,
                "name": "",
                "export_link": export_u,
                "deep_backtest": None,
                "phase1_pairs": None,
            }
        )
    return out if out else None


def _parse_line_format(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if _URL_START.match(s):
            # Whole line is one URL only (no comma-separated second link)
            if "," in s and s.count("http") >= 2:
                continue
            out.append(
                {
                    "url": s,
                    "name": "",
                    "export_link": None,
                    "deep_backtest": None,
                    "phase1_pairs": None,
                }
            )
            continue
        if "|" in s:
            left, right = s.split("|", 1)
            name, url = left.strip(), right.strip()
        elif "\t" in s:
            parts = s.split("\t", 1)
            name, url = parts[0].strip(), parts[1].strip() if len(parts) > 1 else ""
        else:
            continue
        if url and _URL_START.match(url):
            out.append(
                {
                    "url": url,
                    "name": name,
                    "export_link": None,
                    "deep_backtest": None,
                    "phase1_pairs": None,
                }
            )
    return out


def parse_queue_import_text(text: str) -> list[dict[str, Any]]:
    """
    Parse file body into job dicts: url, name, export_link, deep_backtest?, phase1_pairs?
    Missing optional fields use None (apply defaults in UI).
    """
    raw = text.lstrip("\ufeff")
    pair_rows = _parse_headerless_two_url_csv(raw)
    if pair_rows is not None:
        return pair_rows
    csv_rows = _parse_csv_or_tsv(raw)
    if csv_rows is not None:
        return csv_rows
    lines = raw.splitlines()
    return _parse_line_format(lines)


def apply_queue_defaults(
    entries: list[dict[str, Any]],
    *,
    default_deep: bool,
    default_phase1_pairs: str,
) -> list[dict[str, Any]]:
    ph = default_phase1_pairs if default_phase1_pairs in ("all", "top300") else "top300"
    fixed: list[dict[str, Any]] = []
    for e in entries:
        d = dict(e)
        if d.get("deep_backtest") is None:
            d["deep_backtest"] = default_deep
        p = d.get("phase1_pairs")
        if p not in ("all", "top300"):
            d["phase1_pairs"] = ph
        fixed.append(d)
    return fixed


def parse_queue_import_file(path: str | Any) -> list[dict[str, Any]]:
    """Read UTF-8 file and parse."""
    p = path if isinstance(path, str) else str(path)
    text = open(p, "r", encoding="utf-8-sig", errors="replace").read()
    return parse_queue_import_text(text)
