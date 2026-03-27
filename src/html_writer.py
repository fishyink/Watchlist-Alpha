"""
HTML report writer for strategy scan results. Dark dashboard theme with sortable columns.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .branding import BRAND_TAGLINE, BRAND_TITLE, TRADE_HARBOUR_LINK_TEXT, TRADE_HARBOUR_URL


def _format_cell(val: Any) -> str:
    """Format value for display."""
    if val is None or val == "":
        return ""
    if isinstance(val, float) and val != val:  # NaN
        return ""
    if isinstance(val, (int, float)):
        if isinstance(val, float):
            return f"{val:,.2f}"
        return f"{val:,}"
    return str(val)


def _cell_is_positive(val: Any) -> bool:
    if val is None or val == "":
        return False
    try:
        return float(val) > 0
    except (TypeError, ValueError):
        return False


def _cell_is_negative(val: Any) -> bool:
    if val is None or val == "":
        return False
    try:
        return float(val) < 0
    except (TypeError, ValueError):
        return False


# Display columns (Net Profit, Max Drawdown, Gross Profit/Loss omitted)
COLUMNS = [
    "Symbol",
    "Net Profit %",
    "Max Drawdown %",
    "Sharpe Ratio",
    "Sortino Ratio",
    "Win Rate %",
    "# Trades",
    "Profit Factor",
]
# Row indices in excel data: 0=Symbol, 1=NetProfit, 2=NetProfit%, 3=GrossProfit, 4=GrossLoss, 5=MaxDD, 6=MaxDD%, 7=Sharpe, 8=Sortino, 9=WinRate, 10=Trades, 11=ProfitFactor
# Skip 1,3,4,5 (Net Profit, Gross Profit, Gross Loss, Max Drawdown)
ROW_INDICES = [0, 2, 6, 7, 8, 9, 10, 11]

SIGN_COLUMNS = {"Net Profit %", "Max Drawdown %"}


def _col_type(col: str) -> str:
    return "str" if col == "Symbol" else "num"


def _symbol_matches(symbol_cell: str, original_pair: str) -> bool:
    """Check if row symbol matches the original chart pair (e.g. LINK, CAKE)."""
    if not original_pair or not symbol_cell:
        return False
    # Normalize: "BYBIT:LINKUSDT.P" -> "LINKUSDT.P", "LINKUSDT.P" -> "LINKUSDT.P"
    def _norm(s: str) -> str:
        s = (s or "").strip().upper()
        if ":" in s:
            s = s.split(":")[-1]
        return s
    return _norm(symbol_cell) == _norm(original_pair)


def write_html_report(
    output_dir: Path,
    strategy_index: int,
    strategy_url: str,
    original_pair: str,
    strategy_name: str | None,
    rows: list[list[Any]],
    xlsx_path: Path,
) -> Path:
    """
    Write an HTML report with dark dashboard styling and sortable columns.
    rows: list of [symbol, net_profit, net_profit_pct, ...] matching COLUMNS order.
    """
    stem = xlsx_path.stem
    html_filename = f"{stem}.html"
    filepath = output_dir / html_filename

    col_count = len(COLUMNS)

    def _has_data(r: list) -> bool:
        """Exclude rows where all key metrics are empty (failed extractions)."""
        if not r or len(r) < 2:
            return False
        # Need at least one of: net profit, gross profit, max drawdown, total trades
        indices = [1, 3, 4, 5, 10]  # Net Profit, Gross Profit, Gross Loss, Max DD, # Trades
        for i in indices:
            if i < len(r):
                v = r[i]
                if v is not None and v != "":
                    if not isinstance(v, (int, float)) or v == v:  # exclude NaN
                        return True
        return False

    rows_filtered = [r for r in rows if _has_data(r)]
    num_rows = len(rows_filtered)

    tbody_rows = ""
    for row in rows_filtered:
        row_symbol = (row[0] if row else "") or ""
        is_original = _symbol_matches(row_symbol, original_pair)
        row_class = ' class="original-pair"' if is_original else ""
        # Build display row: skip Gross Profit (3), Gross Loss (4)
        display_row = [(row[i] if i < len(row) else "") for i in ROW_INDICES]
        cells = []
        for col_idx, (col_name, val) in enumerate(zip(COLUMNS, display_row)):
            text = _format_cell(val) if val is not None and val != "" else "—"
            if col_name in ("Net Profit %", "Max Drawdown %") and text and text != "—":
                text = f"{text}%"
            cls = ""
            if col_name in SIGN_COLUMNS:
                if _cell_is_positive(val):
                    cls = " positive"
                elif _cell_is_negative(val):
                    cls = " negative"
            cells.append(f'<td class="symbol{cls}">{text}</td>')
        tbody_rows += f'<tr{row_class}>' + "".join(cells) + "</tr>\n"

    header_cells = "".join(
        f'<th data-col="{i}" data-type="{_col_type(col)}">{col}<span class="sort-icon"></span></th>'
        for i, col in enumerate(COLUMNS)
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Strategy {strategy_index} – {original_pair or "Scan"}</title>
  <style>
    :root {{
      --bg: #0d1117;
      --bg-row: #161b22;
      --bg-row-alt: #21262d;
      --text: #e6edf3;
      --text-muted: #8b949e;
      --border: #30363d;
      --positive: #3fb950;
      --negative: #f85149;
      --link: #58a6ff;
      --link-hover: #79b8ff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 24px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif;
      background: var(--bg);
      color: var(--text);
      font-size: 14px;
      line-height: 1.5;
    }}
    .container {{ max-width: 1400px; margin: 0 auto; }}
    h1 {{ font-size: 20px; font-weight: 600; margin: 0 0 8px 0; }}
    .meta {{ color: var(--text-muted); font-size: 13px; margin-bottom: 16px; }}
    .meta a {{ color: var(--link); text-decoration: none; }}
    .meta a:hover {{ color: var(--link-hover); text-decoration: underline; }}
    .info {{ margin-bottom: 16px; font-size: 13px; color: var(--text-muted); }}
    .table-wrap {{
      background: var(--bg-row);
      border: 1px solid var(--border);
      border-radius: 12px;
      overflow: hidden;
    }}
    table {{ width: 100%; border-collapse: collapse; }}
    th {{
      background: var(--bg-row-alt);
      color: var(--text-muted);
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      padding: 12px 16px;
      text-align: left;
      border-bottom: 1px solid var(--border);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    th:hover {{ color: var(--text); }}
    th .sort-icon {{ margin-left: 6px; opacity: 0.4; font-size: 10px; }}
    th.sorted-asc .sort-icon::after {{ content: " ▲"; opacity: 1; }}
    th.sorted-desc .sort-icon::after {{ content: " ▼"; opacity: 1; }}
    td {{ padding: 12px 16px; border-bottom: 1px solid var(--border); }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: rgba(255,255,255,0.02); }}
    tr.original-pair td {{ background: rgba(88, 166, 255, 0.08); }}
    td.symbol a {{ color: var(--link); text-decoration: none; }}
    td.symbol a:hover {{ color: var(--link-hover); text-decoration: underline; }}
    td.positive {{ color: var(--positive); }}
    td.negative {{ color: var(--negative); }}
    td {{ text-align: right; }}
    td:first-child {{ text-align: left; }}
    .brand-banner {{
      background: #0b121e;
      margin: -24px -24px 24px -24px;
      padding: 16px 24px;
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .brand-banner a {{ color: #58a6ff; text-decoration: none; }}
    .brand-banner a:hover {{ text-decoration: underline; }}
    .brand-logo {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
    /* Emoji ⚓ → consistent blue (avoids pink/orange OS gradient on emoji) */
    .brand-anchor {{
      font-size: 28px;
      line-height: 1;
      display: inline-block;
      filter: brightness(0) saturate(100%) invert(56%) sepia(98%) saturate(1269%) hue-rotate(172deg) brightness(99%) contrast(93%);
    }}
    .brand-title {{ font-weight: 700; font-size: 20px; color: #fff; display: block; }}
    .brand-tagline {{ font-size: 13px; font-style: italic; color: #8ba3b8; display: block; margin-top: 2px; }}
  </style>
</head>
<body>
  <header class="brand-banner">
    <span class="brand-prefix" style="color: #8b949e; font-size: 13px;">Powered By</span>
    <a href="{TRADE_HARBOUR_URL}" target="_blank" rel="noopener">{TRADE_HARBOUR_LINK_TEXT}</a>
    <span class="brand-logo" style="margin-left: 16px; padding-left: 16px; border-left: 1px solid #30363d;">
      <span class="brand-anchor">⚓</span>
      <span>
        <span class="brand-title">{BRAND_TITLE}</span>
        <span class="brand-tagline">{BRAND_TAGLINE}</span>
      </span>
    </span>
  </header>
  <div class="container">
    <h1>Strategy {strategy_index} – {original_pair or "Scan"}</h1>
    <div class="meta">
      Chart: <a href="{strategy_url}" target="_blank" rel="noopener">{strategy_url}</a>
    </div>
    <div class="info">Showing {num_rows} of {num_rows} pairs</div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>{header_cells}</tr>
        </thead>
        <tbody>{tbody_rows}</tbody>
      </table>
    </div>
  </div>
  <script>
    const tbody = document.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const headers = document.querySelectorAll('th[data-col]');

    function parseVal(cell, type) {{
      const t = cell.textContent.trim();
      if (t === '—' || t === '') return type === 'num' ? -Infinity : '';
      if (type === 'num') {{
        const n = parseFloat(t.replace(/,/g, ''));
        return isNaN(n) ? -Infinity : n;
      }}
      return t.toLowerCase();
    }}

    function sort(colIdx, type) {{
      // Toggle: sorted-desc -> click -> asc; sorted-asc -> click -> desc
      const dir = headers[colIdx].classList.contains('sorted-desc') ? 1 : -1;
      headers.forEach(h => {{ h.classList.remove('sorted-asc', 'sorted-desc'); }});
      headers[colIdx].classList.add(dir === 1 ? 'sorted-asc' : 'sorted-desc');

      rows.sort((a, b) => {{
        const ca = a.cells[colIdx];
        const cb = b.cells[colIdx];
        const va = parseVal(ca, type);
        const vb = parseVal(cb, type);
        if (type === 'num') {{
          if (va === vb) return 0;
          return (va < vb ? -1 : 1) * dir;
        }}
        const cmp = String(va).localeCompare(String(vb));
        return cmp * dir;
      }});

      rows.forEach(r => tbody.appendChild(r));
    }}

    headers.forEach((th, i) => {{
      th.addEventListener('click', () => sort(i, th.dataset.type));
    }});
  </script>
</body>
</html>
"""
    filepath.write_text(html, encoding="utf-8")
    return filepath
