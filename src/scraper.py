"""
TradingView chart scraper - navigates to strategy charts, changes symbols,
and extracts Strategy Tester Performance Summary metrics.
"""
import re
import logging
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

from playwright.sync_api import Page

logger = logging.getLogger(__name__)


class PairStallTimeoutError(RuntimeError):
    """Raised when change_symbol + extract_metrics exceeds the per-pair stall deadline (watchdog)."""


# Strategy Tester "report outdated" — must NOT match random "outdated" in Alerts JSON / sidebar
_REPORT_OUTDATED_RE = re.compile(
    r"\b(?:the\s+)?strategy\s+report\s+is\s+outdated\b|\b(?:the\s+)?report\s+is\s+outdated\b",
    re.I,
)


def _body_suggests_report_outdated(body_text: str) -> bool:
    """True only when Strategy Tester explicitly says the report is outdated (not Alerts JSON)."""
    if not body_text:
        return False
    return bool(_REPORT_OUTDATED_RE.search(body_text))


# Metrics we want: (key, [TradingView label variants])
# TradingView "Metrics" tab uses: Total P&L, Max equity drawdown, Total trades, Profitable trades, Profit factor, etc.
METRIC_LABELS = [
    # TV renames / locales — keep specific strings before vague ones ("P&L" last for net_profit)
    ("net_profit", ["Total P&L", "Net Profit", "Total P/L", "Net profit", "P&L"]),
    ("net_profit_pct", ["Net Profit %", "Net profit %"]),
    ("gross_profit", ["Gross profit", "Total profit", "Gross Profit"]),
    ("gross_loss", ["Gross loss", "Total loss", "Gross Loss"]),
    ("max_drawdown", ["Max equity drawdown", "Max Drawdown", "Max. drawdown"]),
    ("max_drawdown_pct", ["Max Drawdown %", "Max equity drawdown %"]),
    ("sharpe_ratio", ["Sharpe Ratio", "Sharpe"]),
    ("sortino_ratio", ["Sortino Ratio", "Sortino"]),
    ("profit_factor", ["Profit factor", "Profit Factor"]),
    ("total_trades", ["Total trades", "Total Closed Trades", "# Trades"]),
    ("win_rate_pct", ["Profitable trades", "Percent Profitable", "Win rate", "Winning trades", "% Profitable"]),
]
METRIC_KEYS = [k for k, _ in METRIC_LABELS]
METRIC_NAMES = [name for _, names in METRIC_LABELS for name in names]  # Flat list for backward compat


def _parse_metric_value(text: str):
    """Parse metric text to numeric/string value. Returns raw str if unparseable."""
    if not text or text.strip() in ("", "—", "-", "N/A"):
        return None
    # Skip non-numeric labels that can appear after metric names
    skip = ("Commission", "USDT", "USD", "ratio")
    if text.strip() in skip or any(text.strip().startswith(s) for s in skip):
        return None
    if not re.search(r"\d", text):  # No digits - not a valid metric value
        return None
    # Extract number - strip commas; handle unicode minus (−) and plus
    clean = text.strip().replace(",", "").replace("\u2212", "-")
    m = re.search(r"[-+\u2212]?\d+\.?\d*", clean)  # \u2212 = unicode minus (−)
    if m:
        text = m.group(0)
    else:
        text = text.strip().replace(",", "").replace("%", "")
    # Handle K, M suffixes
    mult = 1
    if text.endswith("K"):
        mult = 1_000
        text = text[:-1]
    elif text.endswith("M"):
        mult = 1_000_000
        text = text[:-1]
    try:
        val = float(text) * mult
        return int(val) if val == int(val) else val
    except ValueError:
        return text.strip()


def extract_original_pair_from_url(url: str) -> Optional[str]:
    """Extract symbol from TradingView chart URL, e.g. symbol=BYBIT:BTCUSDT."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    symbol = qs.get("symbol", [None])[0]
    return symbol


# Backtest date range options (matches TradingView dropdown)
BACKTEST_DATE_RANGE_OPTIONS = [
    "range_from_chart",  # Default - use chart's visible range
    "entire_history",
    "last_7_days",
    "last_30_days",
    "last_90_days",
    "last_365_days",
]


class TradingViewScraper:
    def __init__(
        self,
        page: Page,
        delay_after_symbol_sec: float = 5.0,
        symbol_search_timeout_ms: int = 8_000,
        backtest_date_range: str = "range_from_chart",
        debug_screenshot_dir: Optional[Path] = None,
        chart_load_wait_ms: int = 5000,
        backtest_poll_ms: int = 800,
        backtest_stability_ms: int = 1500,
        debug_screenshots_full_page: bool = False,
        prefer_url_symbol_change: bool = True,
        url_symbol_settle_ms: int = 2800,
    ):
        self.page = page
        self.delay_after_symbol_sec = delay_after_symbol_sec
        self.symbol_search_timeout_ms = symbol_search_timeout_ms
        self.backtest_date_range = backtest_date_range
        self._last_chart_url: str | None = None  # for URL fallback when symbol dialog won't open
        self.debug_screenshot_dir = Path(debug_screenshot_dir) if debug_screenshot_dir else None
        self.debug_screenshots_full_page = bool(debug_screenshots_full_page)
        self.chart_load_wait_ms = int(chart_load_wait_ms)
        self.backtest_poll_ms = int(backtest_poll_ms)
        self.backtest_stability_ms = int(backtest_stability_ms)
        self.prefer_url_symbol_change = bool(prefer_url_symbol_change)
        self.url_symbol_settle_ms = int(url_symbol_settle_ms)
        self._screenshot_seq = 0
        self._symbol_apply_seq = 0
        self._pair_deadline: Optional[float] = None  # unix time when current pair must finish; None = off

    def set_pair_deadline(self, when: Optional[float]) -> None:
        """Set wall-clock deadline for the current pair (None disables stall checks)."""
        self._pair_deadline = when

    def _check_pair_deadline(self) -> None:
        d = self._pair_deadline
        if d is not None and time.time() > d:
            raise PairStallTimeoutError("Pair exceeded stall deadline (chart may be hung)")

    def _wait_ms_chunked(self, total_ms: int) -> None:
        """Like wait_for_timeout but honors pair stall deadline between chunks."""
        if total_ms <= 0:
            return
        if self._pair_deadline is None:
            self.page.wait_for_timeout(total_ms)
            return
        chunk = 500
        left = total_ms
        while left > 0:
            self._check_pair_deadline()
            step = min(chunk, left)
            self.page.wait_for_timeout(step)
            left -= step

    def _debug_screenshot(self, tag: str) -> None:
        """PNG when debug_screenshot_dir is set (viewport or full page)."""
        if not self.debug_screenshot_dir:
            return
        try:
            self.debug_screenshot_dir.mkdir(parents=True, exist_ok=True)
            self._screenshot_seq += 1
            safe = re.sub(r"[^\w.\-]+", "_", tag)[:120]
            path = self.debug_screenshot_dir / f"{self._screenshot_seq:04d}_{safe}.png"
            self.page.screenshot(
                path=str(path),
                full_page=self.debug_screenshots_full_page,
                timeout=30_000,
            )
            logger.info("Debug screenshot: %s", path.name)
        except Exception as e:
            logger.debug("Screenshot skipped: %s", e)

    def navigate_and_wait(self, url: str, extra_wait_sec: float = 0) -> bool:
        """Navigate to chart and wait for it to load."""
        self._last_chart_url = url
        logger.info("Navigating to %s", url)
        # Use 'load' not 'networkidle' - TradingView has constant websocket activity, never goes idle
        self.page.goto(url, wait_until="load", timeout=45_000)
        base_wait = self.chart_load_wait_ms
        extra = int(extra_wait_sec * 1000) if extra_wait_sec else 0
        self.page.wait_for_timeout(base_wait + extra)  # Chart + Strategy Tester; extra for deep scan
        # Confirm interval dialog BEFORE Escape — Escape can cancel it and TV may reopen it or stall
        self._dismiss_change_interval_modal()
        self._dismiss_colliding_ui()
        self._dismiss_change_interval_modal()
        self._dismiss_tool_search_palette()
        self._debug_screenshot("after_navigate")
        return True

    def _dismiss_change_interval_modal(self) -> None:
        """
        Close TradingView 'Change interval' dialog — it blocks clicks on the Strategy Report
        and can stall automation (common after symbol change on mismatching timeframes).

        TV often uses custom divs (not role=dialog) and interval chips as div/span, not <button>.
        """
        try:
            body = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            if "change interval" not in body.lower():
                return

            logger.info("Closing 'Change interval' modal (was blocking Strategy Report)")

            # --- 0) Input-field variant: TV often shows this after symbol change even when chart already says 45m
            #     (strategy vs chart resolution). Pre-filled minutes — Enter or primary button confirms. ---
            for attempt in range(3):
                try:
                    box = self.page.locator("div, [role='dialog']").filter(
                        has_text=re.compile(r"Change interval", re.I)
                    ).first
                    if box.count() == 0 or not box.is_visible(timeout=400):
                        break
                    try:
                        box_lower = (box.inner_text(timeout=1000) or "").lower()
                    except Exception:
                        box_lower = ""
                    if "not applicable" in box_lower:
                        logger.info("  Interval modal: Not applicable — repair or dismiss")
                        if self._repair_interval_modal_numeric_input(box):
                            body_chk = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
                            if "change interval" not in body_chk.lower():
                                return
                        if self._dismiss_interval_modal_escape_cancel():
                            return
                    # Primary actions (TV sometimes uses OK / Apply instead of Enter)
                    for btn_pat in (
                        r"^OK$",
                        r"^Apply$",
                        r"^Done$",
                        r"^Confirm$",
                        r"^Continue$",
                    ):
                        try:
                            ok = box.get_by_role("button", name=re.compile(btn_pat, re.I)).first
                            if ok.count() > 0 and ok.is_visible(timeout=400):
                                ok.click(timeout=3000)
                                self.page.wait_for_timeout(1000)
                                logger.info("  Confirmed interval (%s)", btn_pat.strip("^$"))
                                return
                        except Exception:
                            pass
                    inp = box.locator("input").first
                    if inp.count() > 0 and inp.is_visible(timeout=400):
                        val = (inp.input_value() or "").strip()
                        if val and re.match(r"^\d+$", val):
                            inp.click()
                            self.page.wait_for_timeout(300)
                            self.page.keyboard.press("Enter")
                            self.page.wait_for_timeout(400)
                            self.page.keyboard.press("Enter")  # Some builds need second confirm
                            self.page.wait_for_timeout(1000)
                            logger.info("  Confirmed interval (input): %s", val)
                            return
                        if val and not re.match(r"^\d+$", val):
                            if self._repair_interval_modal_numeric_input(box):
                                body_chk = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
                                if "change interval" not in body_chk.lower():
                                    return
                except Exception:
                    pass
                self.page.wait_for_timeout(500)  # Modal may render with delay

            # --- 0b) JS fallback: find input, focus, submit via Enter ---
            submitted = self.page.evaluate(
                """
                () => {
                    let best = null;
                    let bestLen = Infinity;
                    document.querySelectorAll('div, section, [role="dialog"]').forEach(el => {
                        const t = el.innerText || '';
                        if (!t.includes('Change interval')) return;
                        if (t.length >= bestLen || t.length > 600) return;
                        bestLen = t.length;
                        best = el;
                    });
                    if (!best) return null;
                    const inp = best.querySelector('input[type="text"], input[type="number"], input:not([type])');
                    if (inp && /^\\d+$/.test(inp.value)) {
                        inp.focus();
                        inp.click();
                        return inp.value;
                    }
                    return null;
                }
                """
            )
            if submitted:
                self.page.wait_for_timeout(200)
                self.page.keyboard.press("Enter")
                self.page.wait_for_timeout(1200)
                logger.info("  Confirmed interval (input): %s", submitted)
                return

            # --- 1) Playwright: dialog + buttons ---
            skip_labels = {"cancel", "close", "×", "✕"}
            preferred_order = (
                "45",
                "60",
                "240",
                "30",
                "15",
                "120",
                "180",
                "360",
                "720",
                "1",
                "5",
                "D",
                "W",
            )

            def _click_interval_candidates(btns_locator) -> bool:
                n = btns_locator.count()
                candidates: list[tuple[int, str]] = []
                for i in range(n):
                    b = btns_locator.nth(i)
                    try:
                        if not b.is_visible(timeout=300):
                            continue
                    except Exception:
                        continue
                    label = (b.text_content() or "").strip()
                    ll = label.lower()
                    if not label or ll in skip_labels:
                        continue
                    if len(label) == 1 and label.upper() in ("X",):
                        continue
                    if not self._interval_modal_chip_label_ok(label):
                        continue  # e.g. BYBIT:10 — causes "Not applicable"
                    candidates.append((i, label))
                if len(candidates) == 1:
                    btns_locator.nth(candidates[0][0]).click(timeout=5000)
                    self.page.wait_for_timeout(1000)
                    logger.info("  Confirmed interval: %s", candidates[0][1])
                    return True
                labels_seen = {lbl for _, lbl in candidates}
                for pref in preferred_order:
                    if pref not in labels_seen:
                        continue
                    for i, lbl in candidates:
                        if lbl == pref:
                            btns_locator.nth(i).click(timeout=5000)
                            self.page.wait_for_timeout(1000)
                            logger.info("  Confirmed interval: %s", lbl)
                            return True
                if candidates:
                    btns_locator.nth(candidates[0][0]).click(timeout=5000)
                    self.page.wait_for_timeout(1000)
                    logger.info("  Confirmed interval: %s", candidates[0][1])
                    return True
                return False

            for sel in (
                self.page.get_by_role("dialog").filter(
                    has=self.page.get_by_text(re.compile(r"Change interval", re.I))
                ),
                self.page.locator('[role="dialog"]').filter(
                    has_text=re.compile(r"Change interval", re.I)
                ),
            ):
                if sel.count() > 0 and sel.first.is_visible(timeout=500):
                    box = sel.first
                    for role in ("button",):
                        if _click_interval_candidates(box.get_by_role(role)):
                            return
                    # divs acting as chips
                    for chip_sel in (
                        'button',
                        '[role="button"]',
                        'div[class*="button"]',
                        'span[class*="button"]',
                        '[class*="interval"]',
                        '[class*="timeframe"]',
                    ):
                        try:
                            loc = box.locator(chip_sel)
                            if loc.count() > 0 and _click_interval_candidates(loc):
                                return
                        except Exception:
                            continue
                    break

            # --- 2) JS: smallest container with "Change interval", click numeric chip ---
            clicked = self.page.evaluate(
                """
                () => {
                    const skip = new Set(['cancel','close','×','✕','x']);
                    let best = null;
                    let bestLen = Infinity;
                    document.querySelectorAll('div, section, article, [role="dialog"]').forEach(el => {
                        const t = el.innerText || '';
                        if (!t.includes('Change interval') && !t.includes('Change Interval')) return;
                        if (t.length >= bestLen || t.length > 800) return;
                        bestLen = t.length;
                        best = el;
                    });
                    if (!best) return null;
                    const selectors = [
                        'button', '[role="button"]', 'a[role="button"]',
                        'div[tabindex="0"]', 'span[tabindex="0"]',
                        'div[class*="button"]', 'span[class*="button"]',
                        '[class*="chip"]', '[class*="item-"]'
                    ];
                    const nodes = [];
                    selectors.forEach(s => best.querySelectorAll(s).forEach(n => nodes.push(n)));
                    const preferred = ['45','60','240','30','15','120','180','360','720','1','5','D','W'];
                    const byText = {};
                    for (const n of nodes) {
                        const txt = (n.innerText || '').trim();
                        if (!txt || skip.has(txt.toLowerCase())) continue;
                        if (!/^\\d{1,4}$/.test(txt) && !/^[1-9][hHdDmMwW]$/.test(txt)) continue;
                        byText[txt] = n;
                    }
                    for (const p of preferred) {
                        if (byText[p]) {
                            byText[p].click();
                            return p;
                        }
                    }
                    const keys = Object.keys(byText).sort((a,b) => a.length - b.length);
                    if (keys.length === 1) {
                        byText[keys[0]].click();
                        return keys[0];
                    }
                    return null;
                }
                """
            )
            if clicked:
                self.page.wait_for_timeout(1200)
                logger.info("  Confirmed interval (DOM): %s", clicked)
                return

            # --- 3) Last resort: visible "45" / "60" in page when modal text exists ---
            for txt in ("45", "60", "30", "15", "240"):
                try:
                    el = self.page.get_by_text(txt, exact=True).first
                    if el.count() > 0 and el.is_visible(timeout=400):
                        # Only if we're still in interval context (avoid chart labels)
                        if self.page.get_by_text(re.compile(r"Change interval", re.I)).count() > 0:
                            el.click(timeout=3000)
                            self.page.wait_for_timeout(1000)
                            logger.info("  Confirmed interval (fallback click): %s", txt)
                            return
                except Exception:
                    continue

            # --- 3b) Still stuck: invalid chip (BYBIT:10) / Not applicable — repair input, then Cancel/Escape
            body_chk2 = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            if "change interval" in body_chk2.lower():
                try:
                    sb = self.page.locator("div, [role='dialog']").filter(
                        has_text=re.compile(r"Change interval", re.I)
                    ).first
                    if sb.count() > 0 and sb.is_visible(timeout=500):
                        self._repair_interval_modal_numeric_input(sb)
                except Exception:
                    pass
                self._dismiss_interval_modal_escape_cancel()

        except Exception as e:
            logger.debug("Change interval modal: %s", e)

    def _interval_modal_chip_label_ok(self, label: str) -> bool:
        """True if label looks like a timeframe chip, not a symbol (e.g. BYBIT:10 → Not applicable)."""
        if not label:
            return False
        s = label.strip()
        sl = s.lower()
        if sl in ("cancel", "close", "×", "✕", "x", "ok", "apply", "done"):
            return False
        if "bybit" in sl or "binance" in sl or "usdt" in sl or ":" in s:
            return False
        if re.match(r"^\d{1,4}$", s):
            return True
        if re.match(r"^[1-9]\s*[hdmw]", sl):
            return True  # 1h, 4h, 1d, 1w
        if re.match(r"^\d+\s*[hdmw]$", sl):
            return True
        return False

    def _guess_chart_interval_minutes(self) -> Optional[int]:
        """Infer chart timeframe (e.g. 45m) from UI text before the modal; avoids wrong chip clicks."""
        try:
            body = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            low = body.lower()
            cut = low.find("change interval")
            head = body[:cut] if cut > 0 else body[:5000]
            found = re.findall(r"\b(\d{1,4})\s*m\b", head, re.I)
            for cand in reversed(found):
                n = int(cand)
                if 1 <= n <= 1440:
                    return n
            if re.search(r"\b1\s*h\b", head, re.I):
                return 60
            if re.search(r"\b4\s*h\b", head, re.I):
                return 240
            if re.search(r"\b1\s*d\b", head, re.I):
                return 1440
        except Exception:
            pass
        return None

    def _dismiss_interval_modal_escape_cancel(self) -> bool:
        """Close stuck modal (Not applicable, invalid chip) via Cancel/Close or Escape."""
        try:
            box = self.page.locator("div, [role='dialog']").filter(
                has_text=re.compile(r"Change interval", re.I)
            ).first
            if box.count() == 0 or not box.is_visible(timeout=400):
                return False
            for pat in (
                r"^Cancel$",
                r"^Close$",
                r"^Dismiss$",
                r"^Got it$",
                r"^Back$",
            ):
                try:
                    b = box.get_by_role("button", name=re.compile(pat, re.I)).first
                    if b.count() > 0 and b.is_visible(timeout=400):
                        b.click(timeout=3000)
                        self.page.wait_for_timeout(600)
                        logger.info("  Closed interval modal (%s)", pat.strip("^$"))
                        return True
                except Exception:
                    pass
            for _ in range(4):
                self.page.keyboard.press("Escape")
                self.page.wait_for_timeout(350)
            body = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            if "change interval" not in body.lower():
                logger.info("  Closed interval modal (Escape)")
                return True
        except Exception:
            pass
        return False

    def _repair_interval_modal_numeric_input(self, box) -> bool:
        """Replace invalid interval field (e.g. BYBIT:10) with minutes read from chart (e.g. 45)."""
        try:
            inp = box.locator("input").first
            if inp.count() == 0 or not inp.is_visible(timeout=500):
                return False
            val = (inp.input_value() or "").strip()
            if re.match(r"^\d+$", val):
                return False
            mins = self._guess_chart_interval_minutes()
            if not mins:
                return False
            inp.click()
            inp.fill("")
            inp.fill(str(mins))
            self.page.wait_for_timeout(250)
            self.page.keyboard.press("Enter")
            self.page.wait_for_timeout(900)
            logger.info("  Repaired interval input → %s (from chart toolbar)", mins)
            return True
        except Exception:
            return False

    def _dismiss_colliding_ui(self) -> None:
        """
        Close Alerts, Symbol Search, tool panels, etc. They persist on saved layouts and can:
        - Steal focus from symbol search / Strategy Tester
        - Put the word 'outdated' in page text (JSON) and break backtest-wait logic
        """
        try:
            for _ in range(4):
                self.page.keyboard.press("Escape")
                self.page.wait_for_timeout(180)
        except Exception:
            pass

    def get_original_pair_from_chart(self) -> Optional[str]:
        """Try to read current symbol from chart UI. Fallback: None."""
        # TradingView shows symbol in top-left (e.g. "DOGEUSDT.P" or "BYBIT:BTCUSDT.P")
        selectors = [
            '[data-name="symbol-search"]',
            '[class*="symbolSearch"]',
            '[class*="symbol-search"]',
            'button[data-name="header-chart-panel-symbol"]',
            'div[data-role="symbol"]',
        ]
        for sel in selectors:
            try:
                el = self.page.locator(sel).first
                if el.count() > 0:
                    text = el.text_content(timeout=2000)
                    if text and "USDT" in text.upper():
                        return text.strip()
            except Exception:
                continue
        # Fallback: symbol in chart header (first ~800 chars to avoid sidebar alerts)
        try:
            body = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            header = body[:800]
            m = re.search(r"(BYBIT:\w+USDT\.?P?|\w+USDT\.?P?)(?:\s|Perpetual|\n|$)", header)
            if m:
                return m.group(1).strip()
        except Exception:
            pass
        return None

    def _save_debug_screenshot(self, name: str) -> None:
        """Save screenshot to output/ for debugging when date range setup fails."""
        try:
            from pathlib import Path
            out = Path("output")
            out.mkdir(exist_ok=True)
            path = out / f"debug_{name}.png"
            self.page.screenshot(path=path)
            logger.info("Debug screenshot saved: %s", path)
        except Exception as e:
            logger.debug("Could not save screenshot: %s", e)

    def set_backtest_date_range(self) -> bool:
        """
        Set Strategy Tester date range (e.g. Entire history for deep backtest).
        Call after navigate_and_wait. Skips if backtest_date_range is 'range_from_chart'.
        Expands collapsed Properties panel if needed.
        """
        if self.backtest_date_range == "range_from_chart":
            return True

        # Map config value to TradingView dropdown label
        label_map = {
            "entire_history": "Entire history",
            "last_7_days": "Last 7 days",
            "last_30_days": "Last 30 days",
            "last_90_days": "Last 90 days",
            "last_365_days": "Last 365 days",
        }
        target_label = label_map.get(self.backtest_date_range)
        if not target_label:
            logger.warning("Unknown backtest_date_range: %s", self.backtest_date_range)
            return False

        try:
            # Extra wait when setting date range - Strategy Tester may render slowly
            self.page.wait_for_timeout(3000)
            # Scroll to Strategy Tester (bottom)
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            self.page.wait_for_timeout(1500)

            # Expand Properties panel if collapsed - click "Properties" to expand date range section
            for expand_text in ["Properties", "Inputs", "Backtesting"]:
                try:
                    expand_el = self.page.get_by_text(expand_text, exact=True).first
                    if expand_el.count() > 0:
                        expand_el.click(timeout=3000)
                        self.page.wait_for_timeout(800)
                        break
                except Exception:
                    continue

            # Expand date range wrapper if collapsed - JS to remove any collapsed class
            try:
                self.page.evaluate("""
                    () => {
                        document.querySelectorAll('[class*="dateRange"]').forEach(el => {
                            el.className = el.className.replace(/\\bcollapsed[-\\w]*\\b/g, '').trim();
                            el.style.display = '';
                        });
                    }
                """)
                self.page.wait_for_timeout(800)
            except Exception:
                pass

            # Also try clicking the collapsed wrapper to expand (toggles on click)
            try:
                collapsed = self.page.locator('[class*="dateRange"][class*="collapsed"]').first
                if collapsed.count() > 0:
                    collapsed.click(force=True, timeout=3000)
                    self.page.wait_for_timeout(600)
            except Exception:
                pass

            # Locate date range selector - click the date display (e.g. "Jan 5, 2026 – Mar 19, 2026")
            date_clicked = False
            # First: click element showing the date range (top-right of Strategy Report)
            for pattern in [
                re.compile(r"Jan \d{1,2}, \d{4}.*Mar \d{1,2}, \d{4}"),
                re.compile(r"\w{3} \d{1,2}, \d{4}\s*[–—-]\s*\w{3} \d{1,2}, \d{4}"),
                re.compile(r"\d{4}.*[–—-].*\d{4}"),
            ]:
                try:
                    el = self.page.get_by_text(pattern).first
                    if el.count() > 0:
                        el.scroll_into_view_if_needed(timeout=3000)
                        self.page.wait_for_timeout(300)
                        el.click(timeout=5000)
                        date_clicked = True
                        logger.info("Clicked date range display")
                        break
                except Exception:
                    pass
                if date_clicked:
                    break

            if not date_clicked:
                for sel in [
                    'button:has-text("Range from chart")',
                    'div:has-text("Range from chart")',
                    '[class*="dateRange"]:not([class*="collapsed"])',
                    '[class*="dateRange"]',
                ]:
                    try:
                        el = self.page.locator(sel).first
                        if el.count() > 0:
                            el.scroll_into_view_if_needed(timeout=3000)
                            self.page.wait_for_timeout(300)
                            el.click(timeout=5000)
                            date_clicked = True
                            break
                    except Exception:
                        try:
                            el = self.page.locator(sel).first
                            if el.count() > 0:
                                el.click(force=True, timeout=3000)
                                date_clicked = True
                                break
                        except Exception:
                            continue

            if not date_clicked:
                # Fallback: click element with date pattern (Jan 1, 2023 — Mar 18, 2026)
                try:
                    el = self.page.get_by_text(re.compile(r"\d{4}.*—.*\d{4}")).first
                    if el.count() > 0:
                        el.scroll_into_view_if_needed(timeout=3000)
                        el.click(timeout=5000)
                        date_clicked = True
                except Exception:
                    pass

            if not date_clicked:
                # Last resort: JS programmatic click on date range - bypasses visibility check
                try:
                    clicked = self.page.evaluate("""
                        () => {
                            const el = document.querySelector('[class*="dateRange"]');
                            if (el) {
                                el.click();
                                return true;
                            }
                            const btns = Array.from(document.querySelectorAll('button, div[role="button"], [class*="button"]'));
                            const rangeBtn = btns.find(e => e.textContent && e.textContent.includes('Range from chart'));
                            if (rangeBtn) {
                                rangeBtn.click();
                                return true;
                            }
                            return false;
                        }
                    """)
                    if clicked:
                        date_clicked = True
                        self.page.wait_for_timeout(500)
                except Exception:
                    pass

            if not date_clicked:
                logger.warning("Could not find date range selector")
                self._save_debug_screenshot("date_range_selector")
                return False

            self.page.wait_for_timeout(2000)  # Wait for dropdown to open and render

            # Click "Entire history" - TradingView: role="menuitemcheckbox" aria-label="Entire history"
            option_clicked = False
            try:
                option = self.page.get_by_role("menuitemcheckbox", name=target_label).first
                option.wait_for(state="visible", timeout=5000)
                option.click(timeout=5000)
                option_clicked = True
            except Exception:
                pass
            if not option_clicked:
                try:
                    option = self.page.locator(f'[aria-label="{target_label}"]').first
                    if option.count() > 0:
                        option.click(timeout=5000)
                        option_clicked = True
                except Exception:
                    pass
            if not option_clicked:
                try:
                    option = self.page.get_by_text(target_label, exact=True).first
                    option.click(timeout=5000)
                    option_clicked = True
                except Exception:
                    try:
                        option = self.page.get_by_text(target_label, exact=True).first
                        option.click(force=True, timeout=3000)
                        option_clicked = True
                    except Exception:
                        # JS click fallback: aria-label or text content
                        clicked = self.page.evaluate(f"""
                        () => {{
                            const byAria = document.querySelector('[aria-label="{target_label}"]');
                            if (byAria) {{ byAria.click(); return true; }}
                            const opts = Array.from(document.querySelectorAll('[role="menuitemcheckbox"]'));
                            const el = opts.find(e => e.textContent?.trim() === '{target_label}' || e.getAttribute('aria-label') === '{target_label}');
                            if (el) {{ el.click(); return true; }}
                            return false;
                        }}
                    """)
                    option_clicked = bool(clicked)

            if not option_clicked:
                logger.warning("Could not click '%s' in dropdown", target_label)
                self._save_debug_screenshot("entire_history_option")
                return False

            self.page.wait_for_timeout(int(self.delay_after_symbol_sec * 1000))
            logger.info("Set backtest date range to: %s", target_label)
            return True
        except Exception as e:
            logger.warning("Failed to set backtest date range: %s", e)
            return False

    def _dismiss_tool_search_palette(self) -> None:
        """
        Close TradingView 'Search tool or function' (drawings / settings palette).
        Opened by Ctrl+K or '/' — must never be used for changing symbol; blocks the chart.
        """
        try:
            body = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            low = body.lower()
            if "search tool or function" not in low and "type to search for drawings" not in low:
                return
            logger.info("Closing 'Search tool or function' palette (was blocking chart)")
            for _ in range(3):
                self.page.keyboard.press("Escape")
                self.page.wait_for_timeout(200)
        except Exception:
            pass

    def _light_escape_panels(self) -> None:
        """Close transient overlays without aggressive Escape (avoids wrong dialogs vs full _dismiss_colliding_ui)."""
        try:
            for _ in range(2):
                self.page.keyboard.press("Escape")
                self.page.wait_for_timeout(120)
        except Exception:
            pass

    def _symbol_search_input_visible(self, timeout_ms: int = 3000) -> bool:
        try:
            self.page.locator('input[data-name="symbol-search-input"]').first.wait_for(
                state="visible", timeout=timeout_ms
            )
            return True
        except Exception:
            return False

    def _open_symbol_search_dialog(self) -> bool:
        """
        Open symbol search via header only. Do NOT use Ctrl+K — on TradingView that opens
        'Search tool or function' (command palette), not the symbol dialog.
        """
        def _verify_opened() -> bool:
            self.page.wait_for_timeout(350)
            return self._symbol_search_input_visible(3500)

        # Prefer exact TradingView hooks (try even if not "visible" — headless / overlays lie)
        header_attempts = [
            self.page.locator('button[data-name="header-chart-panel-symbol"]'),
            self.page.locator('[data-name="symbol-search"]'),
            self.page.locator('[data-name="header-toolbar-symbol-search"]'),
        ]
        for loc in header_attempts:
            try:
                if loc.count() == 0:
                    continue
                target = loc.first
                try:
                    target.scroll_into_view_if_needed(timeout=2000)
                except Exception:
                    pass
                self.page.wait_for_timeout(120)
                try:
                    if target.is_visible(timeout=1200):
                        target.click(timeout=5000)
                    else:
                        target.click(timeout=5000, force=True)
                except Exception:
                    target.click(timeout=5000, force=True)
                if _verify_opened():
                    return True
            except Exception:
                continue

        # JS: same elements, bypass Playwright visibility (headless / stacking)
        try:
            clicked = self.page.evaluate("""
                () => {
                    const btn = document.querySelector('button[data-name="header-chart-panel-symbol"]');
                    if (btn) { btn.click(); return 'header-chart-panel-symbol'; }
                    const wrap = document.querySelector('[data-name="symbol-search"]');
                    if (wrap) {
                        const b = wrap.querySelector('button') || wrap;
                        b.click();
                        return 'symbol-search';
                    }
                    return null;
                }
            """)
            if clicked and _verify_opened():
                logger.debug("Opened symbol search via JS (%s)", clicked)
                return True
        except Exception:
            pass

        # Top layout strip only — never page-wide USDT (Alerts / lists)
        for top_sel in (
            '[class*="layout__area--top"]',
            '[class*="toolbar-chart"]',
            '[class*="header-chart-panel"]',
        ):
            try:
                top = self.page.locator(top_sel).first
                if top.count() == 0:
                    continue
                sym = top.get_by_text(re.compile(r"\w+USDT\.?P?", re.I)).first
                if sym.count() > 0:
                    sym.click(timeout=5000)
                    if _verify_opened():
                        return True
            except Exception:
                continue

        # Header pixel probes (toolbar Y ~40–70 at 1920 viewport)
        for xy in ((160, 52), (240, 52), (320, 52), (200, 68), (280, 68)):
            try:
                self.page.mouse.click(xy[0], xy[1])
                self.page.wait_for_timeout(400)
                if _verify_opened():
                    return True
            except Exception:
                continue

        logger.warning("Could not open symbol search from header; chart layout may differ")
        return False

    @staticmethod
    def _symbol_key_for_compare(symbol: str) -> str:
        """Normalize BYBIT:BTCUSDT.P vs URL-encoded variants for equality."""
        s = unquote((symbol or "").strip()).upper().replace(" ", "")
        if ":" in s:
            s = s.split(":", 1)[1]
        if s.endswith(".P"):
            s = s[:-2]
        return s

    def _browser_url_symbol_key(self) -> Optional[str]:
        try:
            qs = parse_qs(urlparse(self.page.url).query)
            raw = (qs.get("symbol") or [None])[0]
            if not raw:
                return None
            return self._symbol_key_for_compare(unquote(raw))
        except Exception:
            return None

    def _already_showing_symbol(self, symbol: str) -> bool:
        have = self._browser_url_symbol_key()
        want = self._symbol_key_for_compare(symbol)
        return bool(have and want and have == want)

    def _change_symbol_via_chart_url(self, symbol: str) -> bool:
        """
        Navigate to same layout with ?symbol=. Keeps interval= and other query params.
        Skips reload if the address bar already matches (avoids double-load / same-pair flash).
        """
        if not self._last_chart_url:
            return False
        try:
            if self._already_showing_symbol(symbol):
                logger.info("Chart URL already on %s — skip duplicate load", symbol[:44])
                self._dismiss_change_interval_modal()
                self._dismiss_tool_search_palette()
                return True

            parsed = urlparse(self._last_chart_url)
            qs = parse_qs(parsed.query)
            qs["symbol"] = [symbol]
            new_query = urlencode(qs, doseq=True, safe="")
            new_url = urlunparse(
                (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
            )
            logger.info("Changing symbol via chart URL: %s", symbol[:44])
            self.page.goto(new_url, wait_until="load", timeout=60_000)
            self._last_chart_url = new_url
            self._wait_ms_chunked(self.url_symbol_settle_ms)
            self._dismiss_change_interval_modal()
            self._dismiss_colliding_ui()
            self._dismiss_change_interval_modal()
            self._dismiss_tool_search_palette()
            return True
        except Exception as e:
            logger.warning("URL symbol change failed: %s", e)
            return False

    def _get_symbol_search_input(self):
        """Return locator for symbol dialog input only — never chat / tool search."""
        dialog = self.page.locator('[data-name="symbol-search-dialog"]')
        if dialog.count() > 0:
            inp = dialog.locator('input[data-name="symbol-search-input"]').first
            if inp.count() > 0:
                return inp
        inp = self.page.locator('input[data-name="symbol-search-input"]').first
        if inp.count() > 0:
            return inp
        # Fallback: visible symbol placeholder, still not chat
        for sel in (
            'input[placeholder*="Symbol or"]',
            'input[placeholder*="Symbol"]',
            '[class*="symbolSearch"] input',
            '[class*="symbol-search"] input',
        ):
            try:
                loc = self.page.locator(sel).first
                if loc.count() > 0 and loc.is_visible(timeout=400):
                    ph = (loc.get_attribute("placeholder") or "").lower()
                    if "chat" in ph or ("find" in ph and "symbol" not in ph):
                        continue
                    return loc
            except Exception:
                continue
        return self.page.locator('input[data-name="symbol-search-input"]').first

    def change_symbol(self, symbol: str) -> bool:
        """
        Change chart symbol. Default: one chart URL navigation (?symbol=) — no header dialog,
        so we avoid a long failed UI attempt then a second URL change (same pair / double wait).
        Falls back to header search only if URL change is disabled or fails.
        symbol: e.g. BYBIT:BTCUSDT.P
        """
        self._dismiss_tool_search_palette()
        self._dismiss_change_interval_modal()
        self._light_escape_panels()
        self._dismiss_change_interval_modal()
        self._check_pair_deadline()

        used_url = False
        if self.prefer_url_symbol_change and self._last_chart_url:
            used_url = self._change_symbol_via_chart_url(symbol)

        opened = False
        if not used_url:
            opened = self._open_symbol_search_dialog()
            if not opened:
                self.page.wait_for_timeout(400)
                try:
                    self.page.evaluate("""
                        () => {
                            const btn = document.querySelector('button[data-name="header-chart-panel-symbol"]');
                            if (btn) { btn.click(); return true; }
                            const w = document.querySelector('[data-name="symbol-search"]');
                            if (w) { (w.querySelector('button') || w).click(); return true; }
                            return false;
                        }
                    """)
                    self.page.wait_for_timeout(450)
                    opened = self._symbol_search_input_visible(2800)
                except Exception:
                    pass

            if not opened:
                if not self._change_symbol_via_chart_url(symbol):
                    logger.error("Symbol change failed: header search and chart URL both failed")
                    return False

        if opened:
            input_el = self._get_symbol_search_input()
            input_el.wait_for(state="visible", timeout=self.symbol_search_timeout_ms)
            input_el.click()
            self.page.wait_for_timeout(200)
            input_el.fill("")
            try:
                input_el.press_sequentially(symbol, delay=28)
            except Exception:
                input_el.fill(symbol)
            self.page.wait_for_timeout(850)

            picked = False
            try:
                menu = self.page.locator(
                    '[data-name="symbol-search-dialog"] [data-name="symbol-menu-item"]'
                ).first
                if menu.count() > 0 and menu.is_visible(timeout=1500):
                    menu.click(timeout=4000)
                    picked = True
            except Exception:
                pass
            if not picked:
                for sel in (
                    '[data-name="symbol-menu-item"]',
                    '[data-name="symbol-search-dialog"] [role="option"]',
                    '[role="option"]',
                ):
                    try:
                        opt = self.page.locator(sel).first
                        if opt.count() > 0 and opt.is_visible(timeout=800):
                            opt.click(timeout=4000)
                            picked = True
                            break
                    except Exception:
                        continue
            if not picked:
                self.page.wait_for_timeout(400)
                self.page.keyboard.press("Enter")

            self.page.wait_for_timeout(350)
            self._dismiss_change_interval_modal()

        # Initial fixed wait
        self._wait_ms_chunked(int(self.delay_after_symbol_sec * 1000))

        # Click "Update report" when report is outdated (required when changing pairs)
        self._click_update_report_if_needed()

        # Critical: wait for TradingView backtest to finish - "Updating report" disappears when done
        self._wait_for_backtest_ready(symbol)
        self._symbol_apply_seq += 1
        slug = symbol.split(":")[-1] if ":" in symbol else symbol
        self._debug_screenshot(f"pair_{self._symbol_apply_seq:04d}_{slug}")
        return True

    def _click_update_report_if_needed(self) -> None:
        """Click 'Update report' when 'The report is outdated' appears after symbol change."""
        try:
            # Scroll to bottom where the button appears
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            self.page.wait_for_timeout(320)
            for btn_text in ["Update report", "Update Report"]:
                btn = self.page.get_by_role("button", name=re.compile(btn_text, re.I)).first
                if btn.count() > 0:
                    btn.click(timeout=3000)
                    logger.info("Clicked '%s' (report was outdated)", btn_text)
                    self.page.wait_for_timeout(1500)
                    return
            # Fallback: get_by_text
            btn = self.page.get_by_text("Update report", exact=False).first
            if btn.count() > 0:
                btn.click(timeout=3000)
                logger.info("Clicked 'Update report' (report was outdated)")
                self.page.wait_for_timeout(1500)
        except Exception:
            pass  # No "Update report" visible - report may already be current

    def _wait_for_backtest_ready(self, expected_symbol: str, timeout_sec: int = 45) -> None:
        """Wait for Strategy Report to finish updating. Clicks 'Update report' if outdated, polls until done."""
        poll_interval_ms = self.backtest_poll_ms
        stability_ms = self.backtest_stability_ms
        elapsed = 0
        while elapsed < timeout_sec * 1000:
            self._check_pair_deadline()
            try:
                self._dismiss_tool_search_palette()
                self._dismiss_change_interval_modal()
                body_text = self.page.evaluate("() => document.body ? document.body.innerText : ''") or ""
                # If report is outdated, click Update report (narrow match — Alerts JSON contains "outdated" too)
                if _body_suggests_report_outdated(body_text):
                    self._click_update_report_if_needed()
                    self.page.wait_for_timeout(2000)  # Give time for recalculation to start
                    elapsed += 2000
                    continue
                if "Updating report" not in body_text:
                    self.page.wait_for_timeout(stability_ms)
                    logger.debug("Backtest ready for %s (no 'Updating report')", expected_symbol)
                    return
            except Exception:
                pass
            self.page.wait_for_timeout(poll_interval_ms)
            elapsed += poll_interval_ms
        logger.warning("Timeout waiting for backtest ready (still saw 'Updating report'?) - proceeding anyway")

    def _ensure_performance_summary_tab(self) -> bool:
        """Click 'Metrics' tab (not 'List of trades') to show Net Profit, Sharpe, etc. Waits for content to load."""
        # TradingView Strategy Report: default is "List of trades" - must switch to "Metrics"
        tab_clicked = False
        tab_texts = ["Metrics", "Performance Summary", "Performance summary", "Overview", "Summary", "Stats"]
        # Prefer real tabs in Strategy Tester (avoid get_by_text matching chart overlays like "Key metrics")
        for txt in tab_texts:
            self._check_pair_deadline()
            try:
                tabs = self.page.get_by_role("tab", name=re.compile(f"^{re.escape(txt)}$", re.I))
                n = tabs.count()
                if n == 0:
                    continue
                # If multiple (rare), prefer the last visible — often bottom Strategy Tester vs other panels
                for i in range(n - 1, -1, -1):
                    t = tabs.nth(i)
                    try:
                        if t.is_visible(timeout=500):
                            t.click(timeout=3000)
                            tab_clicked = True
                            break
                    except Exception:
                        continue
                if tab_clicked:
                    break
            except Exception:
                pass
        if not tab_clicked:
            for txt in tab_texts:
                try:
                    tab = self.page.get_by_role("tab", name=re.compile(txt, re.I)).last
                    if tab.count() > 0:
                        tab.click(timeout=3000)
                        tab_clicked = True
                        break
                except Exception:
                    pass
        if not tab_clicked:
            return False
        # Wait for loading spinner - Metrics content has "Total P&L" or "Profit factor"
        try:
            self.page.get_by_text("Total P&L", exact=False).first.wait_for(state="visible", timeout=15_000)
            self.page.wait_for_timeout(1100)  # Metrics grid often paints after tab switch
        except Exception:
            self.page.wait_for_timeout(3000)  # Fallback: fixed wait
        return True

    def extract_metrics(self) -> dict:
        """
        Extract Performance Summary metrics from Strategy Tester.
        Returns dict with METRIC_KEYS; missing values are None.
        """
        result = {k: None for k in METRIC_KEYS}

        # Scroll Strategy Tester (bottom panel) into view
        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            self.page.wait_for_timeout(320)
        except Exception:
            pass

        self._dismiss_tool_search_palette()
        self._dismiss_change_interval_modal()
        self._ensure_performance_summary_tab()
        self._check_pair_deadline()

        def _extract_from_text(text: str) -> None:
            """Parse metric values from block of text. Uses METRIC_LABELS (key, label variants)."""
            for key, labels in METRIC_LABELS:
                if result.get(key) is not None:
                    continue
                for name in labels:
                    if name not in text:
                        continue
                    parts = text.split(name, 1)
                    if len(parts) < 2:
                        continue
                    rest = parts[1].strip().lstrip(": \t")  # Handle "Metric: value" format
                    # Special: Max equity drawdown has "1,176.03 USD 41.98%" - strip commas so regex captures full %
                    if name in ("Max equity drawdown", "Max Drawdown") and result.get("max_drawdown_pct") is None:
                        rest_norm = rest.replace(",", "").replace("\u2212", "-").replace("\u2013", "-")
                        pct_m = re.search(r"(?:USD|USDT)\s*([+-]?\d+\.?\d*)\s*%", rest_norm[:200])
                        if not pct_m:
                            pct_m = re.search(r"([+-]?\d+\.?\d*)\s*%", rest_norm[:200])
                        if pct_m:
                            pct_val = _parse_metric_value(pct_m.group(1).replace("%", ""))
                            if pct_val is not None:
                                result["max_drawdown_pct"] = pct_val
                    # Special: Total P&L has "+1,175.44 USD +117.54%" or "+1,197%" - strip commas so regex captures full %
                    if name in ("Total P&L", "Net Profit", "Total P/L") and result.get("net_profit_pct") is None:
                        rest_norm = rest.replace(",", "").replace("\u2212", "-").replace("\u2013", "-")
                        pct_m = re.search(r"(?:USD|USDT)\s*([+-]?\d+\.?\d*)\s*%?", rest_norm)
                        if not pct_m:
                            # Fallback: any percentage (e.g. −44.86% or +117.54%) in Total P&L block
                            pct_m = re.search(r"[\d,.]+\s*(?:USD|USDT)\s*([+-]?\d+\.?\d*)\s*%", rest_norm[:300])
                        if not pct_m:
                            pct_m = re.search(r"([+-]?\d+\.?\d*)\s*%", rest_norm[:200])
                        if pct_m:
                            pct_val = _parse_metric_value(pct_m.group(1).replace("%", ""))
                            if pct_val is not None:
                                result["net_profit_pct"] = pct_val
                    # Value: number, "1.2K", "45%", "—". Stop at currency/metric labels
                    for stop in ("\n", "\t", "  ", "USDT", "USD", "Gross", "Net ", "Max ", "Sharpe",
                                "Sortino", "Profit", "Total", "Percent", "Equity", "Commission", "—"):
                        if stop in rest:
                            rest = rest.split(stop)[0].strip()
                    val_str = rest.split()[0] if rest else ""
                    # Handle "58.55% 89/152" -> extract first number (with % or decimal)
                    if val_str and not val_str.startswith("/"):
                        # Strip "ratio" prefix for Sharpe/Sortino (e.g. "ratio0.816")
                        if val_str.startswith("ratio"):
                            val_str = val_str[5:]
                        # Win rate "68.87104/151" -> take digits before / or %
                        num_match = re.search(r"[-+]?[\d,]+\.?\d*%?", val_str)
                        if num_match:
                            val_str = num_match.group(0)
                        parsed = _parse_metric_value(val_str)
                        if parsed is not None:
                            result[key] = parsed
                            # Max equity drawdown: also extract % from same block - strip commas for full %
                            if key == "max_drawdown" and name in ("Max equity drawdown", "Max Drawdown") and result.get("max_drawdown_pct") is None:
                                pct_str = parts[1].replace(",", "").replace("\u2212", "-")
                                pct_m = re.search(r"[\d.]+\s*(?:USD|USDT)\s*([+-]?\d+\.?\d*)\s*%", pct_str)
                                if not pct_m:
                                    pct_m = re.search(r"([+-]?\d+\.?\d*)\s*%", pct_str[:200])
                                if pct_m:
                                    pct_val = _parse_metric_value(pct_m.group(1).replace("%", ""))
                                    if pct_val is not None:
                                        result["max_drawdown_pct"] = pct_val
                            # Special: Total P&L line has "amount USD +117.54%" or "+1,197%" - strip commas for full %
                            if key == "net_profit" and name in ("Total P&L", "Net Profit", "Total P/L"):
                                if result.get("net_profit_pct") is None:
                                    pct_str = parts[1].replace(",", "").replace("\u2212", "-")
                                    pct_m = re.search(r"(?:USD|USDT)\s*([+\-\u2212]?\d+\.?\d*%?)", pct_str)
                                    if pct_m:
                                        pct_val = _parse_metric_value(pct_m.group(1).replace("%", ""))
                                        if pct_val is not None:
                                            result["net_profit_pct"] = pct_val
                                # TV sometimes shows amount without minus (red styling); infer sign from %
                                if result.get("net_profit_pct") is not None and result["net_profit_pct"] < 0 and parsed > 0:
                                    result[key] = -parsed
                            break

        # Strategy 1: get_by_text for each label — prefer visible matches (headless often has hidden duplicates)
        try:
            for key, labels in METRIC_LABELS:
                if result.get(key) is not None:
                    continue
                for name in labels:
                    loc = self.page.get_by_text(name, exact=False)
                    n = loc.count()
                    if n == 0:
                        continue
                    for i in range(min(n, 15)):
                        self._check_pair_deadline()
                        try:
                            node = loc.nth(i)
                            if not node.is_visible(timeout=600):
                                continue
                        except Exception:
                            continue
                        try:
                            parent = node.locator("xpath=..")
                            if parent.count() > 0:
                                text = parent.first.text_content(timeout=2000) or ""
                                if name in text:
                                    _extract_from_text(text)
                                    if result.get(key) is not None:
                                        break
                            if result.get(key) is None:
                                grandparent = node.locator("xpath=../..")
                                if grandparent.count() > 0:
                                    text = grandparent.first.text_content(timeout=2000) or ""
                                    _extract_from_text(text)
                        except Exception:
                            continue
                        if result.get(key) is not None:
                            break
                    if result.get(key) is not None:
                        break
        except Exception as e:
            logger.debug("extract strategy 1: %s", e)

        # Strategy 2: Whole Strategy Tester panel - look for backtest/summary container
        panel_selectors = [
            '[class*="report"]',
            '[class*="summary"]',
            '[class*="backtest"]',
            '[class*="strategyTester"]',
            '[class*="performanceSummary"]',
            'table',
            '[role="table"]',
            '[role="grid"]',
        ]
        full_text = ""
        for sel in panel_selectors:
            try:
                els = self.page.locator(sel).all()
                for ei, el in enumerate(els[:12]):
                    if ei % 3 == 0:
                        self._check_pair_deadline()
                    t = el.text_content() or ""
                    if ("Total P&L" in t or "Net Profit" in t or "Profit factor" in t or "Total trades" in t
                            or "Gross profit" in t or "Gross loss" in t or "Total profit" in t or "Total loss" in t
                            or "Max equity drawdown" in t or "Max Drawdown" in t):
                        full_text += "\n" + t
            except Exception:
                continue
        if full_text:
            _extract_from_text(full_text)

        # Strategy 2b: ARIA rows (TV often uses role=row for the metrics grid)
        if any(v is None for v in result.values()):
            try:
                for ri, row in enumerate(self.page.get_by_role("row").all()[:120]):
                    if ri % 12 == 0:
                        self._check_pair_deadline()
                    try:
                        text = row.text_content(timeout=800) or ""
                        if len(text) < 8:
                            continue
                        if any(
                            m in text
                            for m in (
                                "Total P&L",
                                "Net Profit",
                                "Profit factor",
                                "Sharpe",
                                "Total trades",
                                "Max ",
                                "drawdown",
                            )
                        ):
                            _extract_from_text(text)
                    except Exception:
                        continue
                    if all(v is not None for v in result.values()):
                        break
            except Exception:
                pass

        # Strategy 3: Any element containing multiple metrics (row-like)
        if any(v is None for v in result.values()):
            try:
                all_divs = self.page.locator("div, tr, li").all()
                for di, el in enumerate(all_divs[:320]):
                    if di % 24 == 0:
                        self._check_pair_deadline()
                    text = el.text_content() or ""
                    if ("Total P&L" in text or "Net Profit" in text or "Profit factor" in text
                            or "Gross profit" in text or "Gross loss" in text or "Total profit" in text or "Total loss" in text
                            or "Max equity drawdown" in text or "Max Drawdown" in text):
                        _extract_from_text(text)
                    if all(v is not None for v in result.values()):
                        break
            except Exception:
                pass

        # Strategy 4: Full page visible text via JS (handles complex DOM)
        if any(v is None for v in result.values()):
            try:
                self._check_pair_deadline()
                body_text = self.page.evaluate("() => document.body ? document.body.innerText : ''")
                if body_text and ("Total P&L" in body_text or "Profit factor" in body_text or "Net Profit" in body_text
                        or "Gross profit" in body_text or "Gross loss" in body_text or "Total profit" in body_text or "Total loss" in body_text):
                    _extract_from_text(body_text)
            except Exception:
                pass

        # Debug: when all None, dump page text to inspect structure
        if all(v is None for v in result.values()):
            try:
                from pathlib import Path
                dump = self.page.evaluate("() => document.body ? document.body.innerText : ''")
                out_dir = Path("output")
                out_dir.mkdir(exist_ok=True)
                (out_dir / "debug_metrics_dump.txt").write_text(dump or "(empty)", encoding="utf-8")
                logger.info("Debug: saved page text to output/debug_metrics_dump.txt (metrics were all None)")
            except Exception:
                pass

        return result


def scrape_strategy_for_symbols(
    page: Page,
    chart_url: str,
    symbols: list[str],
    delay_sec: float = 5.0,
) -> tuple[Optional[str], list[dict]]:
    """
    For one strategy URL: navigate, get original pair, then for each symbol
    change symbol, extract metrics, return (original_pair, list of {symbol, metrics}).
    """
    scraper = TradingViewScraper(page, delay_after_symbol_sec=delay_sec)
    scraper.navigate_and_wait(chart_url)
    original = extract_original_pair_from_url(chart_url) or scraper.get_original_pair_from_chart()
    results = []
    for sym in symbols:
        try:
            scraper.change_symbol(sym)
            metrics = scraper.extract_metrics()
            results.append({"symbol": sym, **metrics})
        except Exception as e:
            logger.warning("Failed for %s: %s", sym, e)
            results.append({"symbol": sym, "error": str(e), **{k: None for k in METRIC_KEYS}})
    return original, results
