# Watchlist Alpha

<p align="center">
  <a href="https://trade-harbour.com.au/tools?tool=watchlist-alpha"><strong>See Watchlist Alpha in action — Trade-Harbour demo</strong></a>
  <br /><br />
  <sub>TradingView Strategy Tester automation · Bybit USDT perpetuals · queue, run, export to Excel</sub>
</p>

## What it does

Watchlist Alpha is a **Windows desktop app** (with an optional **command-line** mode for automation) that uses your **TradingView Plus** (or higher) plan and **automates Strategy Tester** for charts you already set up.

You add **TradingView chart links** where **your strategy is on the chart** and the date range is as you want it. The tool then:

- **Cycles through Bybit USDT perpetual pairs** (or a **top 300 by market cap** list, if you choose that instead of “all pairs”).
- **Runs or refreshes the backtest** for each symbol and **reads the metrics** TradingView shows (e.g. net profit, drawdown, Sharpe, Sortino, win rate, and related stats).
- **Exports results to Excel** (one workbook pattern per strategy run), with **incremental saves** so long runs survive crashes. HTML output can also be produced depending on your workflow.

The **desktop UI** gives you a **queue** (line up many charts), **runs** (start/stop, progress, resume after restart), and a **results** area to open output files. You can do the same scanning **without the UI** via `run_scan.py` / `run_deep_scan.py` if you prefer scripts.

---

## What you need

| | |
|--|--|
| **TradingView** | **Plus** or higher (Strategy Tester requires a paid plan). |
| **Windows** | Desktop app targets Windows (Python install or packaged `.exe`). |
| **Browser** | **Google Chrome** is recommended (`browser_channel: "chrome"` in config helps with Google sign-in). Playwright’s Chromium is used if you don’t use Chrome. |
| **Python** | `3.10+` only when running from source. Not needed if you use a shared **`WatchlistScanner.exe`** build. |

---

## Install once, then run

| Step | What to do |
|------|------------|
| **1. Python** | Install from [python.org](https://www.python.org/downloads/) and enable **Add to PATH** (or ensure the **`py`** launcher works). |
| **2. Dependencies** | Double-click **`install_prerequisites.bat`** *once*. It runs `pip install -r requirements.txt` and `playwright install chromium`. |
| **3. App** | Double-click **`run.bat`** whenever you want to open the UI (`run_ui.py` is the same program from a terminal). |

`run.bat` only starts the app — it does not install packages. Use **`install_prerequisites.bat`** after cloning or updating dependencies.

---

## Log in to TradingView (before your first real run)

The app needs a saved TradingView session to drive the browser reliably — especially if you want **headless** runs (no window).

1. Open the app (**`run.bat`** or `python run_ui.py`).
2. Go to the **Runs** tab.
3. Click **Step 1 — Log into TradingView**. A **visible** Chrome window opens (this step is never headless).
4. Sign in to TradingView as you normally would (email, Google, etc.). You have about **90 seconds** by default; if that’s tight, raise **`login_wait_seconds`** in `config/config.yaml`.
5. Wait until the status line says something like **Session saved** (or check that **`config/tv_session.json`** exists). That file is your logged-in cookies — keep it private; do not commit it to git.

If login times out, click **Step 1** again and complete sign-in faster, or increase **`login_wait_seconds`**.

---

## Headless: when to show the browser vs hide it

| Mode | What it’s for |
|------|----------------|
| **Visible browser** (`Run headless` **OFF** on the Runs tab) | Easiest way to **see** what TradingView is doing: first-time setup, debugging, or if a run gets stuck (captcha, layout change, session expired). |
| **Headless** (`Run headless` **ON**) | **After** you’ve saved a session with Step 1 — runs without popping a window, good for long queues. The app blocks **Start** in headless mode until you’re logged in (saved session). |

**Practical order:**

1. Install prerequisites → open app → **Step 1 — Log into TradingView** → confirm session saved.  
2. Run a **short** job with **headless OFF** and confirm pairs complete and **`output/`** gets new files.  
3. Turn **headless ON** for longer unattended runs.

For **CLI** scans (`run_scan.py` / `run_deep_scan.py`), `config/config.yaml` has **`headless: true`** by default. Set **`headless: false`** (or use flags where available) until you’re confident the session file works, then switch to headless for routine runs.

---

## Packaged app (no Python)

If you **build** or **receive** **`WatchlistScanner.exe`**

1. Unzip the **entire** `WatchlistScanner` folder.  
2. Run **`WatchlistScanner.exe`**.  
3. Still do **Step 1 — Log into TradingView** on the Runs tab before headless queue runs.  
4. Users still need a **TradingView** paid plan and **Chrome** (or your configured browser channel).

To build: run **`build.bat`**, then zip **`dist\WatchlistScanner`**. Details below.

---

## Using the desktop app

| Area | Purpose |
|------|--------|
| **Queue** | Add or edit chart URLs, optional fields, deep backtest option, all pairs vs **top 300** by market cap. |
| **Runs** | Step 1 login, **headless** toggle, **Start** / **Stop**, progress, resume after restart. |
| **Results** | Browse and open **`.xlsx`** / **`.html`** outputs. |

Queue and history live in **`data/watchlist.db`**. To pull strategies from YAML into the queue, use **Import from config** in the app.

---

## Chart URLs

Each entry must be a **TradingView chart** URL with **your strategy already on the chart**.

1. Open the chart, attach the strategy, set parameters.  
2. Copy the browser URL.  
3. Paste into the **Queue** (or under **`strategies`** in `config/config.yaml`).

```yaml
strategies:
  - url: "https://www.tradingview.com/chart/xxxxx/?symbol=BYBIT:BTCUSDT"
    name: "ma_crossover"
  - url: "https://www.tradingview.com/chart/yyyyy/?symbol=BYBIT:ETHUSDT"
    name: "rsi_reversal"
```

More options (delays, browser channel, paths) are in **`config/config.yaml`**.

---

## Where results go

- **One Excel file per strategy** (typical columns: symbol, net profit, drawdown, Sharpe, Sortino, win rate, …).  
- **`output/`** next to the app (or inside the unzipped `WatchlistScanner` folder).  
- Progress is written **incrementally** so an interrupted run keeps completed rows.

---

## Command line (no UI)

After installing prerequisites:

```bash
python run_scan.py          # small test (few pairs)
python run_scan.py --full   # all Bybit USDT perps
```

Two-phase / Pass 2 workflows: [Advanced: CLI & two-phase scans](#advanced-cli--two-phase-scans).

---

## Troubleshooting

| Issue | What to try |
|-------|-------------|
| **Headless won’t start** | Do **Step 1** until session is saved; or turn **headless OFF** and run once visibly. |
| **Stuck / no progress** | **Stop** → turn **headless OFF** → **Start** to see the browser; re-run **Step 1** if TradingView signed you out. |
| **Rate limits / flakiness** | Increase **`delay_between_symbols_sec`** in `config/config.yaml` (e.g. `5`). |
| **UI / selectors break** | TradingView changes pages sometimes — may need updates in `src/scraper.py`. |

---

## Advanced: CLI & two-phase scans

**Phase 1** — `run_scan.py`: chart date range → `output/strategy_NN_*_scan_*.xlsx` (+ HTML).  

**Phase 2** — `run_deep_scan.py`: filters Phase 1, re-runs **entire history** on survivors → `*_deep_scan_*`. See `src/pass2_filter.py`.

Strategy index **N** is **1-based** (first chart in config = `1`).

```bash
python run_scan.py --full --strategy 5
python run_deep_scan.py --strategy 5
```

Run Phase 2 only after Phase 1 produced the latest `strategy_05_*_scan_*.xlsx` for that chart.

```bash
python run_scan.py --config my_config.yaml --test
```

---

## Building a standalone app

```bash
flet pack run_ui.py -n WatchlistScanner -D --add-data "config;config"
```

Or double-click **`build.bat`**. Output: **`dist/WatchlistScanner/`** with `WatchlistScanner.exe`, `config/`, and folders for `output/` and `data/` at runtime. Zip the **whole** folder for distribution.

---

## For contributors

- Debug with **visible** browser and **`headless: false`** until flows are stable.  
- Login / session: `src/tv_login.py`, `config/tv_session.json` (gitignored).  
- Selector issues: `src/scraper.py`.
