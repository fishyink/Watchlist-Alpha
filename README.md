# Watchlist Alpha

<p align="center">
  <a href="https://trade-harbour.com.au/tools?tool=watchlist-alpha"><strong>See Watchlist Alpha on Trade-Harbour (demo)</strong></a>
  <br /><br />
  <sub>TradingView Strategy Tester automation · Bybit USDT perpetuals · queue, run, export to Excel</sub>
</p>

You give it TradingView chart links with your strategy already on the chart. It walks through Bybit USDT perpetual pairs, pulls Strategy Tester numbers, and drops them into Excel. Built for Windows.

---

## What this does

- **Windows desktop app** is the main path. Queue charts, run batches, open results in the UI.
- **CLI** (`run_scan.py`, `run_deep_scan.py`) is optional if you want scripts or automation.
- You paste **chart URLs** where the strategy is already loaded. Date range comes from what you set on the chart.
- It scans **Bybit USDT perp** symbols (or a **top 300 market cap** slice if you choose that in the queue).
- It reads **Strategy Tester** metrics from TradingView (profit, drawdown, ratios, win rate, and the rest).
- It saves to **Excel** in `output/`, one file pattern per strategy run, with incremental saves so a crash does not wipe progress.

---

## Quick start

Do these in order the first time.

1. Install **Python 3.10+** from [python.org](https://www.python.org/downloads/). Enable **Add to PATH** or confirm `py` works in a terminal.
2. Double-click **`install_prerequisites.bat`** once. Wait until it finishes (pip + Playwright Chromium).
3. Double-click **`run.bat`** to open the app.
4. Open the **Runs** tab. Use the **Log into TradingView** step (Step 1 on that screen). Finish sign-in in the browser window. Wait until the app shows a saved session (see **First run checklist** below).
5. Add one chart in the **Queue**, then start a **small** run with **headless off** so you can see the browser. Confirm rows land in **`output/`**.
6. After that works, turn **headless** on if you want long unattended runs. The app expects a saved session before headless **Start**.

`run.bat` only launches the UI. **`install_prerequisites.bat`** is what installs dependencies.

---

## What you need

| | |
|--|--|
| **TradingView** | Paid plan that includes Strategy Tester (Plus or higher). |
| **Windows** | Desktop workflow is built for Windows. |
| **Chrome** | Recommended. `browser_channel: "chrome"` in `config/config.yaml` helps with Google sign-in. |
| **Python** | Only if you run from source. Not needed if you use a packaged **`WatchlistScanner.exe`**. |

---

## First run checklist

- [ ] Strategy is already on the chart in TradingView, range set how you want it.
- [ ] Copy the **chart URL** from the browser (not just a symbol page).
- [ ] Paste into **Queue** in the app (or import from `config/config.yaml` if you use YAML).
- [ ] **Runs** tab: TradingView **login step** completed, session file present (see `storage_state_path` in `config/config.yaml`, default `config/tv_session.json`). Do not commit that file.
- [ ] First scan is **small** and **visible** (headless off) so you know it is alive.
- [ ] Open **`output/`** and confirm a new **`.xlsx`**.

---

## Results

- Roughly **one Excel file per strategy** run pattern (see filenames in `output/`).
- Everything lands under **`output/`** next to the app (or next to the exe if you use a build).
- Data is written **as pairs finish**, not a single save at the end.

---

## Headless mode

- **Visible:** use when you set things up, debug, or TradingView kicks you to a login wall.
- **Headless:** use after **Step 1** saved a session. Good for long queues with no window.

The **Runs** tab has the **Run headless** toggle. If headless **Start** refuses to run, you are missing a saved session. Do **Step 1** again or run visible once.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| **Headless will not start** | Complete **Step 1** so `tv_session.json` exists. Or turn headless off and run once visible. |
| **Stuck / no progress** | **Stop**, headless **off**, **Start** again and watch the browser. Check `delay_between_symbols_sec` in `config/config.yaml` if it feels rushed. |
| **TradingView signed out** | Run **Step 1** again. Session files expire or get invalidated like any browser login. |
| **Layout broke / scraper fails** | TradingView changes the DOM. Fixes live in `src/scraper.py`. No shame, they move buttons. |
| **Rate limits / flaky runs** | Raise **`delay_between_symbols_sec`** in `config/config.yaml` (try `5`). |

---

## Advanced

**Packaged exe**

Build with **`build.bat`**. You get `dist/WatchlistScanner/` with **`WatchlistScanner.exe`**, a `config/` folder, and runtime `output/` and `data/`. Zip the **whole** folder for someone else. They still need TradingView paid + Chrome. They still do **Step 1** before trusting headless.

**CLI scans**

After the same prerequisite install:

```bash
python run_scan.py          # few pairs, quick sanity check
python run_scan.py --full   # all Bybit USDT perps in config
```

Custom config:

```bash
python run_scan.py --config my_config.yaml --test
```

**Two-phase scans**

Phase 1: `run_scan.py` writes `output/strategy_NN_*_scan_*.xlsx`. Phase 2: `run_deep_scan.py` filters Phase 1, then deep pass. Strategy index **N** is **1-based** (first chart in `config.yaml` is `1`).

```bash
python run_scan.py --full --strategy 5
python run_deep_scan.py --strategy 5
```

Only run Phase 2 after Phase 1 produced the matching `strategy_05_*_scan_*.xlsx`.

**Standalone pack command**

```bash
flet pack run_ui.py -n WatchlistScanner -D --add-data "config;config"
```

**Config example** (`config/config.yaml`)

```yaml
strategies:
  - url: "https://www.tradingview.com/chart/xxxxx/?symbol=BYBIT:BTCUSDT"
    name: "ma_crossover"
```

**Maintainers**

When debugging, use visible browser and `headless: false` in `config/config.yaml` until stable. Queue state lives in **`data/watchlist.db`**.
