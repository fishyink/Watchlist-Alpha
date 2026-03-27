# TradingView Bybit Strategy Scanner

Scans TradingView strategy charts against every Bybit USDT perpetual pair, recording metrics (PnL, drawdown, Sharpe, Sortino, win rate, etc.) in Excel.

## Requirements

- **TradingView Plus** or higher (Strategy Tester requires paid plan)
- Python 3.10+
- Chromium (via Playwright)

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Config

Edit `config/config.yaml`:

1. **strategies**: List of chart URLs. Each URL must have a **strategy already applied** on the chart. Copy the chart link from TradingView after adding your strategy.
2. Optional `name` per strategy for cleaner Excel filenames.

Example:

```yaml
strategies:
  - url: "https://www.tradingview.com/chart/xxxxx/?symbol=BYBIT:BTCUSDT"
    name: "ma_crossover"
  - url: "https://www.tradingview.com/chart/yyyyy/?symbol=BYBIT:ETHUSDT"
    name: "rsi_reversal"
```

## Desktop UI

**For non-technical users:**
- **Option A**: Double-click `run.bat` (requires Python installed)
- **Option B**: After building (see below), double-click `WatchlistScanner.exe` in the built folder

**From command line:**
```bash
python run_ui.py
```

Opens a desktop app with:

- **Queue**: Add/edit chart URLs, optional dtech link, deep backtest toggle, all pairs vs top 300 market cap
- **Runs**: Start/stop the queue, view progress, resume after restart
- **Results**: Browse and open output xlsx/html files

Queue state is stored in `data/watchlist.db`. Use "Import from config" to migrate existing `config.yaml` strategies into the queue.

### Building a standalone executable (for distribution)

Run `build.bat` (or `flet pack run_ui.py -n WatchlistScanner -D --add-data "config;config"`). This creates `dist/WatchlistScanner/` with:
- `WatchlistScanner.exe` — double-click to run
- `config/` — editable settings
- `output/` — created when you run scans
- `data/` — queue and run history

**To share with others:** Zip the entire `WatchlistScanner` folder. Recipients unzip, double-click the exe. They need Chrome installed and a TradingView account.

## CLI Run

```bash
# Test mode: 4 pairs (BTC, ETH, SOL, DOGE) - validate first
python run_scan.py

# Full run: all Bybit USDT perp pairs
python run_scan.py --full

# Custom config
python run_scan.py --config my_config.yaml --test
```

## Output

- **One Excel file per strategy**
- Row 1: Strategy link (clickable), Original pair
- Rows 2+: Symbol, Net Profit, Drawdown, Sharpe, Sortino, Win Rate, etc.
- Saved after each pair (crash-safe)

## Phase 1 and Phase 2 (Pass 2)

- **Phase 1** (`run_scan.py`): Chart date range → `output/strategy_NN_*_scan_*.xlsx` (+ HTML).
- **Phase 2** (`run_deep_scan.py`): Filters Pass 1 (see `src/pass2_filter.py`), re-runs **Entire history** on survivors → `*_deep_scan_*` files.

Strategy index **N** is **1-based** (first chart in `config.yaml` = `--strategy 1`).

Example for the 5th strategy in config:

```bash
python run_scan.py --full --strategy 5
python run_deep_scan.py --strategy 5
```

Run Phase 2 only after Phase 1 has produced the latest `strategy_05_*_scan_*.xlsx` for that chart.

## Notes

- Run with `headless: false` first to debug (default in config)
- If symbol search fails, TradingView may have changed their UI - check selectors in `src/scraper.py`
- Use `delay_between_symbols_sec` (e.g. 5) if you hit rate limits
