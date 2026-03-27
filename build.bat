@echo off
REM Build Watchlist Scanner as standalone Windows executable.
REM Requires: Python, pip install -r requirements.txt
REM Output: dist/WatchlistScanner/ folder - zip and share that folder.

echo Building Watchlist Scanner...
py -m pip install flet pyinstaller -q
if exist WatchlistScanner.spec (
    py -m PyInstaller WatchlistScanner.spec -y
) else (
    flet pack run_ui.py -n WatchlistScanner -D --add-data "config;config" --hidden-import src --hidden-import src.paths --hidden-import src.db --hidden-import src.queue_worker --hidden-import src.tv_login --hidden-import src.main --hidden-import src.bybit_client --hidden-import src.market_cap --hidden-import src.excel_writer --hidden-import src.html_writer --hidden-import src.scraper --hidden-import src.pass2_filter
)
echo.
echo Done. Output in dist/WatchlistScanner/
echo Share the entire WatchlistScanner folder - users double-click WatchlistScanner.exe
pause
