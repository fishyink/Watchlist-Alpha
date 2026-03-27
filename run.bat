@echo off
REM Double-click to run Watchlist Scanner (requires Python installed)
cd /d "%~dp0"
py run_ui.py
if errorlevel 1 (
    echo.
    echo Python not found or error. Install Python from python.org
    pause
)
