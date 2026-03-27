@echo off
REM Install Python packages and Playwright Chromium (run once after clone / download).
cd /d "%~dp0"

py -3 --version >nul 2>&1
if errorlevel 1 (
    echo Python was not found. Install Python 3.10+ from https://www.python.org/downloads/
    echo Enable "Add Python to PATH" or use the "py" launcher from the installer.
    pause
    exit /b 1
)

echo Installing packages from requirements.txt ...
py -3 -m pip install -r requirements.txt
if errorlevel 1 (
    echo pip install failed.
    pause
    exit /b 1
)

echo.
echo Installing Playwright Chromium (large download) ...
py -3 -m playwright install chromium
if errorlevel 1 (
    echo playwright install failed.
    pause
    exit /b 1
)

echo.
echo Done. You can run run.bat or: py -3 run_ui.py
pause
