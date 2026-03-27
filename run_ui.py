#!/usr/bin/env python3
"""
Watchlist — desktop UI (Trade-Harbour).
Usage: python run_ui.py

Window size: starts maximized by default. In config/config.yaml set:
  ui:
    window_start: maximized   # or fullscreen, or normal
"""
from __future__ import annotations

import sys
from pathlib import Path

# When packaged (PyInstaller), modules are in sys._MEIPASS
if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
    _project_root = Path(sys._MEIPASS)
else:
    _project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_project_root))

import flet as ft
from ui.app import main

if __name__ == "__main__":
    ft.app(target=main)
