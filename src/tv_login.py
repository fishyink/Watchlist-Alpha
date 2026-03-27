"""
Standalone TradingView login: open browser, wait for user to log in, save session.
Used by the UI "Login" button so scans can run headless afterward.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from .paths import resolve_storage_state_path

logger = logging.getLogger(__name__)


def do_tradingview_login(
    storage_state_path: str,
    login_wait_seconds: int = 90,
    browser_channel: str = "chrome",
) -> bool:
    """
    Open TradingView in visible Chrome. User logs in. Session is saved to storage_state_path.
    Returns True if session was saved.
    """
    resolved = resolve_storage_state_path(storage_state_path)
    if resolved is None:
        logger.error("Invalid storage_state_path: %r", storage_state_path)
        return False
    path = resolved
    path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        opts = {"headless": False, "args": ["--disable-blink-features=AutomationControlled"]}
        if browser_channel:
            opts["channel"] = browser_channel
        browser = p.chromium.launch(**opts)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        page = context.new_page()
        page.goto("https://www.tradingview.com", wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3000)

        for remaining in range(login_wait_seconds, 0, -10):
            time.sleep(min(10, remaining))
            if remaining <= 10:
                break

        time.sleep(3)
        context.storage_state(path=str(path))
        browser.close()
        logger.info("Saved TradingView session to %s", path)
    return path.exists()
