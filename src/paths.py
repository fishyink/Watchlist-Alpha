"""
Resolve app root for both development and packaged (frozen) builds.
When packaged with PyInstaller, use the executable's directory so config/output/data
live next to the exe for easy access.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path


def get_app_root() -> Path:
    """Root directory for config, output, data. Works when run from source or as packaged exe."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def get_bundle_root() -> Path | None:
    """When frozen, path where PyInstaller extracts bundled files. None when not frozen."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return None


def resolve_storage_state_path(storage_state_path: str | None) -> Path | None:
    """
    Config uses a relative path (e.g. config/tv_session.json). That must be resolved against
    app root — not process cwd — or scans won't find the file after Step 1 login (which saves
    under PROJECT_ROOT).
    """
    if storage_state_path is None or not str(storage_state_path).strip():
        return None
    p = Path(str(storage_state_path).strip())
    if not p.is_absolute():
        p = get_app_root() / p
    try:
        return p.resolve()
    except OSError:
        return p


def ensure_config() -> Path:
    """Ensure config exists. When frozen, copy from bundle on first run. Returns config dir."""
    root = get_app_root()
    config_dir = root / "config"
    config_yaml = config_dir / "config.yaml"

    if not config_yaml.exists():
        bundle = get_bundle_root()
        if bundle and (bundle / "config" / "config.yaml").exists():
            shutil.copytree(bundle / "config", config_dir)
        else:
            config_dir.mkdir(parents=True, exist_ok=True)
            if not config_yaml.exists():
                config_yaml.write_text(
                    "# Watchlist Scanner\nstrategies: []\noutput_dir: output\nheadless: false\n"
                    "pause_for_manual_login: true\nlogin_wait_seconds: 90\nstorage_state_path: config/tv_session.json\n"
                    "browser_channel: chrome\nphase1_market_cap_top_n: 300\nmarket_cap_provider: coingecko\n",
                    encoding="utf-8",
                )
    return config_dir
