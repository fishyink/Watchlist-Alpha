# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['run_ui.py'],
    pathex=[],
    binaries=[],
    datas=[('config', 'config'), ('src', 'src'), ('ui', 'ui')],
    hiddenimports=[
        'src',
        'src.paths',
        'src.db',
        'src.queue_worker',
        'src.tv_login',
        'src.main',
        'src.bybit_client',
        'src.market_cap',
        'src.excel_writer',
        'src.html_writer',
        'src.scraper',
        'src.pass2_filter',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='WatchlistScanner',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version='C:\\Users\\fishy\\AppData\\Local\\Temp\\dd85616f-ffb6-4554-9d89-424fe20d3b47',
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='WatchlistScanner',
)
