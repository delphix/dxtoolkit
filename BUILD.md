# Build Guide

## Prerequisites
- Python 3.10+ on the target OS
- `pip` available
- For Windows `.exe` builds: run natively in PowerShell/cmd (not WSL) so PyInstaller emits PE binaries

## Install Dependencies
```bash
pip install -r requirements.txt
```

## Build Standalone Binaries (PyInstaller)
Run on each platform you need binaries for; artifacts land in `dist/`.

### macOS/Linux
```bash
tools/build_binaries.sh
```

### Windows (PowerShell)
```powershell
pip install -r requirements.txt
bash tools/build_binaries.sh
```
If Git Bash is unavailable, run PyInstaller directly:
```powershell
py -m PyInstaller --onefile --distpath dist --workpath build/pyinstaller --specpath build/pyinstaller bin/cli_v2.py
py -m PyInstaller --onefile --distpath dist --workpath build/pyinstaller --specpath build/pyinstaller bin/dx_get_analytics.py
```

## Environment Overrides
- `PYTHON_BIN` (default: `python3`)
- `DIST_DIR` (default: `dist`)
- `WORK_DIR` (default: `build/pyinstaller`)

## Outputs
- Binaries are written to `dist/` per platform (macOS/Linux ELF, Windows EXE).
