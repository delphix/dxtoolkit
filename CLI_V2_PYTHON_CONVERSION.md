# cli_v2.py — Python Conversion of cli_v2.sh

## Overview

`cli_v2.py` is a Python port of the bash script `cli_v2.sh`, maintaining feature parity while leveraging Python's strengths for cross-platform compatibility and easier maintenance.

## Key Features

✅ **Config Management**
- Reads/writes JSON (`dxtools.conf`) and CSV config formats
- Automatic backup and restore of original config
- Merges admin/sysadmin rows on demand
- Reuses existing FQDN/port/protocol from config

✅ **Credential Handling**
- Optional `--address`, `--port`, `--protocol` flags to avoid interactive prompts
- Secure `getpass()` for password input when needed
- Smart fallback logic: command-line > existing config > defaults

✅ **Output Organization**
- Two output directories: `analytics/` (time-series) and `misc/` (CSV reports)
- Optional `--preserve-output` flag to keep existing folders
- Automatic cleanup of temp files on exit

✅ **Test Orchestration**
- Runs network latency/throughput tests (supports `win|unix|both`)
- Collects CPU/disk/iSCSI/NFS analytics
- Exports capacity, appliance, storage tests, and system config
- Sysadmin-gated operations (bundle, storage test details, config export)

## Usage

```bash
python3 cli_v2.py -d <engine|all> -t <win|unix|both> -b <dxtoolkit_path> -o <output_dir> \
  [--address <fqdn>] [--port <port>] [--protocol <http|https>] [--preserve-output]
```

### Arguments

| Arg | Required | Description |
|-----|----------|-------------|
| `-d` | ✓ | Engine alias or `all` |
| `-t` | ✓ | Analytics type: `win` \| `unix` \| `both` |
| `-b` | ✓ | Path to dxtoolkit directory (containing dx_* scripts) |
| `-o` | ✓ | Output directory (will be created if missing) |
| `--address` | | FQDN or IP (reuses from config if not provided) |
| `--port` | | Port number (reuses from config if not provided) |
| `--protocol` | | `http` or `https` (reuses from config if not provided) |
| `--preserve-output` | | Keep existing output folders instead of clearing |
| `-h` / `--help` | | Show help message |

### Examples

#### Basic run (interactive password prompts)
```bash
python3 cli_v2.py -d myengine -t unix -b /opt/dxtoolkit -o /mnt/output
```

#### Non-interactive (no prompts)
```bash
python3 cli_v2.py -d myengine -t both \
  --address myengine.example.com --port 443 --protocol https \
  -b /opt/dxtoolkit -o /mnt/output
```

#### Run all engines
```bash
python3 cli_v2.py -d all -t win -b /opt/dxtoolkit -o /mnt/output/daily
```

#### Preserve previous outputs and add to them
```bash
python3 cli_v2.py -d engine1 -t unix -b /opt/dxtoolkit -o /mnt/output --preserve-output
```

## Required dxtoolkit Scripts

The script expects Python versions of these dxtoolkit CLIs in the `-b` directory:

- `dx_config` — Config conversion (JSON ↔ CSV)
- `dx_ctl_network_tests` — Initiate latency/throughput/DSP tests
- `dx_ctl_bundle` — Download/upload support bundles
- `dx_get_analytics` — Retrieve analytics data
- `dx_get_capacity` — Database capacity utilization
- `dx_get_appliance` — Appliance summary (UP/DOWN, versions, counts)
- `dx_get_storage_tests` — Storage test results and IORC generation
- `dx_get_config` — System configuration export (sysadmin-only)

**Note:** The script calls these via shell PATH or direct reference. Ensure they are executable.

## Output Structure

```
<output_dir>/
  analytics/
    <engine>_cpu.csv
    <engine>_disk.csv
    <engine>_iscsi.csv (for win/both)
    <engine>_nfs.csv  (for unix/both)
    <engine>_network.csv
  misc/
    <engine>_NL.csv          # Network latency (last test)
    <engine>_NT.csv          # Network throughput (last test)
    <engine>_capacity.csv    # Database capacity
    <engine>_appliance.csv   # Appliance info
    <engine>_config.csv      # System config (sysadmin)
    iorc_<timestamp>_*.json  # IORC files (sysadmin)
```

## Config Backup & Restore

The script:

1. **Backs up** your original `dxtools.conf` (e.g., `dxtools.conf.orig.2025-12-23.12345.bak`) before modifying
2. **Merges** admin/sysadmin rows into the config (if missing)
3. **Restores** the original config when finished (in the `finally` block)

This ensures your `dxtools.conf` is never permanently altered by the script.

## Differences from Bash Version

### Improvements

- ✅ **Better error handling**: Explicit validation before running tests
- ✅ **Cross-platform**: Runs on Linux, macOS, Windows (with Python 3.7+)
- ✅ **Cleaner code**: More maintainable than shell; easier to debug
- ✅ **No eval**: No shell injection risks; subprocess calls are explicit

### Compatibility Notes

- Python 3.7+ required (uses `pathlib`, `subprocess`, `argparse`)
- Requires same dxtoolkit scripts (Perl or Python versions)
- Same argument semantics as bash version
- Output format unchanged
- Data collected: the Python workflow gathers capacity, appliance, and system config CSVs (via dx_get_capacity/dx_get_appliance/dx_get_config). If you keep running the legacy bash wrapper without these steps, expect fewer files under `misc/`.

## Troubleshooting

### "Missing dx_config"
```
Error: Missing /path/to/dx_config
```
→ Ensure dxtoolkit path is correct and contains required scripts.

### "Can't write to output_dir"
```
Error: Can't write to /mnt/output
```
→ Check permissions on the output directory.

### Config conversion fails
```
Error: dx_config tocsv failed: ...
```
→ Ensure `dx_config` is executable and `dxtools.conf` is valid JSON.

### Type validation
```
Error: -t must be win|unix|both
```
→ Use one of: `win`, `unix`, `both` (lowercase).

### Missing passwords (non-interactive mode)
If you run with `--address`, `--port`, `--protocol` and no matching config exists, the script will add rows with empty passwords. You must ensure the engine is already in dxtools.conf or has valid credentials.

## Development Notes

### Key Functions

| Function | Purpose |
|----------|---------|
| `parse_args()` | CLI argument parsing |
| `validate_args()` | Path and value validation |
| `check_required_files()` | Verify dx_* scripts exist |
| `read_config_json()` / `write_config_json()` | JSON config I/O |
| `read_csv_rows()` / `write_csv_rows()` | CSV config I/O |
| `find_row_by_alias()` | Locate config entry by alias |
| `convert_json_to_csv()` | Use `dx_config` to JSON→CSV |
| `convert_csv_to_dxconf()` | Use `dx_config` to CSV→JSON |
| `run_command()` / `run_command_capture()` | Subprocess execution |

### Customization

To add new tests or output files:

1. Call `run_command()` or `run_command_capture()` with your command
2. Direct output to `misc_dir` or `perf_data`
3. Output files are listed at the end

Example:
```python
print(f"Gathering custom metric -> {misc_dir}/{base}_custom.csv")
returncode, stdout, stderr = run_command_capture(
    f"dx_get_custom {de} -format csv".split(),
    cwd=dx_path,
)
if returncode == 0:
    with open(misc_dir / f"{base}_custom.csv", "w") as f:
        f.write(stdout)
```

## Testing

Run with `--help`:
```bash
python3 cli_v2.py -h
```

Dry-run with invalid path (test validation):
```bash
python3 cli_v2.py -d test -t unix -b /nonexistent -o /tmp/test
# Should fail with: Error: ... is not a directory
```

## Version

**Script Version:** 3.1.0  
**Python Version:** 3.7+  
**Last Updated:** 2025-12-23
