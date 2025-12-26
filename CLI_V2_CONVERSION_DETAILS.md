# Bash → Python Conversion Reference

## Feature Mapping

| Feature | Bash (`cli_v2.sh`) | Python (`cli_v2.py`) |
|---------|-------------------|----------------------|
| **Help** | `showhelp()` function, `cat <<EOF` | `show_help()` function, f-string |
| **Arg Parsing** | Manual `while` loop with `case` | `argparse.ArgumentParser` |
| **Path Handling** | `cd` command, `${VAR}` substitution | `Path` from `pathlib`, `os.chdir()` |
| **JSON I/O** | Via `dx_config` CLI | `json.load()` / `json.dump()` |
| **CSV I/O** | Via `dx_config` CLI | `csv.DictReader` / manual write |
| **Error Handling** | `set -euo pipefail`, `trap cleanup EXIT` | Try/finally with cleanup function |
| **Password Input** | `read -r -s -p` prompt | `getpass.getpass()` |
| **Subprocess** | Shell command execution (`$()`, `\|`) | `subprocess.run()` with `capture_output` |
| **Date Formatting** | `date '+%Y-%m-%d'` | `datetime.now().strftime()` |
| **Directory Cleanup** | `rm -rf` | `shutil.rmtree()` with `ignore_errors` |
| **Backup/Restore** | Named file backup, `mv -f` restore | Pathlib with explicit restore in cleanup |

## Code Snippets: Before & After

### Help Display

**Bash:**
```bash
showhelp() {
  cat <<EOF
Script Version ${scriptVersion}
...
EOF
}
```

**Python:**
```python
def show_help():
    """Display help message."""
    print(f"""Script Version {SCRIPT_VERSION}
...
""")
```

### Argument Parsing

**Bash:**
```bash
while [[ $# -gt 0 ]]; do
  case "$1" in
    -d) DE_READ="${2:-}"; shift 2 ;;
    -t) DE_TYPE="${2:-}"; shift 2 ;;
    --address) ADDR_OPT="${2:-}"; shift 2 ;;
    *) echo "Invalid option: $1"; exit 1 ;;
  esac
done
```

**Python:**
```python
parser = argparse.ArgumentParser(prog="cli_v2.py", add_help=False)
parser.add_argument("-d", dest="engine", required=True)
parser.add_argument("-t", dest="type", required=True)
parser.add_argument("--address", dest="address", default="")
args = parser.parse_args()
```

### Config Backup & Restore

**Bash:**
```bash
BACKUP=""
cleanup() {
  if [[ -n "${BACKUP}" && -f "${BACKUP}" ]]; then
    mv -f "${BACKUP}" "${DCC}"
  fi
}
trap cleanup EXIT

# Later:
if [[ -f "${DCC}" ]]; then
  BACKUP="${DCC}.orig.${DATE}.$$.bak"
  cp -p "${DCC}" "${BACKUP}"
fi
```

**Python:**
```python
backup_file = None

def cleanup():
    nonlocal backup_file
    if backup_file and backup_file.exists():
        shutil.copy2(backup_file, config_file)
        backup_file.unlink()

try:
    # ... do work ...
finally:
    cleanup()

# Later:
if config_file.exists():
    backup_file = config_file.with_suffix(f".orig.{date_str}.{os.getpid()}.bak")
    shutil.copy2(config_file, backup_file)
```

### Finding Config Row

**Bash:**
```bash
read_row() {
  local alias="$1"
  local line
  line="$(grep -m1 "^${alias}," "${CSV}" || true)"
  if [[ -n "${line}" ]]; then
    IFS=',' read -r _ IP PORT USER PASS ENC PROTO <<< "${line}"
    echo "${IP},${PORT},${USER},${PASS},${ENC},${PROTO}"
  fi
}
ADMIN_ROW="$(read_row "${BASE}")" || true
```

**Python:**
```python
def find_row_by_alias(rows: list, alias: str) -> Optional[Dict]:
    """Find first row matching alias."""
    for row in rows:
        if row.get("alias") == alias:
            return row
    return None

admin_row = find_row_by_alias(csv_rows, base)
```

### Running Subprocess Commands

**Bash:**
```bash
echo "Gathering capacity -> ${MISCDIR}/${BASE}_capacity.csv"
dx_get_capacity ${DE} -unvirt -format csv > "${MISCDIR}/${BASE}_capacity.csv"

# With error handling:
"${DXLOC}/dx_config" -convert tocsv ... >/dev/null || {
  echo "dx_config tocsv failed"; exit 1;
}
```

**Python:**
```python
print(f"Gathering capacity -> {misc_dir}/{base}_capacity.csv")
returncode, stdout, stderr = run_command_capture(
    f"dx_get_capacity {de} -unvirt -format csv".split(),
    cwd=dx_path,
)
if returncode == 0:
    with open(misc_dir / f"{base}_capacity.csv", "w") as f:
        f.write(stdout)

# With error handling:
returncode, stdout, stderr = run_command_capture(cmd)
if returncode != 0:
    print(f"Error: dx_config tocsv failed: {stderr}")
    sys.exit(1)
```

### Building Dynamic Command Flags

**Bash:**
```bash
if [[ "${DE_READ}" == "all" ]]; then
  DE="-all"; DESYS="-all"
else
  DE="-d ${BASE}"; DESYS="-d ${SYS_ALIAS}"
fi

# Usage in command:
dx_ctl_network_tests ${DE} -type latency -remoteaddr all
```

**Python:**
```python
if args.engine == "all":
    de = "-all"
    desys = "-all"
else:
    de = f"-d {base}"
    desys = f"-d {sys_alias}"

# Usage in command:
run_command(
    f"dx_ctl_network_tests {de} -type latency -remoteaddr all".split(),
    cwd=dx_path,
)
```

### Analytics Type Selection

**Bash:**
```bash
case "${DE_TYPE}" in
  win)  ARG_TYPES="cpu,disk,iscsi,network" ;;
  unix) ARG_TYPES="cpu,disk,nfs,network" ;;
  both) ARG_TYPES="cpu,disk,iscsi,nfs,network" ;;
  *)    echo "Invalid -t"; exit 1 ;;
esac
dx_get_analytics ${DE} -i 60 -outdir "${PERFDATA}" -type "${ARG_TYPES}"
```

**Python:**
```python
if args.type == "win":
    arg_types = "cpu,disk,iscsi,network"
elif args.type == "unix":
    arg_types = "cpu,disk,nfs,network"
else:  # both
    arg_types = "cpu,disk,iscsi,nfs,network"

run_command(
    f"dx_get_analytics {de} -i 60 -outdir {perf_data} -type {arg_types}".split(),
    cwd=dx_path,
)
```

## Error Handling Comparison

### Validation Pattern

**Bash (strict mode):**
```bash
set -euo pipefail  # Exit on error, undefined vars, pipe failures
[[ -z "${DE_READ}" ]] && { echo "Missing -d"; showhelp; exit 1; }
[[ -d "${MAINDIR}" && -w "${MAINDIR}" ]] || { echo "Can't write"; exit 1; }
```

**Python:**
```python
if not args.engine:
    print("Error: Missing -d")
    show_help()
    sys.exit(1)

if not os.access(out_path, os.W_OK):
    print(f"Error: Can't write to {args.output_dir}")
    sys.exit(1)
```

### File Existence Check

**Bash:**
```bash
need_file() { [[ -f "$1" ]] || { echo "Missing $1"; exit 1; }; }
need_file "${DXLOC}/dx_config"
```

**Python:**
```python
def check_required_files(dx_path: Path):
    for fname in required:
        fpath = dx_path / fname
        if not fpath.exists():
            print(f"Error: Missing {fpath}")
            sys.exit(1)
```

## Python Advantages in This Context

1. **Type Hints**: Optional but helpful for maintainability
   ```python
   def find_row_by_alias(rows: list, alias: str) -> Optional[Dict]:
   ```

2. **Built-in Modules**: No need for external CLI (e.g., `dx_config` for JSON)
   ```python
   import json
   data = json.load(f)
   ```

3. **String Formatting**: More readable than bash substitution
   ```python
   print(f"Gathering {metric} -> {misc_dir}/{base}_{metric}.csv")
   ```

4. **Path Handling**: `pathlib.Path` is cleaner than shell globbing
   ```python
   for f in sorted(misc_dir.iterdir()):
       print(f"  {f}")
   ```

5. **Password Input**: Secure `getpass` module vs shell `read -s`
   ```python
   password = getpass.getpass("Enter password: ")
   ```

6. **No Escaping Issues**: Subprocess args are list-based (no shell injection)
   ```python
   subprocess.run(["dx_config", "-convert", "tocsv", ...])
   ```

## Testing & Validation

### Test 1: Help Output
```bash
python3 cli_v2.py -h
# Output: Shows usage and version
```

### Test 2: Missing Arguments
```bash
python3 cli_v2.py
# Output: argparse error with usage hint
```

### Test 3: Invalid Path
```bash
python3 cli_v2.py -d test -t unix -b /nonexistent -o /tmp/out
# Output: Error: ... is not a directory
```

### Test 4: Missing Required Files
```bash
python3 cli_v2.py -d test -t unix -b /opt/empty -o /tmp/out
# Output: Error: Missing /opt/empty/dx_config
```

## Migration Checklist

- [x] Parse command-line arguments with `argparse`
- [x] Validate required paths and arguments
- [x] Check for required dxtoolkit files
- [x] Create output directory structure
- [x] Read/write JSON and CSV configs
- [x] Find and merge config rows
- [x] Implement secure password input
- [x] Backup and restore original config
- [x] Run subprocess commands with proper error handling
- [x] Capture stdout for file writing
- [x] List output files at end
- [x] Cleanup on exit (via try/finally)
- [x] Handle all argument variations from bash

## Next Steps

1. **Deploy**: Copy `cli_v2.py` to your dxtoolkit directory
2. **Make executable**: `chmod +x cli_v2.py`
3. **Test with engine**: Use `--address` and `--protocol` flags to avoid prompts
4. **Schedule**: Use in cron jobs just like bash version
5. **Archive**: Keep bash version as backup if needed
