# CLI_V2 Parity Analysis: Bash vs Python

**Date:** December 26, 2025  
**Scripts Compared:**  
- `bin/cli_v2.sh` (Bash version 3.1.0)
- `bin/cli_v2.py` (Python version 3.2.0)

---

## Executive Summary

⚠️ **PARITY ISSUE IDENTIFIED**: The analytics collection logic differs between bash and Python versions, potentially causing different output files.

### Critical Difference

| Aspect | Bash Version | Python Version | Impact |
|--------|-------------|----------------|---------|
| Analytics Types | Dynamic based on `-t` parameter:<br>• win: `cpu,disk,iscsi,network`<br>• unix: `cpu,disk,nfs,network`<br>• both: `cpu,disk,iscsi,nfs,network` | **Fixed:** `-type standard` for all cases | ⚠️ **HIGH**: Python ignores `-t` parameter for analytics, may collect wrong subset |

---

## Detailed Code Comparison

### 1. Command-Line Arguments

#### Bash (`cli_v2.sh`)
```bash
while [[ $# -gt 0 ]]; do
  case "$1" in
    -d) DE_READ="${2:-}"; shift 2 ;;
    -t) DE_TYPE="${2:-}"; shift 2 ;;
    -b) DXLOC="$(cd "${2:-}" && pwd)"; shift 2 ;;
    -o) MAINDIR="$(mkdir -p "${2:-}" && cd "${2:-}" && pwd)"; shift 2 ;;
    --address)   ADDR_OPT="${2:-}"; shift 2 ;;
    --port)      PORT_OPT="${2:-}"; shift 2 ;;
    --protocol)  PROTO_OPT="${2:-}"; shift 2 ;;
    --preserve-output) PRESERVE_OUTPUT=1; shift 1 ;;
    -h|--help) showhelp; exit 0 ;;
    *) echo "Invalid option: $1"; showhelp; exit 1 ;;
  esac
done
```

#### Python (`cli_v2.py`)
```python
parser.add_argument("-d", dest="engine", required=True, help="engine|all")
parser.add_argument("-t", dest="type", required=True, help="win|unix|both")
parser.add_argument("-b", dest="dxtoolkit_path", required=True, help="dxtoolkit directory path")
parser.add_argument("-o", dest="output_dir", required=True, help="output directory path")
parser.add_argument("--address", dest="address", default="", help="FQDN or IP")
parser.add_argument("--port", dest="port", default="", help="port number")
parser.add_argument("--protocol", dest="protocol", default="", help="http|https")
parser.add_argument("--preserve-output", dest="preserve_output", action="store_true", help="keep existing output folders")
```

**Status:** ✅ **MATCH** - Same arguments supported

---

### 2. Config Management

#### Bash
```bash
cd "${DXLOC}"
DCC="${DXLOC}/dxtools.conf"
CSV="${DXLOC}/.dxconf.csv"

# Convert JSON to CSV
perl "${DXLOC}/dx_config.pl" -convert tocsv -configfile "${DCC}" -csvfile "${CSV}"

# Merge rows, back up, write back
BACKUP="${DCC}.orig.${DATE}.$$.bak"
cp -p "${DCC}" "${BACKUP}"
perl "${DXLOC}/dx_config.pl" -convert todxconf -configfile "${DCC}" -csvfile "${CSV}"
```

#### Python
```python
os.chdir(dx_path)
config_file = dx_path / "dxtools.conf"
csv_file = dx_path / ".dxconf.csv"

# Convert JSON to CSV (via helper function)
convert_json_to_csv(dx_path, config_file, csv_file)

# Merge rows, back up, write back
backup_file = config_file.with_suffix(f".orig.{date_str}.{os.getpid()}.bak")
shutil.copy2(config_file, backup_file)
write_csv_rows(csv_file, csv_rows)
convert_csv_to_dxconf(dx_path, config_file, csv_file)
```

**Status:** ✅ **MATCH** - Equivalent logic, Python uses helper functions

---

### 3. Network Tests

#### Bash
```bash
echo "Run a network latency test on all environments"
perl "${DXLOC}/dx_ctl_network_tests.pl" ${DE} -type latency -remoteaddr all

echo "Run a network throughput test on all environments"
perl "${DXLOC}/dx_ctl_network_tests.pl" ${DE} -type throughput -remoteaddr all

echo "Gathering network latency results -> ${MISCDIR}/${BASE}_NL.csv"
perl "${DXLOC}/dx_get_network_tests.pl" ${DE} -last -type latency -remoteaddr all -format csv > "${MISCDIR}/${BASE}_NL.csv"

echo "Gathering network throughput results -> ${MISCDIR}/${BASE}_NT.csv"
perl "${DXLOC}/dx_get_network_tests.pl" ${DE} -last -type throughput -remoteaddr all -format csv > "${MISCDIR}/${BASE}_NT.csv"
```

#### Python
```python
print("Run a network latency test on all environments")
run_command([sys.executable, str(dx_path / PY_DX_CTL_NETWORK_TESTS), *f"{de} -type latency -remoteaddr all".split()], ...)

print("Run a network throughput test on all environments")
run_command([sys.executable, str(dx_path / PY_DX_CTL_NETWORK_TESTS), *f"{de} -type throughput -remoteaddr all".split()], ...)

print(f"Gathering network latency results -> {misc_dir}/{base}_NL.csv")
returncode, stdout, _ = run_command_capture([sys.executable, str(dx_path / PY_DX_GET_NETWORK_TESTS), *f"{de} -last -type latency -remoteaddr all -format csv".split()], ...)
if returncode == 0:
    with open(misc_dir / f"{base}_NL.csv", "w") as f:
        f.write(stdout)

print(f"Gathering network throughput results -> {misc_dir}/{base}_NT.csv")
returncode, stdout, _ = run_command_capture([sys.executable, str(dx_path / PY_DX_GET_NETWORK_TESTS), *f"{de} -last -type throughput -remoteaddr all -format csv".split()], ...)
if returncode == 0:
    with open(misc_dir / f"{base}_NT.csv", "w") as f:
        f.write(stdout)
```

**Status:** ✅ **MATCH** - Calls Python scripts instead of Perl, but same logic

**Expected Outputs:**
- `misc/<engine>_NL.csv` - Network latency results
- `misc/<engine>_NT.csv` - Network throughput results

---

### 4. Analytics Collection ⚠️ **CRITICAL DIFFERENCE**

#### Bash
```bash
echo "Gathering analytics (${DE_TYPE})"
case "${DE_TYPE}" in
  win)  ARG_TYPES="cpu,disk,iscsi,network" ;;
  unix) ARG_TYPES="cpu,disk,nfs,network" ;;
  both) ARG_TYPES="cpu,disk,iscsi,nfs,network" ;;
  *)    echo "Invalid -t (use win|unix|both)"; exit 1 ;;
esac
perl "${DXLOC}/dx_get_analytics.pl" ${DE} -i 60 -outdir "${PERFDATA}" -type "${ARG_TYPES}"
```

**Bash Behavior:**
- For `-t win`: Collects cpu, disk, **iSCSI**, network (4 types)
- For `-t unix`: Collects cpu, disk, **NFS**, network (4 types)
- For `-t both`: Collects cpu, disk, **iSCSI**, **NFS**, network (5 types)

#### Python
```python
print("Gathering analytics metadata (Python control)")

# Analytics raw + aggregated export (Python-only) and normalize filenames to match legacy bash outputs
rc_analytics = run_command(
    [sys.executable, str(dx_path / PY_DX_GET_ANALYTICS), *f"{de} -i 60 -outdir {perf_data} -type standard -format csv".split()],
    cwd=dx_path,
    env=env_base,
)
if rc_analytics == 0:
    normalize_analytics_outputs(perf_data)
```

**Python Behavior:**
- **Always uses `-type standard` regardless of `-t` parameter**
- **Does not vary analytics types based on win/unix/both**

**Impact:**
- ❌ Python ignores the `-t` (DE_TYPE) parameter for analytics
- ❌ May collect wrong subset of analytics (e.g., collecting NFS on Windows environments or iSCSI on Unix)
- ❌ Output files will differ from bash version

**Expected Outputs (Bash):**
- For `-t unix`:
  - `analytics/<engine>-analytics-cpu-raw.csv`
  - `analytics/<engine>-analytics-cpu-aggregated.csv`
  - `analytics/<engine>-analytics-disk-raw.csv`
  - `analytics/<engine>-analytics-disk-aggregated.csv`
  - `analytics/<engine>-analytics-nfs-raw.csv`
  - `analytics/<engine>-analytics-nfs-aggregated.csv`
  - `analytics/<engine>-analytics-network-raw.csv`
  - `analytics/<engine>-analytics-network-aggregated.csv`

**Expected Outputs (Python - needs fix):**
- Currently: **All analytics types** (depends on what `-type standard` means)
- Should match bash based on `-t` parameter

---

### 5. Capacity, Appliance, and Storage Tests

#### Bash
```bash
echo "Gathering capacity -> ${MISCDIR}/${BASE}_capacity.csv"
perl "${DXLOC}/dx_get_capacity.pl" ${DE} -unvirt -format csv > "${MISCDIR}/${BASE}_capacity.csv"

echo "Gathering appliance -> ${MISCDIR}/${BASE}_appliance.csv"
perl "${DXLOC}/dx_get_appliance.pl" ${DE} -format csv > "${MISCDIR}/${BASE}_appliance.csv"

echo "Gathering IORC (sysadmin) -> ${MISCDIR}/"
perl "${DXLOC}/dx_get_storage_tests.pl" ${DESYS} -testid last -iorc "${MISCDIR}"

echo "Gathering system configuration (sysadmin) -> ${MISCDIR}/${BASE}_config.csv"
perl "${DXLOC}/dx_get_config.pl" ${DESYS} -format csv | sed "s/${SYS_ALIAS}/${BASE}/" > "${MISCDIR}/${BASE}_config.csv"
```

#### Python
```python
print(f"Gathering capacity -> {misc_dir}/{base}_capacity.csv")
returncode, stdout, _ = run_command_capture([sys.executable, str(dx_path / PY_DX_GET_CAPACITY), *f"{de} -unvirt -format csv".split()], ...)
if returncode == 0:
    with open(misc_dir / f"{base}_capacity.csv", "w") as f:
        f.write(stdout)

print(f"Gathering appliance -> {misc_dir}/{base}_appliance.csv")
returncode, stdout, _ = run_command_capture([sys.executable, str(dx_path / PY_DX_GET_APPLIANCE), *f"{de} -format csv".split()], ...)
if returncode == 0:
    with open(misc_dir / f"{base}_appliance.csv", "w") as f:
        f.write(stdout)

print(f"Gathering IORC (sysadmin) -> {misc_dir}/")
run_command([sys.executable, str(dx_path / PY_DX_GET_STORAGE_TESTS), *f"{desys} -testid last -iorc {misc_dir}".split()], ...)

print(f"Gathering system configuration (sysadmin) -> {misc_dir}/{base}_config.csv")
returncode, stdout, _ = run_command_capture([sys.executable, str(dx_path / PY_DX_GET_CONFIG), *f"{desys} -format csv".split()], ...)
if returncode == 0:
    output = stdout.replace(sys_alias, base)
    with open(misc_dir / f"{base}_config.csv", "w") as f:
        f.write(output)
```

**Status:** ✅ **MATCH** - Calls Python scripts instead of Perl, but same logic and output files

**Expected Outputs:**
- `misc/<engine>_capacity.csv` - Capacity report
- `misc/<engine>_appliance.csv` - Appliance info
- `misc/<iorc_files>` - IORC storage test results
- `misc/<engine>_config.csv` - System configuration

---

### 6. Cleanup and Output Display

#### Bash
```bash
cleanup() {
  if [[ -n "${BACKUP}" && -f "${BACKUP}" ]]; then
    mv -f "${BACKUP}" "${DCC}"
  fi
  rm -f "${CSV}" 2>/dev/null || true
}
trap cleanup EXIT

export PATH="${OLDPATH}"
echo "Done. Outputs:"
ls -l "${PERFDATA}"/* 2>/dev/null || true
ls -l "${MISCDIR}"/*  2>/dev/null || true
```

#### Python
```python
def cleanup():
    nonlocal backup_file
    if backup_file and backup_file.exists():
        try:
            shutil.copy2(backup_file, config_file)
            backup_file.unlink()
        except Exception as e:
            print(f"Warning: Failed to restore backup: {e}")
    try:
        if csv_file.exists():
            csv_file.unlink()
    except Exception:
        pass

try:
    # ... main logic ...
finally:
    cleanup()

os.environ["PATH"] = old_path
print("Done. Outputs:")
try:
    for f in sorted(perf_data.iterdir()):
        print(f"  {f}")
except Exception:
    pass
try:
    for f in sorted(misc_dir.iterdir()):
        print(f"  {f}")
except Exception:
    pass
```

**Status:** ✅ **MATCH** - Equivalent cleanup logic using try/finally

---

## Complete Output File Comparison

### Bash Version Outputs

#### For `-t unix`:
```
/tmp/output/
├── analytics/
│   ├── <engine>-analytics-cpu-raw.csv
│   ├── <engine>-analytics-cpu-aggregated.csv
│   ├── <engine>-analytics-disk-raw.csv
│   ├── <engine>-analytics-disk-aggregated.csv
│   ├── <engine>-analytics-nfs-raw.csv
│   ├── <engine>-analytics-nfs-aggregated.csv
│   ├── <engine>-analytics-network-raw.csv
│   └── <engine>-analytics-network-aggregated.csv
└── misc/
    ├── <engine>_NL.csv
    ├── <engine>_NT.csv
    ├── <engine>_capacity.csv
    ├── <engine>_appliance.csv
    ├── <iorc_files>
    └── <engine>_config.csv
```

#### For `-t win`:
```
/tmp/output/
├── analytics/
│   ├── <engine>-analytics-cpu-raw.csv
│   ├── <engine>-analytics-cpu-aggregated.csv
│   ├── <engine>-analytics-disk-raw.csv
│   ├── <engine>-analytics-disk-aggregated.csv
│   ├── <engine>-analytics-iscsi-raw.csv      # Instead of NFS
│   ├── <engine>-analytics-iscsi-aggregated.csv
│   ├── <engine>-analytics-network-raw.csv
│   └── <engine>-analytics-network-aggregated.csv
└── misc/
    └── (same as unix)
```

### Python Version Outputs (Current - Needs Fix)

```
/tmp/output/
├── analytics/
│   ├── <files depend on what "-type standard" collects>
│   └── (may not match bash outputs)
└── misc/
    └── (matches bash outputs ✓)
```

---

## Issues Summary

### 🔴 Critical Issue: Analytics Type Selection

**Problem:** Python version uses hardcoded `-type standard` instead of respecting the `-t` parameter.

**Root Cause:** Line 477 in `cli_v2.py`:
```python
[sys.executable, str(dx_path / PY_DX_GET_ANALYTICS), *f"{de} -i 60 -outdir {perf_data} -type standard -format csv".split()]
```

**Expected Behavior:**
```python
# Determine analytics types based on args.type (win/unix/both)
if args.type == "win":
    analytics_types = "cpu,disk,iscsi,network"
elif args.type == "unix":
    analytics_types = "cpu,disk,nfs,network"
elif args.type == "both":
    analytics_types = "cpu,disk,iscsi,nfs,network"
else:
    print("Invalid -t (use win|unix|both)")
    sys.exit(1)

[sys.executable, str(dx_path / PY_DX_GET_ANALYTICS), *f"{de} -i 60 -outdir {perf_data} -type {analytics_types}".split()]
```

**Impact:**
- Python version does not have output parity with bash version
- Wrong analytics types may be collected for the environment
- Breaks compatibility with existing automation expecting specific files

---

## Validation Test Plan

Once the analytics issue is fixed, validate parity using:

```bash
# 1. Run bash version
bash bin/cli_v2.sh -d <engine> -t unix -b /path/to/bin -o /tmp/bash_output \
  --address <engine> --port 80 --protocol http

# 2. Run Python version
python bin/cli_v2.py -d <engine> -t unix -b /path/to/bin -o /tmp/python_output \
  --address <engine> --port 80 --protocol http

# 3. Compare outputs
diff -r /tmp/bash_output/analytics /tmp/python_output/analytics
diff -r /tmp/bash_output/misc /tmp/python_output/misc

# 4. Check file counts
ls -1 /tmp/bash_output/analytics/ | wc -l
ls -1 /tmp/python_output/analytics/ | wc -l

# 5. Verify filenames match
ls -1 /tmp/bash_output/analytics/ | sort > /tmp/bash_files.txt
ls -1 /tmp/python_output/analytics/ | sort > /tmp/python_files.txt
diff /tmp/bash_files.txt /tmp/python_files.txt
```

---

## Recommendations

### Immediate Actions

1. **Fix analytics type selection** in `cli_v2.py` line 477 to respect `-t` parameter
2. **Remove `-format csv` flag** if not present in bash version (verify dx_get_analytics.pl behavior)
3. **Test with all three types**: `-t win`, `-t unix`, `-t both`
4. **Validate output file structure** matches bash exactly

### Additional Considerations

1. **normalize_analytics_outputs()**: Verify this function creates compatible filenames with bash version
2. **Error handling**: Ensure Python version continues on non-zero exit codes same as bash
3. **Progress output**: Bash shows "Gathering analytics (unix)" - Python should match
4. **-format csv**: Investigate if this is required for Python version or changes output format

---

## Conclusion

✅ **Configuration management**: Parity achieved  
✅ **Network tests**: Parity achieved (calls Python equivalents)  
⚠️ **Analytics collection**: **PARITY ISSUE - Needs fix**  
✅ **Capacity/appliance/storage**: Parity achieved  
✅ **Cleanup logic**: Parity achieved  

**Overall Status:** ⚠️ **Partial parity** - Analytics type selection must be fixed before Python version can be considered a drop-in replacement for bash version.
