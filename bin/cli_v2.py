#!/usr/bin/env python3
"""
cli_v2.py — Delphix CD engine healthcheck (robust config handling)
Created: 2025-10-08

Reuses existing FQDN/port/protocol from dxtools.conf when adding rows.
Adds --address/--port/--protocol flags to avoid prompts.
Normalizes base alias to avoid 'syssys'.
Backs up and auto-restores dxtools.conf after run.
Optional --preserve-output to keep existing output folders.
"""

import sys
import argparse
import os
import json
import csv
import shutil
import subprocess
import tempfile
import getpass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple

SCRIPT_VERSION = "3.2.0"

# Script names (Python-only)
PY_DX_CTL_NETWORK_TESTS = "dx_ctl_network_tests.py"
PY_DX_CTL_ANALYTICS = "dx_ctl_analytics.py"
PY_DX_GET_CAPACITY = "dx_get_capacity.py"
PY_DX_GET_APPLIANCE = "dx_get_appliance.py"
PY_DX_GET_STORAGE_TESTS = "dx_get_storage_tests.py"
PY_DX_GET_CONFIG = "dx_get_config.py"
PY_DX_GET_NETWORK_TESTS = "dx_get_network_tests.py"
PY_DX_GET_ANALYTICS = "dx_get_analytics.py"


def show_help():
    """Display help message."""
    print(f"""Script Version {SCRIPT_VERSION}
Script to generate Delphix CD healthcheck data

Usage:
  cli_v2.py -d <engine|all> -t <win|unix|both> -b <dxtoolkit_path> -o <output_dir>
  [--address <fqdn_or_ip>] [--port <port>] [--protocol <http|https>] [--preserve-output] [-h]

Required Python dxtoolkit scripts in -b:
    {PY_DX_CTL_NETWORK_TESTS}, {PY_DX_CTL_ANALYTICS}, {PY_DX_GET_CAPACITY},
    {PY_DX_GET_APPLIANCE}, {PY_DX_GET_STORAGE_TESTS}, {PY_DX_GET_CONFIG}

Notes:
  • Set timeouts to 600 in your dxtools.conf entries for long-running calls.
""")


def parse_args():
    """Parse command-line arguments."""
    # Check for help first
    if "-h" in sys.argv or "--help" in sys.argv:
        show_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        prog="cli_v2.py",
        add_help=False,
    )
    parser.add_argument("-d", dest="engine", required=True, help="engine|all")
    parser.add_argument("-t", dest="type", required=True, help="win|unix|both")
    parser.add_argument(
        "-b", dest="dxtoolkit_path", required=True, help="dxtoolkit directory path"
    )
    parser.add_argument(
        "-o", dest="output_dir", required=True, help="output directory path"
    )
    parser.add_argument("--address", dest="address", default="", help="FQDN or IP")
    parser.add_argument("--port", dest="port", default="", help="port number")
    parser.add_argument(
        "--protocol", dest="protocol", default="", help="http|https"
    )
    parser.add_argument(
        "--preserve-output",
        dest="preserve_output",
        action="store_true",
        help="keep existing output folders",
    )

    args = parser.parse_args()
    return args


def validate_args(args):
    """Validate required arguments."""
    if not args.engine:
        print("Error: Missing -d")
        show_help()
        sys.exit(1)

    if not args.type:
        print("Error: Missing -t")
        show_help()
        sys.exit(1)

    if not args.dxtoolkit_path:
        print("Error: Missing -b")
        show_help()
        sys.exit(1)

    if not args.output_dir:
        print("Error: Missing -o")
        show_help()
        sys.exit(1)

    # Validate type
    if args.type not in ("win", "unix", "both"):
        print("Error: -t must be win|unix|both")
        sys.exit(1)

    # Ensure dxtoolkit path exists
    dx_path = Path(args.dxtoolkit_path).resolve()
    if not dx_path.is_dir():
        print(f"Error: {args.dxtoolkit_path} is not a directory")
        sys.exit(1)

    # Ensure output dir is writable
    out_path = Path(args.output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    if not os.access(out_path, os.W_OK):
        print(f"Error: Can't write to {args.output_dir}")
        sys.exit(1)

    return dx_path, out_path


def check_required_files(dx_path: Path):
    """Check that all required Python dxtoolkit files exist."""
    required = [
        dx_path / PY_DX_CTL_NETWORK_TESTS,
        dx_path / PY_DX_CTL_ANALYTICS,
        dx_path / PY_DX_GET_CAPACITY,
        dx_path / PY_DX_GET_APPLIANCE,
        dx_path / PY_DX_GET_STORAGE_TESTS,
        dx_path / PY_DX_GET_CONFIG,
        dx_path / PY_DX_GET_NETWORK_TESTS,
        dx_path / PY_DX_GET_ANALYTICS,
    ]

    for fpath in required:
        if not fpath.exists():
            print(f"Error: Missing {fpath}")
            sys.exit(1)


def read_config_json(config_file: Path) -> list:
    """Read dxtools.conf JSON and return list of config entries."""
    if not config_file.exists():
        return []

    try:
        with open(config_file, "r") as f:
            data = json.load(f)
            return data.get("data", [])
    except Exception as e:
        print(f"Error reading {config_file}: {e}")
        sys.exit(1)


def write_config_json(config_file: Path, entries: list):
    """Write config entries back to dxtools.conf JSON."""
    data = {"data": entries}
    try:
        with open(config_file, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error writing {config_file}: {e}")
        sys.exit(1)


def read_csv_rows(csv_file: Path) -> list:
    """Read CSV config file and return list of rows as dicts."""
    if not csv_file.exists():
        return []

    rows = []
    try:
        with open(csv_file, "r") as f:
            reader = csv.DictReader(
                f, fieldnames=["alias", "ip", "port", "user", "password", "enc", "protocol"]
            )
            for row in reader:
                if row["alias"]:  # Skip empty rows
                    rows.append(row)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        sys.exit(1)

    return rows


def write_csv_rows(csv_file: Path, rows: list):
    """Write config rows to CSV file."""
    try:
        with open(csv_file, "w", newline="") as f:
            # Write rows directly to avoid unused writer
            for row in rows:
                f.write(f"{row['alias']},{row['ip']},{row['port']},{row['user']},{row['password']},{row['enc']},{row['protocol']}\n")
    except Exception as e:
        print(f"Error writing {csv_file}: {e}")
        sys.exit(1)


def find_row_by_alias(rows: list, alias: str) -> Optional[Dict]:
    """Find first row matching alias."""
    for row in rows:
        if row.get("alias") == alias:
            return row
    return None


def run_command(cmd: list, cwd: Optional[Path] = None, env: Optional[Dict] = None) -> int:
    """Run subprocess command, return exit code."""
    try:
        result = subprocess.run(
            cmd, cwd=cwd, env=env, capture_output=False, text=True
        )
        return result.returncode
    except Exception as e:
        print(f"Error running command: {e}")
        return 1


def run_command_capture(
    cmd: list, cwd: Optional[Path] = None, env: Optional[Dict] = None
) -> Tuple[int, str, str]:
    """Run subprocess command, capture output, return (returncode, stdout, stderr)."""
    try:
        result = subprocess.run(
            cmd, cwd=cwd, env=env, capture_output=True, text=True
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        print(f"Error running command: {e}")
        return 1, "", str(e)


def normalize_analytics_outputs(perf_data: Path):
    """Create legacy analytics filenames (<engine>_<metric>.csv) for parity with cli_v2.sh."""
    for raw in perf_data.glob("*-analytics-*-raw.csv"):
        stem = raw.stem  # e.g., engine-analytics-cpu-raw
        if "-analytics-" not in stem:
            continue
        prefix, metric_part = stem.split("-analytics-", 1)
        metric = metric_part.rsplit("-raw", 1)[0]
        if not metric:
            continue
        legacy = perf_data / f"{prefix}_{metric}.csv"
        if not legacy.exists():
            try:
                shutil.copy2(raw, legacy)
            except Exception as exc:  # pragma: no cover - best-effort compatibility copy
                print(f"Warning: failed to create legacy analytics file {legacy}: {exc}")


def convert_json_to_csv(_dx_path: Path, config_file: Path, csv_file: Path):
    """Pure-Python: Convert dxtools.conf JSON to CSV rows used by this script."""
    entries = read_config_json(config_file)
    rows = []
    for e in entries:
        rows.append({
            "alias": e.get("engine", e.get("alias", "")),
            "ip": e.get("ip_address", e.get("ip", "")),
            "port": str(e.get("port", "")),
            "user": e.get("username", e.get("user", "")),
            "password": e.get("password", ""),
            "enc": str(e.get("encrypted", e.get("enc", "false"))).lower(),
            "protocol": e.get("protocol", ""),
        })
    write_csv_rows(csv_file, rows)


def convert_csv_to_dxconf(_dx_path: Path, config_file: Path, csv_file: Path):
    """Pure-Python: Convert CSV rows back to dxtools.conf JSON file."""
    rows = read_csv_rows(csv_file)
    entries = []
    for r in rows:
        entries.append({
            "engine": r.get("alias", ""),
            "name": r.get("alias", ""),
            "hostname": r.get("alias", ""),
            "ip_address": r.get("ip", ""),
            "username": r.get("user", ""),
            "password": r.get("password", ""),
            "port": int(r.get("port", "0") or 0),
            "protocol": r.get("protocol", ""),
            "encrypted": "true" if str(r.get("enc", "false")).lower() == "true" else "false",
            "default": "true" if r.get("alias") == r.get("ip") else "false",
            "timeout": "60",
        })
    write_config_json(config_file, entries)


def main():
    """Main entry point."""
    args = parse_args()
    dx_path, out_path = validate_args(args)
    check_required_files(dx_path)

    # Get date for backup naming
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Setup paths
    config_file = dx_path / "dxtools.conf"
    csv_file = dx_path / ".dxconf.csv"
    perf_data = out_path / "analytics"
    misc_dir = out_path / "misc"
    backup_file = None

    # Base environment for subprocesses so they see the intended config
    env_base = os.environ.copy()
    env_base["DXTOOLKIT_CONF"] = str(config_file)

    # Cleanup handler
    def cleanup():
        """Restore original config and clean temp files."""
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
        print(f"Script Version {SCRIPT_VERSION}")
        print(f"Dxtoolkit Path: {dx_path}")

        # Setup output dirs
        if not args.preserve_output:
            shutil.rmtree(perf_data, ignore_errors=True)
            shutil.rmtree(misc_dir, ignore_errors=True)

        perf_data.mkdir(parents=True, exist_ok=True)
        misc_dir.mkdir(parents=True, exist_ok=True)

        # Derive base alias (strip trailing 'sys' if present)
        base = args.engine.rstrip("sys") if args.engine != "all" else args.engine
        sys_alias = f"{base}sys" if args.engine != "all" else "allsys"

        # Read/prepare config safely
        os.chdir(dx_path)

        # Convert JSON config to CSV if present
        if config_file.exists():
            convert_json_to_csv(dx_path, config_file, csv_file)
        else:
            csv_file.touch()

        # Read CSV rows
        csv_rows = read_csv_rows(csv_file)

        # Get existing admin row (if any)
        admin_row = find_row_by_alias(csv_rows, base)

        admin_ip = admin_row.get("ip", "") if admin_row else ""
        admin_port = admin_row.get("port", "") if admin_row else ""
        # admin_user not used; omit retrieval
        admin_pass = admin_row.get("password", "") if admin_row else ""
        admin_enc = admin_row.get("enc", "false") if admin_row else "false"
        admin_proto = admin_row.get("protocol", "") if admin_row else ""

        # Compute target values to use when adding rows
        use_ip = args.address or admin_ip or base
        use_port = args.port or admin_port or "80"
        use_proto = args.protocol or admin_proto or "http"
        use_enc = admin_enc or "false"

        # Ensure admin row exists
        if not find_row_by_alias(csv_rows, base):
            if not admin_pass and not (args.address or args.port or args.protocol) and args.engine != "all":
                admin_pass = getpass.getpass(f"Admin password for {base}: ")

            csv_rows.append({
                "alias": base,
                "ip": use_ip,
                "port": use_port,
                "user": "admin",
                "password": admin_pass or "",
                "enc": use_enc,
                "protocol": use_proto,
            })

        # Ensure sys row exists (reuse address/port/proto)
        if not find_row_by_alias(csv_rows, sys_alias):
            syspass = ""
            if not (args.address or args.port or args.protocol) and args.engine != "all":
                syspass = getpass.getpass(f"Sysadmin password for {sys_alias}: ")

            csv_rows.append({
                "alias": sys_alias,
                "ip": use_ip,
                "port": use_port,
                "user": "sysadmin",
                "password": syspass or "",
                "enc": use_enc,
                "protocol": use_proto,
            })

        # Back up original config if it exists and we're going to change it
        if config_file.exists():
            backup_file = config_file.with_suffix(f".orig.{date_str}.{os.getpid()}.bak")
            shutil.copy2(config_file, backup_file)

        # Write merged CSV back to JSON config
        write_csv_rows(csv_file, csv_rows)
        convert_csv_to_dxconf(dx_path, config_file, csv_file)

        # Also copy config to lib/dxtools.conf for subprocesses that fall back there
        try:
            lib_conf = dx_path.parent / "lib" / "dxtools.conf"
            shutil.copy2(config_file, lib_conf)
        except Exception as exc:
            print(f"Warning: failed to copy config to lib/dxtools.conf: {exc}")

        # Build -d flags
        if args.engine == "all":
            de = "-all"
            desys = "-all"
        else:
            de = f"-d {base}"
            desys = f"-d {sys_alias}"

        # Add dxtoolkit to PATH
        old_path = os.environ.get("PATH", "")
        os.environ["PATH"] = f"{dx_path}:{old_path}"

        # Run tests (Python-only)
        print("Run a network latency test on all environments")
        run_command(
            [sys.executable, str(dx_path / PY_DX_CTL_NETWORK_TESTS), *f"{de} -type latency -remoteaddr all".split()],
            cwd=dx_path,
            env=env_base,
        )

        print("Run a network throughput test on all environments")
        run_command(
            [sys.executable, str(dx_path / PY_DX_CTL_NETWORK_TESTS), *f"{de} -type throughput -remoteaddr all".split()],
            cwd=dx_path,
            env=env_base,
        )

        print(f"Gathering network latency results -> {misc_dir}/{base}_NL.csv")
        returncode, stdout, _ = run_command_capture(
            [sys.executable, str(dx_path / PY_DX_GET_NETWORK_TESTS), *f"{de} -last -type latency -remoteaddr all -format csv".split()],
            cwd=dx_path,
            env=env_base,
        )
        if returncode == 0:
            with open(misc_dir / f"{base}_NL.csv", "w") as f:
                f.write(stdout)

        print(f"Gathering network throughput results -> {misc_dir}/{base}_NT.csv")
        returncode, stdout, _ = run_command_capture(
            [sys.executable, str(dx_path / PY_DX_GET_NETWORK_TESTS), *f"{de} -last -type throughput -remoteaddr all -format csv".split()],
            cwd=dx_path,
            env=env_base,
        )
        if returncode == 0:
            with open(misc_dir / f"{base}_NT.csv", "w") as f:
                f.write(stdout)

        # Determine analytics types based on args.type (win/unix/both) to match bash behavior
        print(f"Gathering analytics ({args.type})")
        if args.type == "win":
            analytics_types = "cpu,disk,iscsi,network"
        elif args.type == "unix":
            analytics_types = "cpu,disk,nfs,network"
        elif args.type == "both":
            analytics_types = "cpu,disk,iscsi,nfs,network"
        else:
            print("Invalid -t (use win|unix|both)")
            sys.exit(1)

        # Analytics raw + aggregated export (Python-only) and normalize filenames to match legacy bash outputs
        rc_analytics = run_command(
            [sys.executable, str(dx_path / PY_DX_GET_ANALYTICS), *f"{de} -i 60 -outdir {perf_data} -type {analytics_types}".split()],
            cwd=dx_path,
            env=env_base,
        )
        if rc_analytics == 0:
            normalize_analytics_outputs(perf_data)
        else:
            print("Warning: analytics collection returned non-zero exit code; see above for details")

        print(f"Gathering capacity -> {misc_dir}/{base}_capacity.csv")
        returncode, stdout, _ = run_command_capture(
            [sys.executable, str(dx_path / PY_DX_GET_CAPACITY), *f"{de} -unvirt -format csv".split()],
            cwd=dx_path,
            env=env_base,
        )
        if returncode == 0:
            with open(misc_dir / f"{base}_capacity.csv", "w") as f:
                f.write(stdout)

        print(f"Gathering appliance -> {misc_dir}/{base}_appliance.csv")
        returncode, stdout, _ = run_command_capture(
            [sys.executable, str(dx_path / PY_DX_GET_APPLIANCE), *f"{de} -format csv".split()],
            cwd=dx_path,
            env=env_base,
        )
        if returncode == 0:
            with open(misc_dir / f"{base}_appliance.csv", "w") as f:
                f.write(stdout)

        print(f"Gathering IORC (sysadmin) -> {misc_dir}/")
        run_command(
            [sys.executable, str(dx_path / PY_DX_GET_STORAGE_TESTS), *f"{desys} -testid last -iorc {misc_dir}".split()],
            cwd=dx_path,
            env=env_base,
        )

        print(
            f"Gathering system configuration (sysadmin) -> {misc_dir}/{base}_config.csv"
        )
        returncode, stdout, _ = run_command_capture(
            [sys.executable, str(dx_path / PY_DX_GET_CONFIG), *f"{desys} -format csv".split()],
            cwd=dx_path,
            env=env_base,
        )
        if returncode == 0:
            # Replace sys_alias with base in output
            output = stdout.replace(sys_alias, base)
            with open(misc_dir / f"{base}_config.csv", "w") as f:
                f.write(output)

        # Restore PATH
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

    finally:
        cleanup()


if __name__ == "__main__":
    main()
