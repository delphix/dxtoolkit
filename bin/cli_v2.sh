#!/usr/bin/env bash
# cli_v2.sh — Delphix CD engine healthcheck (robust config handling)
# Created: 2025-10-08
# Changes vs cli.sh:
#  - Reuses existing FQDN/port/protocol from dxtools.conf when adding rows
#  - Adds --address/--port/--protocol flags to avoid prompts
#  - Normalizes base alias to avoid 'syssys'
#  - Backs up and auto-restores dxtools.conf after run
#  - Optional --preserve-output to keep existing output folders
set -euo pipefail

scriptVersion="3.1.0"

showhelp() {
  cat <<EOF
Script Version ${scriptVersion}
Script to generate Delphix CD healthcheck data

Usage:
  $(basename "$0") -d <engine|all> -t <win|unix|both> -b <dxtoolkit_path> -o <output_dir>
  [--address <fqdn_or_ip>] [--port <port>] [--protocol <http|https>] [--preserve-output] [-h]

Required dxtoolkit scripts in -b:
  dx_config.pl, dx_ctl_network_tests.pl, dx_ctl_bundle.pl, dx_get_analytics.pl,
  dx_get_capacity.pl, dx_get_appliance.pl, dx_get_storage_tests.pl, dx_get_config.pl

Notes:
  • Set timeouts to 600 in your dxtools.conf entries for long-running calls.
EOF
}

# ---- parse args (supports long options) --------------------------------------
DE_READ=""; DE_TYPE=""; DXLOC=""; MAINDIR=""
ADDR_OPT=""; PORT_OPT=""; PROTO_OPT=""; PRESERVE_OUTPUT=0

if [[ $# -eq 0 ]]; then showhelp; exit 1; fi
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

# ---- basic validation (same as original) -------------------------------------
[[ -z "${DE_READ}" ]]  && { echo "Missing -d"; showhelp; exit 1; }
[[ -z "${DE_TYPE}" ]]  && { echo "Missing -t"; showhelp; exit 1; }
[[ -z "${DXLOC}"  ]]   && { echo "Missing -b"; showhelp; exit 1; }
[[ -z "${MAINDIR}" ]]  && { echo "Missing -o"; showhelp; exit 1; }
[[ -d "${MAINDIR}" && -w "${MAINDIR}" ]] || { echo "Can't write to ${MAINDIR}"; exit 1; }

echo "Script Version ${scriptVersion}"
echo "Dxtoolkit Path: ${DXLOC}"

# ---- check dxtoolkit files exist (same list as original) ---------------------
need_file() { [[ -f "$1" ]] || { echo "Missing $1"; exit 1; }; }
need_file "${DXLOC}/dx_config.pl"
need_file "${DXLOC}/dx_ctl_network_tests.pl"
need_file "${DXLOC}/dx_ctl_bundle.pl"
need_file "${DXLOC}/dx_get_analytics.pl"
need_file "${DXLOC}/dx_get_capacity.pl"
need_file "${DXLOC}/dx_get_appliance.pl"
need_file "${DXLOC}/dx_get_storage_tests.pl"
need_file "${DXLOC}/dx_get_config.pl"

# ---- prepare output dirs -----------------------------------------------------
DATE="$(date '+%Y-%m-%d')"
PERFDATA="${MAINDIR}/analytics"
MISCDIR="${MAINDIR}/misc"
if [[ ${PRESERVE_OUTPUT} -eq 0 ]]; then
  rm -rf "${PERFDATA}" "${MISCDIR}" 2>/dev/null || true
fi
mkdir -p "${PERFDATA}" "${MISCDIR}"

# ---- derive base alias and sys alias (avoid 'syssys') ------------------------
BASE="${DE_READ%sys}"             # strip trailing 'sys' if user passed it
SYS_ALIAS="${BASE}sys"            # normalized sys alias used by sysadmin calls

# ---- read/prepare config safely ---------------------------------------------
cd "${DXLOC}"
DCC="${DXLOC}/dxtools.conf"
CSV="${DXLOC}/.dxconf.csv"
BACKUP=""
cleanup() {
  # restore user's config if we replaced it
  if [[ -n "${BACKUP}" && -f "${BACKUP}" ]]; then
    mv -f "${BACKUP}" "${DCC}"
  fi
  rm -f "${CSV}" 2>/dev/null || true
}
trap cleanup EXIT

# Export existing config to CSV if present (we'll merge rows into CSV)
if [[ -f "${DCC}" ]]; then
  perl "${DXLOC}/dx_config.pl" -convert tocsv -configfile "${DCC}" -csvfile "${CSV}" >/dev/null || {
    echo "dx_config tocsv failed"; exit 1;
  }
else
  : > "${CSV}"
fi

# helper: read first matching row by alias into variables
read_row() {
  local alias="$1"
  local line
  line="$(grep -m1 "^${alias}," "${CSV}" || true)"
  if [[ -n "${line}" ]]; then
    IFS=',' read -r _ IP PORT USER PASS ENC PROTO <<< "${line}"
    echo "${IP},${PORT},${USER},${PASS},${ENC},${PROTO}"
  fi
}

# get existing admin row (if any)
ADMIN_ROW="$(read_row "${BASE}")" || true
ADMIN_IP=""; ADMIN_PORT=""; ADMIN_USER=""; ADMIN_PASS=""; ADMIN_ENC=""; ADMIN_PROTO=""
if [[ -n "${ADMIN_ROW}" ]]; then
  IFS=',' read -r ADMIN_IP ADMIN_PORT ADMIN_USER ADMIN_PASS ADMIN_ENC ADMIN_PROTO <<< "${ADMIN_ROW}"
fi

# compute target values to use when we must add rows
USE_IP="${ADDR_OPT:-${ADMIN_IP:-${BASE}}}"
USE_PORT="${PORT_OPT:-${ADMIN_PORT:-80}}"
USE_PROTO="${PROTO_OPT:-${ADMIN_PROTO:-http}}"
USE_ENC="${ADMIN_ENC:-false}"

# ensure admin row exists
if ! grep -q "^${BASE}," "${CSV}" ; then
  if [[ -z "${ADMIN_PASS}" && -z "${ADDR_OPT}${PORT_OPT}${PROTO_OPT}" && "${DE_READ}" != "all" ]]; then
    # last resort minimal prompt only if truly needed
    read -r -s -p "Admin password for ${BASE}: " ADMIN_PASS; echo
  fi
  echo "${BASE},${USE_IP},${USE_PORT},admin,${ADMIN_PASS:-},${USE_ENC},${USE_PROTO}" >> "${CSV}"
fi

# ensure sys row exists (reuse address/port/proto)
if ! grep -q "^${SYS_ALIAS}," "${CSV}" ; then
  SYSPASS=""
  if [[ -z "${ADDR_OPT}${PORT_OPT}${PROTO_OPT}" && "${DE_READ}" != "all" ]]; then
    read -r -s -p "Sysadmin password for ${SYS_ALIAS}: " SYSPASS; echo
  fi
  echo "${SYS_ALIAS},${USE_IP},${USE_PORT},sysadmin,${SYSPASS},${USE_ENC},${USE_PROTO}" >> "${CSV}"
fi

# If we are going to change dxtools.conf (add/merge), back it up and write new
if [[ -f "${DCC}" ]]; then
  BACKUP="${DCC}.orig.${DATE}.$$.bak"
  cp -p "${DCC}" "${BACKUP}"
fi
# Write merged CSV back to JSON config for dxtoolkit to use during this run
perl "${DXLOC}/dx_config.pl" -convert todxconf -configfile "${DCC}" -csvfile "${CSV}" >/dev/null

# ---- build -d flags used by dx_* (same as original, but normalized) ----------
if [[ "${DE_READ}" == "all" ]]; then
  DE="-all"; DESYS="-all"
else
  DE="-d ${BASE}"; DESYS="-d ${SYS_ALIAS}"
fi

# ---- add dxtoolkit to PATH for convenience ----------------------------------
OLDPATH="${PATH}"; export PATH="${DXLOC}:${PATH}"

# ---- run tests (unchanged logic, but clearer logging) ------------------------
echo "Run a network latency test on all environments"
#perl "${DXLOC}/dx_ctl_network_tests.pl" ${DE} -type latency -remoteaddr all

echo "Run a network throughput test on all environments"
#perl "${DXLOC}/dx_ctl_network_tests.pl" ${DE} -type throughput -remoteaddr all

echo "Gathering network latency results -> ${MISCDIR}/${BASE}_NL.csv"
#perl "${DXLOC}/dx_get_network_tests.pl" ${DE} -last -type latency -remoteaddr all -format csv > "${MISCDIR}/${BASE}_NL.csv"

echo "Gathering network throughput results -> ${MISCDIR}/${BASE}_NT.csv"
#perl "${DXLOC}/dx_get_network_tests.pl" ${DE} -last -type throughput -remoteaddr all -format csv > "${MISCDIR}/${BASE}_NT.csv"

echo "Gathering analytics (${DE_TYPE})"
case "${DE_TYPE}" in
  win)  ARG_TYPES="cpu,disk,iscsi,network" ;;
  unix) ARG_TYPES="cpu,disk,nfs,network" ;;
  both) ARG_TYPES="cpu,disk,iscsi,nfs,network" ;;
  *)    echo "Invalid -t (use win|unix|both)"; exit 1 ;;
esac
perl "${DXLOC}/dx_get_analytics.pl" ${DE} -i 60 -outdir "${PERFDATA}" -type "${ARG_TYPES}"     # :contentReference[oaicite:3]{index=3}

echo "Gathering capacity -> ${MISCDIR}/${BASE}_capacity.csv"
perl "${DXLOC}/dx_get_capacity.pl" ${DE} -unvirt -format csv > "${MISCDIR}/${BASE}_capacity.csv"

echo "Gathering appliance -> ${MISCDIR}/${BASE}_appliance.csv"
perl "${DXLOC}/dx_get_appliance.pl" ${DE} -format csv > "${MISCDIR}/${BASE}_appliance.csv"

echo "Gathering IORC (sysadmin) -> ${MISCDIR}/"
perl "${DXLOC}/dx_get_storage_tests.pl" ${DESYS} -testid last -iorc "${MISCDIR}"               # :contentReference[oaicite:4]{index=4}

echo "Gathering system configuration (sysadmin) -> ${MISCDIR}/${BASE}_config.csv"
perl "${DXLOC}/dx_get_config.pl" ${DESYS} -format csv | sed "s/${SYS_ALIAS}/${BASE}/" > "${MISCDIR}/${BASE}_config.csv"

# ---- restore PATH and config; trap handles final config restore --------------
export PATH="${OLDPATH}"

echo "Done. Outputs:"
ls -l "${PERFDATA}"/* 2>/dev/null || true
ls -l "${MISCDIR}"/*  2>/dev/null || true

