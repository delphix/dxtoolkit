"""Direct TCP analytics fetcher (no dx_get_analytics.py)

Usage examples:
  python3 scripts/get_tcp_direct.py -d democde1 \
    -ref ANALYTICS_STATISTIC_SLICE-4 -i 60 \
    -st "2025-12-17 21:31:16" -outdir /tmp/perl_analytics

This script connects using dxtools.conf, calls the Delphix analytics
getData endpoint directly via Engine + AnalyticTCPObj, and writes raw
and aggregated CSV files like the Perl toolkit.
"""
import argparse
import os
import sys

# toolkit imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib', 'py'))
from engine import Engine  # type: ignore
from analytics import Analytics  # type: ignore
from analytic_tcp_obj import AnalyticTCPObj  # type: ignore
from analytic_obj import AnalyticObj  # type: ignore
from formater import Formater  # type: ignore
import toolkit_helpers  # type: ignore


ALLOWED_RES = {
    60: 'M',
    3600: 'H',
}


def build_args(engine: Engine, start: str, interval: int, end: str | None = None) -> tuple[str, str]:
    """Return (additional_parms, res_symbol) matching Perl behavior.

    - Converts `start` (engine TZ) to UTC ISOZ via toolkit_helpers.parse_timestamp
    - Formats argument string with leading '&'
    - Maps interval (seconds) to resolution symbol used by renderer
    """
    engine_tz = engine.getTimezone()
    st_iso = toolkit_helpers.parse_timestamp(start, engine_tz) if start else None
    if not st_iso:
        from datetime import datetime, timedelta, timezone
        st_dt = datetime.now(timezone.utc) - timedelta(days=7)
        st_iso = st_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    et_iso = toolkit_helpers.parse_timestamp(end, engine_tz) if end else None

    res_symbol = ALLOWED_RES.get(int(interval), 'M')
    add = f"&resolution={int(interval)}&startTime={st_iso}"
    if et_iso:
        add += f"&endTime={et_iso}"
    return add, res_symbol


def fetch_analytic_details(engine: Engine, reference: str) -> dict:
    op = f"resources/json/delphix/analytics/{reference}"
    js, _fmt, rc = engine.getJSONResult(op)
    if rc or js.get('status') != 'OK':
        raise RuntimeError(f"Failed to get analytic details for {reference}: {js}")
    return js.get('result', {})


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Direct TCP analytics fetcher (CSV output)")
    p.add_argument('-d', '--engine', required=True, help="Engine name in dxtools.conf (e.g., democde1)")
    p.add_argument('-ref', '--reference', required=True, help="Analytics reference id (e.g., ANALYTICS_STATISTIC_SLICE-4)")
    p.add_argument('-i', '--interval', type=int, default=60, help="Resolution in seconds (60 or 3600)")
    p.add_argument('-st', '--start', required=True, help="Start time (engine timezone), e.g., '2025-12-17 21:31:16'")
    p.add_argument('-et', '--end', help="Optional end time (engine timezone)")
    p.add_argument('-o', '--outdir', default='/tmp/perl_analytics', help="Output directory for CSV files")
    p.add_argument('-v', '--debug', type=int, default=None, help="Debug verbosity (>=2 prints HTTP)")
    args = p.parse_args(argv)

    eng = Engine(debug=args.debug)
    eng.load_config()
    if eng.dlpx_connect(args.engine):
        print(f"Can't connect to Delphix Engine {args.engine}")
        return 1
    print(f"Connected to Delphix Engine {args.engine} (IP {eng.getIP()})")

    # Discover analytic details to initialize the object correctly
    details = fetch_analytic_details(eng, args.reference)
    name = details.get('name') or 'tcp'
    type_ = details.get('type') or 'TCP_STATS'
    axes = details.get('collectionAxes') or []
    interval = details.get('collectionInterval') or args.interval
    statisticType = details.get('statisticType') or None

    # Force canonical file name for parity
    canonical_name = 'tcp' if 'tcp' in name.lower() else name

    # Initialize specialized TCP analytic object
    obj = AnalyticTCPObj(eng, name, args.reference, type_, axes, interval, statisticType, debug=args.debug)

    add, res_symbol = build_args(eng, args.start, args.interval, args.end)

    print(f"Gathering data for {canonical_name}")
    rc = obj.getData(add, res_symbol)
    if rc:
        if rc == 2:
            print(f"No data returned for analytics {canonical_name}. Consider restarting collector")
        elif rc == 1:
            print(f"Timeout during gathering data for {canonical_name}")
        else:
            print(f"Unknown error gathering data for {canonical_name} (code {rc})")
        return rc

    # Prepare output paths
    os.makedirs(args.outdir, exist_ok=True)
    raw_fn = os.path.join(args.outdir, f"{args.engine}-analytics-{canonical_name}-raw.csv")
    agg_fn = os.path.join(args.outdir, f"{args.engine}-analytics-{canonical_name}-aggregated.csv")

    # Raw output
    print(f"Generating {canonical_name} raw report file {raw_fn}")
    with open(raw_fn, 'w') as fd:
        obj.processData(10)
        form = getattr(obj, '_output', Formater())
        toolkit_helpers.print_output(form, 'csv', None, fd)

    # Aggregated output
    print(f"Generating {canonical_name} aggregated report file {agg_fn}")
    with open(agg_fn, 'w') as fd:
        obj.doAggregation()
        agg_form = getattr(obj, '_output_aggregation', Formater())
        toolkit_helpers.print_output(agg_form, 'csv', None, fd)

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
