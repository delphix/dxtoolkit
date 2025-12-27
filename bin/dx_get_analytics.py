#!/usr/bin/env python3
"""Python port of dx_get_analytics.pl (lightweight raw + aggregated CSV export)
"""
import argparse
import sys
import os
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

# Prefer mock engine when explicitly requested or if real engine import fails
try:
    from engine import Engine as _RealEngine
except Exception:
    _RealEngine = None
try:
    import mock_engine as _mock
except Exception:
    _mock = None
Engine = _mock.MockEngine if (os.environ.get('DXTOOLKIT_USE_MOCK') == '1' and _mock) else (_RealEngine or (_mock.MockEngine if _mock else None))
from analytics import Analytics
from formater import Formater
import toolkit_helpers


ALLOWED_RES = {
    '1': 'S', 'S': 'S',
    '60': 'M', 'M': 'M',
    '3600': 'H', 'H': 'H'
}


def iso8601(dt: datetime):
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_args(argv):
    p = argparse.ArgumentParser(description="Get analytics data (raw + aggregated)")
    p.add_argument('-d', '--engine', dest='dx_host')
    p.add_argument('-all', action='store_true')
    p.add_argument('-configfile', '-c', dest='config_file')
    p.add_argument('-type', '-t', dest='atype', required=True)
    p.add_argument('-outdir', dest='outdir', required=True)
    p.add_argument('-i', '--interval', dest='interval', default='3600')
    p.add_argument('-st', dest='st')
    p.add_argument('-et', dest='et')
    p.add_argument('-format', dest='fmt', choices=['csv', 'json'], default='csv')
    p.add_argument('-stoponinvalid', action='store_true')
    p.add_argument('-nohead', action='store_true')
    p.add_argument('-debug', dest='debug', type=int, nargs='?', const=1)
    p.add_argument('-dever', dest='dever')
    p.add_argument('-version', action='store_true')
    return p.parse_args(argv)


def aggregate_formater(form: Formater):
    # Build aggregated output by summing numeric columns grouped by first column (timestamp)
    idx_ts = 0
    headers = getattr(form, '_header', [])
    lines = getattr(form, '_lines', [])
    # detect numeric columns (exclude first column)
    num_idx = []
    for i in range(1, len(headers)):
        for row in lines:
            try:
                float(row[i])
                num_idx.append(i)
                break
            except Exception:
                continue
    groups = {}
    for row in lines:
        ts = row[idx_ts]
        if ts not in groups:
            groups[ts] = [0.0 for _ in range(len(headers))]
            groups[ts][idx_ts] = ts
        for i in num_idx:
            try:
                groups[ts][i] += float(row[i])
            except Exception:
                pass
    out = Formater()
    # reuse same headers
    hdr_cols = [{h: max(10, len(str(h)) + 2)} for h in headers]
    out.addHeader(*hdr_cols)
    for ts, vals in sorted(groups.items()):
        # stringify numeric sums
        row = [vals[0]] + [f"{vals[i]:.2f}" if i in num_idx else (str(vals[i]) if vals[i] != 0.0 else '') for i in range(1, len(headers))]
        out.addLine(*row)
    return out


def main(argv):
    args = parse_args(argv)
    if args.version:
        print(toolkit_helpers.version)
        return 0

    # Validate required options early with friendly messages
    if not args.config_file:
        print("ERROR: config file is required (-configfile)")
        return 1
    if not os.path.exists(args.config_file):
        print(f"ERROR: config file not found: {args.config_file}")
        return 1
    if not args.outdir:
        print("ERROR: output directory is required (-outdir)")
        return 1

    if args.all and args.dx_host:
        print('Option all (-all) and engine (-d|engine) are mutually exclusive')
        return 1

    if args.interval not in ALLOWED_RES:
        print('Wrong -i parameter')
        return 1

    # Create engine first to get timezone for timestamp conversion
    eng = Engine(args.dever, args.debug)
    try:
        eng.load_config(args.config_file)
    except FileNotFoundError:
        print(f"ERROR: config file not found: {args.config_file}")
        return 1
    except Exception as exc:
        print(f"ERROR: failed to load config file {args.config_file}: {exc}")
        return 1
    
    # Get first engine to determine timezone for timestamp parsing
    engine_list = toolkit_helpers.get_engine_list(args.all, args.dx_host, eng)
    engine_tz = None
    if engine_list:
        first_engine = sorted(engine_list)[0]
        if not eng.dlpx_connect(first_engine):
            engine_tz = eng.getTimezone()
    
    # Prepare time bounds - convert from engine timezone to UTC (like Perl does)
    # Parse timestamps via toolkit helpers (Perl-compatible)
    st_iso = toolkit_helpers.parse_timestamp(args.st, engine_tz) if args.st else None
    et_iso = toolkit_helpers.parse_timestamp(args.et, engine_tz) if args.et else None
    if not st_iso:
        st_dt = datetime.now(timezone.utc) - timedelta(days=7)
        st_iso = iso8601(st_dt)

    res_symbol = ALLOWED_RES[args.interval]
    # Use numeric resolution value for API (not symbol), matching Perl behavior
    # Perl adds leading & to arguments: "&resolution=$resolution&startTime=$st_timestamp"
    arguments = f"&resolution={args.interval}&startTime={st_iso}"
    if et_iso:
        arguments += f"&endTime={et_iso}"

    os.makedirs(args.outdir, exist_ok=True)

    engine_list = toolkit_helpers.get_engine_list(args.all, args.dx_host, eng)
    ret = 0

    for engine_name in sorted(engine_list):
        if eng.dlpx_connect(engine_name):
            print(f"Can't connect to Delphix Engine {engine_name}")
            ret += 1
            continue
        else:
            print(f"Connected to Delphix Engine {engine_name} (IP {eng.getIP()})")

        anl = Analytics(eng, args.debug)

        # Determine analytics to run
        names = []
        if args.atype.lower() == 'all':
            for ref in anl.getAnalyticsList():
                nm = anl.getName(ref)
                if nm:
                    names.append(nm)
        elif args.atype.lower() == 'standard':
            names = ['cpu', 'network', 'disk', 'nfs']
        else:
            for n in args.atype.split(','):
                if anl.getAnalyticByName(n):
                    names.append(n)
                else:
                    print(f"Analytic name {n} not found. It will be not included in output")

        for n in sorted(names):
            analytic = anl.getAnalyticByName(n)
            if not analytic:
                continue
            print(f"Gathering data for {analytic.getName()}")
            retc = analytic.getData(arguments, res_symbol)
            if retc:
                # Mirror Perl messaging for empty analytics to aid troubleshooting
                if retc == 2:
                    print(f"No data returned for analytics {analytic.getName()}. Consider restarting collector")
                elif retc == 1:
                    print(f"Timeout during gathering data for {analytic.getName()}")
                else:
                    print(f"Unknown error gathering data for {analytic.getName()} (code {retc})")
                if args.stoponinvalid:
                    return retc
                ret += 1
                continue
            # raw
            raw_fn = os.path.join(args.outdir, f"{engine_name}-analytics-{analytic.getName()}-raw.{args.fmt}")
            print(f"Generating {analytic.getName()} raw report file {raw_fn}")
            with open(raw_fn, 'w') as fd:
                # process to build Formater for raw output
                analytic.processData(10)
                form = getattr(analytic, '_output', Formater())
                toolkit_helpers.print_output(form, args.fmt, args.nohead, fd)
            # aggregated - call doAggregation to compute stats, then output aggregation
            agg_fn = os.path.join(args.outdir, f"{engine_name}-analytics-{analytic.getName()}-aggregated.{args.fmt}")
            print(f"Generating {analytic.getName()} aggregated report file {agg_fn}")
            with open(agg_fn, 'w') as fd:
                # Call doAggregation to compute min/max/85pct stats (like Perl)
                analytic.doAggregation()
                agg_form = getattr(analytic, '_output_aggregation', Formater())
                toolkit_helpers.print_output(agg_form, args.fmt, args.nohead, fd)

    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
