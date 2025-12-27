#!/usr/bin/env python3
"""Python port of dx_ctl_bundle.pl."""
import argparse
import sys
import os
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

from engine import Engine
from system_obj import SystemObj
from jobs_obj import JobsObj
import toolkit_helpers


def parse_args(argv):
    p = argparse.ArgumentParser(description="Generate or upload support bundles")
    p.add_argument('-d', '--engine', dest='dx_host')
    p.add_argument('-all', action='store_true')
    p.add_argument('-action', dest='action', required=True, choices=['download', 'upload'])
    p.add_argument('-dirname', dest='dirname')
    p.add_argument('-case', dest='case')
    p.add_argument('-type', dest='bundle_type')
    p.add_argument('-analytics', action='store_true')
    p.add_argument('-debug', dest='debug', type=int, nargs='?', const=1)
    p.add_argument('-dever', dest='dever')
    p.add_argument('-version', action='store_true')
    p.add_argument('-configfile', '-c', dest='config_file')
    return p.parse_args(argv)


def _ts_ymdhms_z_to_filename(ts):
    # Convert 2025-12-20T10:20:30Z -> 20251220-10-20-30
    if not ts:
        return None
    try:
        if ts.endswith('Z'):
            ts = ts[:-1] + '+00:00'
        dt = datetime.fromisoformat(ts)
        return dt.strftime('%Y%m%d-%H-%M-%S')
    except Exception:
        try:
            dt = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')
            return dt.strftime('%Y%m%d-%H-%M-%S')
        except Exception:
            return None


def main(argv):
    args = parse_args(argv)
    if args.version:
        print(toolkit_helpers.version)
        return 0

    if not toolkit_helpers.ensure_config_file(args.config_file):
        return 1

    if args.all and args.dx_host:
        print("Option all (-all) and engine (-d|engine) are mutually exclusive")
        return 1

    if args.action == 'download' and not args.dirname:
        print("Option dirname is required for action download")
        return 1

    eng = Engine(args.dever, args.debug)
    try:
        eng.load_config(args.config_file)
    except FileNotFoundError:
        print(f"ERROR: config file not found: {args.config_file}")
        return 1
    except Exception as exc:
        print(f"ERROR: failed to load config file {args.config_file}: {exc}")
        return 1

    engine_list = toolkit_helpers.get_engine_list(args.all, args.dx_host, eng)
    ret = 0

    for engine_name in sorted(engine_list):
        if eng.dlpx_connect(engine_name):
            print(f"Can't connect to Dephix Engine {engine_name}\n")
            ret += 1
            continue

        if args.action == 'download':
            sysobj = SystemObj(eng, args.debug)
            uuid = sysobj.getUUID()
            cur = eng.getTime()
            suf = _ts_ymdhms_z_to_filename(cur) or 'unknown'
            filename = os.path.join(args.dirname, f"{uuid}-{suf}.tar.gz")

            if not os.path.isdir(args.dirname):
                print(f"Directory {args.dirname} doesn't exists.")
                ret += 1
                continue
            if os.access(args.dirname, os.W_OK):
                print(f"Please wait. Support bundle will be generated and saved into directory - {args.dirname}")
                print("It can take several minutes")
            else:
                print(f"Can't create file - {os.path.basename(filename)} - in directory {args.dirname}")
                ret += 1
                continue

            rc = eng.generateSupportBundle(filename, args.bundle_type, args.analytics)
            if rc:
                print("There was a problem with support bundle generation")
                ret += 1
            else:
                print(f"Support bundle for engine {engine_name} saved into {filename} ")
        else:
            jobno = eng.uploadSupportBundle(args.case, args.bundle_type, args.analytics)
            if jobno:
                print(f"Starting job {jobno} for engine {engine_name}.")
                job = JobsObj(eng, jobno, 'true', args.debug)
                state = job.waitForJob()
                if state != 'COMPLETED':
                    print("There was a problem with support bundle upload")
                    ret += 1
            else:
                print("There was a problem with support bundle upload")
                ret += 1

    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
