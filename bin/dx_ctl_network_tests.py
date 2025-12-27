#!/usr/bin/env python3
"""Run network tests against Delphix Engine hosts (Python port of dx_ctl_network_tests.pl)."""
import argparse
import sys
import os

# adjust import path to find lib/py
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

from engine import Engine
from network_obj import NetworkObj
from host_obj import HostObj
from jobs_obj import JobsObj
from toolkit_helpers import get_engine_list, ensure_config_file


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Run Delphix network tests", add_help=True)
    parser.add_argument('-d', '--engine', dest='dx_host', help='Delphix engine name from dxtools.conf')
    parser.add_argument('-c', '--configfile', dest='config_file', help='Config file path')
    parser.add_argument('-remoteaddr', dest='remoteaddr', help='env_ip|all|env_ip1,env_ip2', required=True)
    parser.add_argument('-type', dest='test_type', choices=['latency', 'throughput', 'dsp'], required=True)
    parser.add_argument('-size', dest='size', type=int, help='Latency packet size')
    parser.add_argument('-duration', dest='duration', type=int, help='Test duration seconds')
    parser.add_argument('-direction', dest='direction', choices=['both', 'transmit', 'receive'], default='both')
    parser.add_argument('-numconn', dest='numconn', type=int, help='Number of connections')
    parser.add_argument('-debug', dest='debug', type=int, nargs='?', const=1, help='Debug level')
    parser.add_argument('-dever', dest='dever', help='Reserved for compatibility')
    parser.add_argument('-all', dest='all', action='store_true', help='Run on all engines')
    parser.add_argument('-version', action='store_true', help='Print version')
    parser.add_argument('-nohead', dest='nohead', action='store_true', help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def direction_list(direction):
    d = direction.lower()
    if d == 'both':
        return ['TRANSMIT', 'RECEIVE']
    if d == 'receive':
        return ['RECEIVE']
    if d == 'transmit':
        return ['TRANSMIT']
    return None


def main(argv):
    args = parse_args(argv)
    if args.version:
        from toolkit_helpers import version
        print(version)
        return 0

    if not ensure_config_file(args.config_file):
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

    if args.all and args.dx_host:
        print('Option all (-all) and engine (-d|engine) are mutually exclusive')
        return 1

    dirs = direction_list(args.direction)
    if dirs is None:
        print(f"Option direction has unknown value - {args.direction}")
        return 1

    engine_list = get_engine_list(args.all, args.dx_host, eng)
    if not engine_list:
        print('No engine selected to process.')
        return 1

    ret = 0
    for engine_name in sorted(engine_list):
        if eng.dlpx_connect(engine_name):
            print(f"Can't connect to Dephix Engine {engine_name}")
            ret += 1
            continue

        net = NetworkObj(eng, args.debug)
        hosts = HostObj(eng, args.debug)

        if args.remoteaddr.lower() == 'all':
            test_hosts = hosts.getAllHosts()
        else:
            test_hosts = []
            for host_item in args.remoteaddr.split(','):
                href = hosts.getHostByAddr(host_item)
                if not href:
                    print(f"Remote host with addr {host_item} not found in Delphix Engine")
                    ret += 1
                    continue
                test_hosts.append(href)

        for hostref in test_hosts:
            if args.test_type == 'latency':
                jobno = net.runLatencyTest(hostref, args.size, args.duration)
                if jobno:
                    print(f"Starting job {jobno} for test .")
                    job = JobsObj(eng, jobno, 'true', args.debug)
                    job.waitForJob()
            elif args.test_type == 'throughput':
                for d in dirs:
                    jobno = net.runThroughputTest(hostref, d, args.numconn, args.duration)
                    if jobno:
                        print(f"Starting job {jobno} for test .")
                        job = JobsObj(eng, jobno, 'true', args.debug)
                        job.waitForJob()
            else:
                for d in dirs:
                    jobno = net.runDSPTest(hostref, d, args.numconn, args.duration)
                    if jobno:
                        print(f"Starting job {jobno} for test .")
                        job = JobsObj(eng, jobno, 'true', args.debug)
                        job.waitForJob()

    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
