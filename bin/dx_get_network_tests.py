#!/usr/bin/env python3
"""Python port of dx_get_network_tests.pl (lightweight CSV/JSON output)
"""
import argparse
import sys
import os

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
from network_obj import NetworkObj
from host_obj import HostObj
from databases import Databases
from formater import Formater
import toolkit_helpers


def parse_args(argv):
    p = argparse.ArgumentParser(description="Get network test results")
    p.add_argument('-d', '--engine', dest='dx_host')
    p.add_argument('-all', action='store_true')
    p.add_argument('-configfile', '-c', dest='config_file')
    p.add_argument('-type', dest='test_type', choices=['latency', 'throughput', 'dsp'], required=True)
    p.add_argument('-remoteaddr', dest='remoteaddr')
    p.add_argument('-last', action='store_true')
    p.add_argument('-format', dest='fmt', choices=['csv', 'json'])
    p.add_argument('-nohead', action='store_true')
    p.add_argument('-debug', dest='debug', type=int, nargs='?', const=1)
    p.add_argument('-dever', dest='dever')
    p.add_argument('-version', action='store_true')
    return p.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    if args.version:
        print(toolkit_helpers.version)
        return 0

    if not toolkit_helpers.ensure_config_file(args.config_file):
        return 1

    if args.all and args.dx_host:
        print('Option all (-all) and engine (-d|engine) are mutually exclusive')
        return 1

    if args.last and (not args.remoteaddr):
        print('Option -last requires -remoteaddr to be defined')
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
            print(f"Can't connect to Dephix Engine {engine_name}")
            ret += 1
            continue

        net = NetworkObj(eng, args.debug)
        hosts = HostObj(eng, args.debug)
        dbs = Databases(eng, args.debug)

        # Prepare output headers
        out = Formater()
        if args.test_type == 'latency':
            out.addHeader({'engine': 20}, {'name': 35}, {'remote host': 20}, {'VDB found': 10},
                          {'state': 15}, {'average': 10}, {'minimum': 10}, {'maximum': 10},
                          {'stddev': 10}, {'count': 10}, {'size': 10}, {'loss': 10})
        else:
            out.addHeader({'engine': 20}, {'name': 35}, {'remote host': 20}, {'VDB found': 10},
                          {'state': 15}, {'direction': 12}, {'no of conn': 12}, {'throughput': 12}, {'block size': 12})

        # Decide hostrefs
        hostrefs = []
        if args.remoteaddr:
            if args.remoteaddr.lower() == 'all':
                hostrefs = hosts.getAllHosts()
                if not hostrefs:
                    hostrefs = [None]
            else:
                for h in args.remoteaddr.split(','):
                    href = hosts.getHostByAddr(h)
                    if href:
                        hostrefs.append(href)
                    else:
                        print(f"Remote host with addr {h} not found in Delphix Engine")
                        ret += 1
        else:
            hostrefs = [None]

        # Collect tests
        testrefs = []
        for href in hostrefs:
            if args.last:
                if args.test_type == 'latency':
                    testrefs.extend(net.getLatencyLastTests(href))
                elif args.test_type == 'throughput':
                    testrefs.extend(net.getThroughputLastTests(href))
                else:
                    testrefs.extend(net.getDSPLastTests(href))
            else:
                if args.test_type == 'latency':
                    testrefs.extend(net.getLatencyTestsList(href))
                elif args.test_type == 'throughput':
                    testrefs.extend(net.getThroughputTestsList(href))
                else:
                    testrefs.extend(net.getDSPTestsList(href))

        # If filtering by engine hosts returned nothing, fall back to all tests
        if (not testrefs) and args.remoteaddr and args.remoteaddr.lower() == 'all':
            if args.test_type == 'latency':
                testrefs.extend(net.getLatencyTestsList())
            elif args.test_type == 'throughput':
                testrefs.extend(net.getThroughputTestsList())
            else:
                testrefs.extend(net.getDSPTestsList())
            if testrefs:
                print("No network tests matched engine hosts; falling back to all available tests.")

        # Render lines
        for tref in testrefs:
            hostref = net.getHost(tref)
            hostobj = hosts.getHost(hostref) if hostref else {'name': 'N/A'}
            hostname = hostobj.get('name', 'N/A')
            vdb_list = dbs.getDBForHost(hostname)
            vdb_found = 'YES' if any(dbs.getDB(t) and dbs.getDB(t).get('provisionContainer') for t in vdb_list) else 'NO'
            state = net.getState(tref)
            name = net.getName(tref)

            if args.test_type == 'latency':
                out.addLine(engine_name, name, hostname, vdb_found, state,
                            net.getLatencyAvg(tref), net.getLatencyMin(tref), net.getLatencyMax(tref),
                            net.getLatencyStdDev(tref), net.getLatencyCount(tref), net.getLatencySize(tref), net.getLatencyLoss(tref))
            else:
                out.addLine(engine_name, name, hostname, vdb_found, state,
                            net.getTestDirection(tref), net.getTestConnections(tref), net.getTestRate(tref), net.getTestBlockSize(tref))

        toolkit_helpers.print_output(out, args.fmt or 'csv', args.nohead)

    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
