#!/usr/bin/env python3
"""Python port of dx_get_storage_tests.pl."""
import argparse
import sys
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

from engine import Engine
from storage_obj import StorageObj
from formater import Formater
import toolkit_helpers


def parse_args(argv):
    p = argparse.ArgumentParser(description="Get storage test results")
    p.add_argument('-d', '--engine', dest='dx_host')
    p.add_argument('-all', action='store_true')
    p.add_argument('-iorc', dest='iorc')
    p.add_argument('-details', action='store_true')
    p.add_argument('-testid', dest='testid')
    p.add_argument('-gradeonly', dest='gradeonly', default='yes')
    p.add_argument('-format', dest='fmt')
    p.add_argument('-debug', dest='debug', type=int, nargs='?', const=1)
    p.add_argument('-dever', dest='dever')
    p.add_argument('-nohead', action='store_true')
    p.add_argument('-version', action='store_true')
    p.add_argument('-configfile', '-c', dest='config_file')
    return p.parse_args(argv)


FULL_TESTS = [
    "Random 4K Read w/ 8 jobs",
    "Random 4K Read w/ 16 jobs",
    "Random 4K Read w/ 32 jobs",
    "Random 4K Read w/ 64 jobs",
    "Random 8K Read w/ 8 jobs",
    "Random 8K Read w/ 16 jobs",
    "Random 8K Read w/ 32 jobs",
    "Random 8K Read w/ 64 jobs",
    "Sequential 1K Write w/ 4 jobs",
    "Sequential 4K Write w/ 4 jobs",
    "Sequential 8K Write w/ 4 jobs",
    "Sequential 16K Write w/ 4 jobs",
    "Sequential 32K Write w/ 4 jobs",
    "Sequential 64K Write w/ 4 jobs",
    "Sequential 128K Write w/ 4 jobs",
    "Sequential 1M Write w/ 4 jobs",
    "Sequential 1K Write w/ 16 jobs",
    "Sequential 4K Write w/ 16 jobs",
    "Sequential 8K Write w/ 16 jobs",
    "Sequential 16K Write w/ 16 jobs",
    "Sequential 32K Write w/ 16 jobs",
    "Sequential 64K Write w/ 16 jobs",
    "Sequential 128K Write w/ 16 jobs",
    "Sequential 1M Write w/ 16 jobs",
    "Sequential 64K Read w/ 4 jobs",
    "Sequential 64K Read w/ 8 jobs",
    "Sequential 64K Read w/ 16 jobs",
    "Sequential 64K Read w/ 32 jobs",
    "Sequential 64K Read w/ 64 jobs",
    "Sequential 128K Read w/ 4 jobs",
    "Sequential 128K Read w/ 8 jobs",
    "Sequential 128K Read w/ 16 jobs",
    "Sequential 128K Read w/ 32 jobs",
    "Sequential 128K Read w/ 64 jobs",
    "Sequential 1M Read w/ 4 jobs",
    "Sequential 1M Read w/ 8 jobs",
    "Sequential 1M Read w/ 16 jobs",
    "Sequential 1M Read w/ 32 jobs",
    "Sequential 1M Read w/ 64 jobs",
]

GRADE_TESTS = [
    "Random 4K Read w/ 16 jobs",
    "Random 8K Read w/ 16 jobs",
    "Sequential 1K Write w/ 4 jobs",
    "Sequential 128K Write w/ 4 jobs",
    "Sequential 1M Read w/ 4 jobs",
]


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

    if args.iorc and args.details:
        print("Options -iorc and -details are mutually exclusive")
        return 1

    if args.gradeonly.lower() not in ('yes', 'no'):
        print(f"Option -gradeonly has a wrong value - {args.gradeonly}")
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

    output = Formater(args.debug)
    if args.details:
        output.addHeader(
            {'engine name': 35}, {'test id': 15}, {'test name': 30},
            {'IOPS': 10}, {'Throughput': 15}, {'Grade': 7},
            {'average': 10}, {'95pct': 10}, {'minimum': 10}, {'maximum': 10}, {'stddev': 10}
        )
    else:
        output.addHeader({'engine name': 35}, {'test id': 15}, {'start time': 30}, {'status': 10})

    engine_list = toolkit_helpers.get_engine_list(args.all, args.dx_host, eng)
    ret = 0

    for engine_name in sorted(engine_list):
        if eng.dlpx_connect(engine_name):
            print(f"Can't connect to Dephix Engine {engine_name}\n")
            ret += 1
            continue

        if eng.getCurrentUserType() != 'SYSTEM':
            print(f"User with sysadmin role is required for this script to run. Please check config file entry for {engine_name}")
            continue

        st = StorageObj(eng, args.debug)
        st.LoadStorageTest()

        # select tests to iterate
        test_ids = []
        if args.testid:
            if args.testid.lower() == 'last':
                tlist = st.getTestList()
                if tlist:
                    test_ids = [tlist[-1]]
                else:
                    print(f"Can't find storage test on engine {engine_name}")
                    ret += 1
                    continue
            else:
                if st.isTestExist(args.testid):
                    test_ids = [args.testid]
                else:
                    print(f"Test id - {args.testid} doesn't exist on engine {engine_name}")
                    ret += 1
                    continue
        else:
            test_ids = st.getTestList()

        for tid in test_ids:
            if args.details:
                if st.getState(tid) == 'COMPLETED':
                    st.parseTestResults(tid)
                    names = GRADE_TESTS if args.gradeonly.lower() == 'yes' else FULL_TESTS
                    for testname in names:
                        output.addLine(
                            engine_name, tid, testname,
                            st.getTestIOPS(tid, testname),
                            st.getTestThoughput(tid, testname),
                            st.getLatencyGrade(tid, testname),
                            st.getLatencyAvg(tid, testname),
                            st.getLatency95(tid, testname),
                            st.getLatencyMin(tid, testname),
                            st.getLatencyMax(tid, testname),
                            st.getLatencyStdDev(tid, testname)
                        )
                else:
                    print(f"Test id - {tid} is not completed on {engine_name}")
                    ret += 1
                    continue
            elif args.iorc:
                if st.getState(tid) == 'COMPLETED':
                    if not os.path.isdir(args.iorc):
                        print(f"{args.iorc} is not a directory")
                    if os.access(args.iorc, os.W_OK):
                        filename = os.path.join(args.iorc, f"{engine_name}_IORC_{tid}.txt")
                        if st.generateIORC(tid, filename):
                            print(f"Problem with generating a IORC {filename}")
                            ret += 1
                            continue
                        else:
                            print(f"IORC saved into file {filename}")
                    else:
                        print(f"Can't write into directory {args.iorc}")
                        ret += 1
                        continue
                else:
                    print(f"Test id - {tid} is not completed on {engine_name}")
                    ret += 1
                    continue
            else:
                output.addLine(engine_name, tid, st.getStartTime(tid), st.getState(tid))

    if not args.iorc:
        toolkit_helpers.print_output(output, args.fmt, args.nohead)

    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
