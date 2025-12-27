#!/usr/bin/env python3
"""Python port of dx_ctl_analytics.pl (minimal, feature-parity for control actions)
"""
import argparse
import sys
import os

# adjust import path to find lib/py
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

from engine import Engine
from analytics import Analytics
from formater import Formater
import toolkit_helpers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--engine', dest='dx_host')
    parser.add_argument('-debug', nargs='?', const=1, type=int)
    parser.add_argument('-all', action='store_true')
    parser.add_argument('-type', '-t', dest='type')
    parser.add_argument('-action', dest='action')
    parser.add_argument('-format', dest='format')
    parser.add_argument('-nohead', action='store_true')
    parser.add_argument('-dever', dest='dever')
    parser.add_argument('-version', action='store_true')
    parser.add_argument('-configfile', '-c', dest='config_file')
    args = parser.parse_args()

    version = toolkit_helpers.version
    if args.version:
        print(version)
        sys.exit(0)

    if not toolkit_helpers.ensure_config_file(args.config_file):
        sys.exit(1)

    if not (args.type and args.action):
        print("Option -action and -type are mandatory")
        parser.print_help()
        sys.exit(1)

    if args.action.lower() not in ('create', 'delete', 'display', 'stop', 'start', 'restart'):
        print(f"Option -action has a wrong argument {args.action}")
        parser.print_help()
        sys.exit(1)

    if args.action.lower() in ('create', 'delete') and args.type.lower() not in ('nfs-all', 'nfs-by-client', 'iscsi-by-client'):
        print('Create or delete action can be done with those types only : nfs-all, nfs-by-client or iscsi-by-client')
        sys.exit(1)

    engine_obj = Engine(args.dever, args.debug)
    try:
        engine_obj.load_config(args.config_file)
    except FileNotFoundError:
        print(f"ERROR: config file not found: {args.config_file}")
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: failed to load config file {args.config_file}: {exc}")
        sys.exit(1)

    engine_list = toolkit_helpers.get_engine_list(args.all, args.dx_host, engine_obj)
    ret = 0

    for engine in sorted(engine_list):
        if engine_obj.dlpx_connect(engine):
            print(f"Can't connect to Dephix Engine {engine}\n\n")
            ret += 1
            continue
        else:
            print(f"Connected to Delphix Engine {engine} (IP {engine_obj.getIP()})\n\n")

        analytic_list = Analytics(engine_obj, args.debug)

        action = args.action.lower()
        type_ = args.type

        if action == 'create':
            if analytic_list.create_analytic(type_):
                ret += 1
                continue
        elif action == 'delete':
            analytic = analytic_list.getAnalyticByName(type_)
            if analytic:
                if analytic.delete_analytic():
                    ret += 1
                    continue
            else:
                print(f"Analytics {type_} not found")
                ret += 1
                continue
        else:
            analytic_array = []
            if type_.lower() == 'all':
                for ref in analytic_list.getAnalyticsList():
                    analytic_array.append(analytic_list.getName(ref))
            elif type_.lower() == 'standard':
                analytic_array = ['cpu', 'network', 'disk', 'nfs']
            else:
                for n in type_.split(','):
                    if analytic_list.getAnalyticByName(n):
                        analytic_array.append(n)
                    else:
                        print(f"Analytic name {n} not found. It will be not included in output")

            if len(analytic_array) < 1:
                print("Can't find an analytic")
                ret += 1
                continue

            output = Formater()
            output.addHeader({'Engine': 20}, {'Analytic': 20}, {'State': 20}, {'Axes': 100})

            for n in sorted(analytic_array):
                anal = analytic_list.getAnalyticByName(n)
                if action == 'display':
                    output.addLine(engine, n, anal.getState(), anal.getAxes())
                elif action == 'stop':
                    anal.pause_analytic()
                elif action == 'start':
                    anal.resume_analytic()
                elif action == 'restart':
                    anal.pause_analytic()
                    anal.resume_analytic()

            if action == 'display':
                toolkit_helpers.print_output(output, args.format, args.nohead)

    sys.exit(ret)


if __name__ == '__main__':
    main()
