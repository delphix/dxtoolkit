#!/usr/bin/env python3
"""Python port of dx_get_appliance.pl."""
import argparse
import sys
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

from engine import Engine
from system_obj import SystemObj
from databases import Databases
from formater import Formater
import toolkit_helpers


def parse_args(argv):
    p = argparse.ArgumentParser(description="Get appliance information")
    p.add_argument('-d', '--engine', dest='dx_host')
    p.add_argument('-all', action='store_true')
    p.add_argument('-format', dest='fmt')
    p.add_argument('-output_unit', dest='output_unit', default='G')
    p.add_argument('-nohead', action='store_true')
    p.add_argument('-details', action='store_true')
    p.add_argument('-debug', dest='debug', type=int, nargs='?', const=1)
    p.add_argument('-dever', dest='dever')
    p.add_argument('-version', action='store_true')
    p.add_argument('-configfile', '-c', dest='config_file')
    return p.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    if args.version:
        print(toolkit_helpers.version)
        return 0

    if not toolkit_helpers.ensure_config_file(args.config_file):
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
        print("Option all (-all) and engine (-d|engine) are mutually exclusive")
        return 1

    output = Formater(args.debug)
    if args.details:
        output.addHeader(
            {'Appliance': 20}, {'Status': 8}, {'Version': 8},
            {toolkit_helpers.get_unit('Total', args.output_unit): 10},
            {toolkit_helpers.get_unit('Used', args.output_unit): 10},
            {toolkit_helpers.get_unit('Free', args.output_unit): 10},
            {'PctUsed(%)': 10}, {'dSource#': 8}, {'VDBs#': 8}, {'Total Objects': 8},
            {'vCpu': 8}, {'vMem [GB]': 9}, {'UUID': 40}, {'Type': 20}
        )
    else:
        output.addHeader(
            {'Appliance': 20}, {'Status': 8}, {'Version': 8},
            {toolkit_helpers.get_unit('Total', args.output_unit): 10},
            {toolkit_helpers.get_unit('Used', args.output_unit): 10},
            {toolkit_helpers.get_unit('Free', args.output_unit): 10},
            {'PctUsed(%)': 10}, {'dSource#': 8}, {'VDBs#': 8}, {'Total Objects': 8}
        )

    engine_list = toolkit_helpers.get_engine_list(args.all, args.dx_host, eng)
    ret = 0
    for engine_name in sorted(engine_list):
        status = 'UP'
        if eng.dlpx_connect(engine_name):
            status = 'DOWN'
            if args.details:
                output.addLine(engine_name, status, '', '', '', '', '', '', '', '', '', '', '', '')
            else:
                output.addLine(engine_name, status, '', '', '', '', '', '', '')
            ret += 1
            continue

        system = SystemObj(eng, args.debug)
        databases = Databases(eng, args.debug)
        vdbs = databases.getDBByType('VDB')
        dsources = databases.getDBByType('dSource')
        storage = system.getStorage()
        if args.details:
            output.addLine(
                engine_name, status, system.getVersion(),
                toolkit_helpers.print_size(storage.get('Total'), 'G', args.output_unit),
                toolkit_helpers.print_size(storage.get('Used'), 'G', args.output_unit),
                toolkit_helpers.print_size(storage.get('Free'), 'G', args.output_unit),
                storage.get('pctused'), len(dsources), len(vdbs), len(dsources) + len(vdbs),
                system.getvCPU(), f"{system.getvMem():8.2f}", system.getUUID(), system.getEngineType()
            )
        else:
            output.addLine(
                engine_name, status, system.getVersion(),
                toolkit_helpers.print_size(storage.get('Total'), 'G', args.output_unit),
                toolkit_helpers.print_size(storage.get('Used'), 'G', args.output_unit),
                toolkit_helpers.print_size(storage.get('Free'), 'G', args.output_unit),
                storage.get('pctused'), len(dsources), len(vdbs), len(dsources) + len(vdbs)
            )

    toolkit_helpers.print_output(output, args.fmt, args.nohead)
    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
