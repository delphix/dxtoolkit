#!/usr/bin/env python3
"""Python port of dx_get_capacity.pl."""
import argparse
import sys
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

from engine import Engine
from capacity_obj import CapacityObj
from databases import Databases
from group_obj import GroupObj
from formater import Formater
import toolkit_helpers


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Get information about databases space usage")
    parser.add_argument('-d', '--engine', dest='dx_host')
    parser.add_argument('-all', action='store_true')
    parser.add_argument('-name', dest='dbname')
    parser.add_argument('-format', dest='fmt')
    parser.add_argument('-type', dest='dbtype')
    parser.add_argument('-group', dest='group')
    parser.add_argument('-dsource', dest='dsource')
    parser.add_argument('-host', dest='host')
    parser.add_argument('-sortby', dest='sortby')
    parser.add_argument('-forcerefresh', action='store_true')
    parser.add_argument('-dbdetails', action='store_true')
    parser.add_argument('-debug', dest='debug', type=int, nargs='?', const=1)
    parser.add_argument('-output_unit', dest='output_unit', default='G')
    parser.add_argument('-details', dest='details', nargs='?', const='')
    parser.add_argument('-dever', dest='dever')
    parser.add_argument('-unvirt', action='store_true')
    parser.add_argument('-version', action='store_true')
    parser.add_argument('-nohead', action='store_true')
    parser.add_argument('-configfile', '-c', dest='config_file')
    return parser.parse_args(argv)


def is_vdb(dbobj):
    parent = dbobj.get('parent') if isinstance(dbobj, dict) else None
    if parent:
        return True
    dtype = str(dbobj.get('type', '')).lower() if isinstance(dbobj, dict) else ''
    return 'virtual' in dtype


def replica_flag(databases, dbref):
    return 'YES' if databases.isReplica(dbref) else 'NO'


def format_size(val, unit):
    return toolkit_helpers.print_size(val, 'G', unit)


def main(argv):
    args = parse_args(argv)
    if args.version:
        print(toolkit_helpers.version)
        return 0

    if not toolkit_helpers.ensure_config_file(args.config_file):
        return 1

    unit = str(args.output_unit or 'G').upper()
    if unit not in ('K', 'M', 'G', 'T'):
        print("Option -output_unit can be only K for KB, M for MB, G for GB and T for TB")
        return 1

    if args.details is not None and args.dbdetails:
        print("Options -details and -dbdetails are mutually exclusive")
        return 1

    if args.sortby and args.sortby.lower() != 'size':
        print("Option sortby can have only size")
        return 1

    if args.all and args.dx_host:
        print("Option all (-all) and engine (-d|engine) are mutually exclusive")
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
    if not engine_list:
        print("There is no engine selected to process. Please check config file.")
        return 1

    output = Formater(args.debug)
    headers = [
        {'Engine': 30},
        {'Group': 20},
        {'Database': 35},
        {'Replica': 3},
        {toolkit_helpers.get_unit('Size', unit): 10},
    ]

    if args.details is not None:
        headers.append({'Type': 20})
        headers.append({toolkit_helpers.get_unit('Size', unit): 10})
        if args.details and args.details.lower() == 'all':
            headers.append({'Snapshots': 35})
            headers.append({toolkit_helpers.get_unit('Size', unit): 10})
    else:
        if args.unvirt:
            headers.append({toolkit_helpers.get_unit('Unvirt', unit): 11})
        if args.dbdetails:
            headers.append({'Environment name': 30})
            headers.append({'Parent': 30})

    output.addHeader(*headers)

    ret = 0
    for engine_name in sorted(engine_list):
        if eng.dlpx_connect(engine_name):
            print(f"Can't connect to Delphix Engine {engine_name}\n")
            ret += 1
            continue

        cap = CapacityObj(eng, args.debug)
        if args.forcerefresh:
            if cap.forcerefesh():
                print(f"Problem with forcerefesh. Skipping results for engine {engine_name}")
                continue

        databases = Databases(eng, args.debug)
        groups = GroupObj(eng, args.debug)
        cap.LoadDatabases()

        db_list = toolkit_helpers.get_dblist_from_filter(
            args.dbtype,
            args.group,
            args.host,
            args.dbname,
            databases,
            groups,
            debug=args.debug,
            dsource=args.dsource,
        )

        if not db_list:
            print(f"There is no DB selected to process on {engine_name}. Please check filter definitions.")
            ret = 1
            continue

        for dbref in db_list:
            dbobj = databases.getDB(dbref)
            if not dbobj:
                continue

            cap_hash = cap.getDetailedDBUsage(dbref, args.details)
            if cap_hash.get('snapshots_shared') == 0 and cap_hash.get('snapshots_total') == 0:
                continue

            grp_name = groups.getName(dbobj.get('group')) or ''
            db_name = databases.getName(dbref) or ''
            replica = replica_flag(databases, dbref)

            if args.details is None:
                line = [
                    engine_name,
                    grp_name,
                    db_name,
                    replica,
                    format_size(cap_hash.get('totalsize'), unit),
                ]

                if args.unvirt:
                    line.append(format_size(cap_hash.get('unvirtualized'), unit))

                if args.dbdetails:
                    env_name = databases.getEnvironmentName(dbref) or ''
                    parent_name = databases.getParentName(dbref)
                    if is_vdb(dbobj):
                        parent_name = parent_name or 'N/A - deleted'
                    else:
                        parent_name = 'N/A - dSource'
                    line.extend([env_name, parent_name])

                output.addLine(*line)
                continue

            # details mode
            detail_val = args.details or ''
            if detail_val == '':
                output.addLine(
                    engine_name,
                    grp_name,
                    db_name,
                    replica,
                    format_size(cap_hash.get('totalsize'), unit),
                    '',
                    '',
                )
                output.addLine('', '', '', '', '', 'Current copy', format_size(cap_hash.get('currentcopy'), unit))
                output.addLine('', '', '', '', '', 'DB Logs', format_size(cap_hash.get('dblogs'), unit))
                output.addLine('', '', '', '', '', 'Snapshots total', format_size(cap_hash.get('snapshots_total'), unit))
            elif detail_val.lower() == 'all':
                output.addLine(
                    engine_name,
                    grp_name,
                    db_name,
                    replica,
                    format_size(cap_hash.get('totalsize'), unit),
                    '',
                    '',
                    '',
                    '',
                )
                output.addLine('', '', '', '', '', 'Current copy', format_size(cap_hash.get('currentcopy'), unit), '', '')
                output.addLine('', '', '', '', '', 'DB Logs', format_size(cap_hash.get('dblogs'), unit), '', '')
                output.addLine('', '', '', '', '', 'Snapshots total', format_size(cap_hash.get('snapshots_total'), unit), '', '')
                output.addLine('', '', '', '', '', '', '', 'Snapshots shared', format_size(cap_hash.get('snapshots_shared'), unit))
                for snap in cap_hash.get('snapshots_list', []):
                    snap_name = f"Snapshot {toolkit_helpers.convert_from_utc(snap.get('snapshotTimestamp'))}"
                    output.addLine('', '', '', '', '', '', '', snap_name, format_size(snap.get('space'), unit))

        held_array = cap.getStorageContainers()
        if held_array:
            for hs in held_array:
                held_hash = cap.getDetailedDBUsage(hs, None)
                groupname = held_hash.get('group_name') or 'N/A'
                base_line = [
                    engine_name,
                    groupname,
                    f"Held space - {held_hash.get('storageContainer')}",
                    'N/A',
                    format_size(held_hash.get('totalsize'), unit),
                ]
                # fill remaining columns with blanks for alignment
                if output.getHeaderSize() - len(base_line) > 0:
                    base_line.extend([''] * (output.getHeaderSize() - len(base_line)))
                output.addLine(*base_line)

    if args.details is None and args.sortby and args.sortby.lower() == 'size':
        output.sortbynumcolumn(4)

    toolkit_helpers.print_output(output, args.fmt, args.nohead)
    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
