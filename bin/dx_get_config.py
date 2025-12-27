#!/usr/bin/env python3
"""Python port of dx_get_config.pl."""
import argparse
import sys
import os
import json

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'lib', 'py'))

from engine import Engine
from system_obj import SystemObj
from storage_obj import StorageObj
from formater import Formater
import toolkit_helpers


def parse_args(argv):
    p = argparse.ArgumentParser(description="Get engine configuration")
    p.add_argument('-d', '--engine', dest='dx_host')
    p.add_argument('-all', action='store_true')
    p.add_argument('-format', dest='fmt')
    p.add_argument('-backup', dest='backup')
    p.add_argument('-nohead', action='store_true')
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

    if args.backup:
        if not os.path.isdir(args.backup):
            print(f"Path {args.backup} is not a directory")
            return 1
        if not os.access(args.backup, os.W_OK):
            print(f"Path {args.backup} is not writtable")
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
    output.addHeader({'engine name': 35}, {'parameter name': 30}, {'value': 30})

    engine_list = toolkit_helpers.get_engine_list(args.all, args.dx_host, eng)
    ret = 0

    for engine_name in sorted(engine_list):
        if eng.dlpx_connect(engine_name):
            print(f"Can't connect to Dephix Engine {engine_name}\n")
            ret += 1
            continue

        if eng.getCurrentUserType() != 'SYSTEM':
            print(f"User with sysadmin role is required for this script to run. Please check config file entry for {engine_name}")
            ret += 1
            continue

        system = SystemObj(eng, args.debug)
        storage = StorageObj(eng, args.debug)
        storage.getDisks(0)

        config = {
            "engine": {
                "type": system.getEngineType(),
                "password": "adminpass_changeme",
                "email": "admin@delphix.com",
            },
            "dns": {
                "dns_server": ','.join(system.getDNSServers()),
                "dns_domain": ','.join(system.getDNSDomains()),
                "source": system.getDNSSource(),
            },
            "snmp": {
                "status": system.getSNMPStatus(),
                "snmp_servers": ','.join([s.get('address','') for s in system.getSNMPServers()]),
                "snmp_severity": system.getSNMPSeverity(),
            },
            "time": {
                "ntp_server": ','.join(system.getNTPServer()),
                "ntp_status": system.getNTPStatus(),
                "timezone": eng.getTimezone(),
            },
            "smtp": {},
            "syslog": {
                "status": system.getSyslogStatus(),
                "servers": system.getSyslogServers(),
                "severity": system.getSyslogSeverity(),
            },
            "ldap": {
                "status": system.getLDAPStatus(),
            },
            "storage": storage.getDisks(0),
            "sso": {
                "status": system.getSSOStatus(),
            },
        }

        smtpserver = system.getSMTPServer()
        if smtpserver != 'N/A':
            config["smtp"]["server"] = smtpserver
            config["smtp"]["status"] = system.getSMTPStatus()

        if config["ldap"]["status"] == 'Enabled':
            ser = system.getLDAPServers() or {}
            config["ldap"]["server"] = {
                "server": ser.get('host'),
                "port": ser.get('port'),
                "ssl": ser.get('useSSL'),
                "authentication": ser.get('authMethod'),
            }

        if config["sso"]["status"] == 'Enabled':
            config["sso"]["entityId"] = system.getSSOEntityId()
            config["sso"]["samlMetadata"] = system.getSSOsamlMetadata()
            config["sso"]["maxAuthenticationAge"] = system.getSSOmaxAuthenticationAge()
            config["sso"]["responseSkewTime"] = system.getSSOresponseSkewTime()

        if args.backup:
            filename = os.path.join(args.backup, f"{eng.getEngineName()}.json")
            with open(filename, 'w') as fd:
                print(f"Exporting configuration into file {filename} ")
                json.dump(config, fd, indent=2)
        else:
            for confclass in sorted(config.keys()):
                if confclass == 'storage':
                    continue
                for par in sorted(config[confclass].keys()):
                    output.addLine(engine_name, f"{confclass}_{par}", config[confclass][par])

    if not args.backup:
        toolkit_helpers.print_output(output, args.fmt, args.nohead)

    return ret


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
