from __future__ import annotations

import argparse
import json
import sys

from .codec import parse_cli_value
from .interface import connect


OK = 0
BAD_ARGS = 2
BAD_KEY = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="imustore", description="Inspect and edit an ImmuStore database.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    get_parser = subparsers.add_parser("get", help="read a value")
    get_parser.add_argument("database")
    get_parser.add_argument("key")

    set_parser = subparsers.add_parser("set", help="write a JSON or text value")
    set_parser.add_argument("database")
    set_parser.add_argument("key")
    set_parser.add_argument("value")

    delete_parser = subparsers.add_parser("delete", help="delete a key")
    delete_parser.add_argument("database")
    delete_parser.add_argument("key")

    keys_parser = subparsers.add_parser("keys", help="list keys in sorted order")
    keys_parser.add_argument("database")
    keys_parser.add_argument("--prefix")

    scan_parser = subparsers.add_parser("scan", help="list key/value pairs in sorted order")
    scan_parser.add_argument("database")
    scan_parser.add_argument("--prefix")
    scan_parser.add_argument("--start")
    scan_parser.add_argument("--stop")

    stats_parser = subparsers.add_parser("stats", help="show storage statistics")
    stats_parser.add_argument("database")

    audit_parser = subparsers.add_parser("audit", help="validate tree ordering and node metadata")
    audit_parser.add_argument("database")

    compact_parser = subparsers.add_parser("compact", help="rewrite live records into a compact file")
    compact_parser.add_argument("database")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)

    db = connect(args.database)
    try:
        if args.command == "get":
            print(json.dumps(db[args.key], sort_keys=True))
        elif args.command == "set":
            db[args.key] = parse_cli_value(args.value)
            db.commit()
        elif args.command == "delete":
            del db[args.key]
            db.commit()
        elif args.command == "keys":
            for key in db.keys(prefix=args.prefix):
                print(key)
        elif args.command == "scan":
            for key, value in db.scan(prefix=args.prefix, start=args.start, stop=args.stop):
                print(json.dumps({"key": key, "value": value}, sort_keys=True))
        elif args.command == "stats":
            print(json.dumps(db.stats().__dict__, sort_keys=True))
        elif args.command == "audit":
            print(json.dumps(db.audit().as_dict(), sort_keys=True))
        elif args.command == "compact":
            print(json.dumps(db.compact().__dict__, sort_keys=True))
        else:
            parser.print_usage()
            return BAD_ARGS
    except KeyError:
        print("Key not found", file=sys.stderr)
        return BAD_KEY
    finally:
        if not db._storage.closed:
            db.close()

    return OK


if __name__ == "__main__":
    raise SystemExit(main())
