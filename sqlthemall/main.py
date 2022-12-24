#!/usr/bin/env python3
"""
Python file to be called from the command line programm
"""

import argparse
import sys
import traceback

import requests
import ujson as json

import sqlthemall.json_importer as sta

def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--databaseurl",
        nargs=1,
        dest="dburl",
        required=True,
        help="Database url to use",
    )
    parser.add_argument(
        "-u", "--url", nargs=1, dest="url", help="URL to read JSON from"
    )
    parser.add_argument(
        "-f",
        "--file",
        nargs=1,
        dest="file",
        help="File to read JSON from (default: stdin)",
    )
    parser.add_argument(
        "-s",
        "--simple",
        action="store_true",
        help="Creates a simple database schema (no mtm)",
    )
    parser.add_argument(
        "-n",
        "--noimport",
        action="store_true",
        help="Only creates database schema, skips import",
    )
    parser.add_argument(
        "-a",
        "--autocommit",
        action="store_true",
        help="Opens database in autocommit mode",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print verbose output"
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Don't print output"
    )
    parser.add_argument(
        "-e",
        "--echo",
        action="store_true",
        help="Print SQL statements (engine.echo = True)",
    )
    parser.add_argument(
        "-t",
        "--root-table",
        nargs=1,
        dest="root_table",
        help="Name of the table to import the outermost subobjects if JSON object is a array",
    )
    parser.add_argument(
        "-l",
        "--line",
        action="store_true",
        help="Uses JSONline instead of JSON",
    )
    parser.add_argument(
        "-S",
        "--sequential",
        action="store_true",
        help="Processes objects in JSONline mode in sequential order",
    )
    parser.add_argument(
        "-N",
        "--batch_size",
        action="store_true",
        dest="batch_size",
        default=100,
        help="Number of objects processed per commit in JSONline mode",
    )

    return parser.parse_args(args)

def main():
    """main function"""
    args = parse_args(sys.argv[1:])

    if not args.root_table:
        args.root_table = ["main"]

    importer = sta.SQLThemAll(
        dburl=args.dburl[0],
        quiet=args.quiet,
        verbose=args.verbose,
        autocommit=args.autocommit,
        simple=args.simple,
        root_table=args.root_table[0],
        echo=args.echo,
    )

    if not args.line:
        if args.url:
            res = requests.get(args.url[0], timeout=300)
            obj = res.json()
        elif args.file:
            with open(args.file[0], encoding="utf-8") as f:
                jsonstr = f.read().strip()
            if not jsonstr:
                sys.stderr.write("Can not parse JSON from empty string!\n")
                sys.exit(1)
            try:
                obj = json.loads(jsonstr)
            except json.JSONDecodeError as e:
                sys.stderr.write("Can not parse JSON string!\n")
                sys.stderr.write(str(e) + "\n")
                sys.stderr.write(str(jsonstr) + "\n")
                sys.exit(1)
        else:
            jsonstr = sys.stdin.read().strip()
            if not jsonstr:
                sys.stderr.write("Can not parse JSON from empty string!\n")
                sys.exit()
            try:
                obj = json.loads(jsonstr)
            except json.JSONDecodeError as e:
                sys.stderr.write("Can not parse JSON string!\n")
                sys.stderr.write(str(e) + "\n")
                sys.stderr.write(str(jsonstr) + "\n")
                sys.exit(1)

        if obj.__class__ == list:
            obj = {args.root_table[0]: obj}

        importer.create_schema(jsonobj=obj)
        if not args.noimport:
            importer.insert_data_to_schema(jsonobj=obj)

    elif args.line:
        if args.url:
            res = requests.get(args.url[0], timeout=300)
            objs = [json.loads(line) for line in res.text.splitlines()]
        elif args.file:
            with open(args.file[0], encoding="utf-8") as f:
                objs = [json.loads(line.strip()) for line in f.readlines()]
        else:
            if not args.sequential:
                objs = [json.loads(line.strip()) for line in sys.stdin.readlines()]
                obj = {importer.root_table: objs}
                importer.create_schema(jsonobj=obj)
                if not args.noimport:
                    importer.insert_data_to_schema(jsonobj=obj)

            else:
                while True:
                    lines = []
                    for n in range(args.batch_size[0]):
                        line = sys.stdin.readline()
                        if not line:
                            break
                        try:
                            lines.append(json.loads(line.strip()))
                        except json.JSONDecodeError:
                            traceback.print_exc()
                            continue
                    if not lines:
                        break

                    try:
                        obj = {importer.root_table: lines}
                        importer.create_schema(jsonobj=obj)
                        if not args.noimport:
                            importer.insert_data_to_schema(jsonobj=obj)
                    except BaseException:
                        traceback.print_exc()
                        for line in lines:
                            try:
                                obj = {importer.root_table: [line]}
                                importer.create_schema(jsonobj=obj)
                                if not args.noimport:
                                    importer.insert_data_to_schema(jsonobj=obj)
                            except BaseException:
                                traceback.print_exc()


if __name__ == "__main__":
    main()
