#!/usr/bin/env python3

import argparse
import sys

import json_importer as sta
import orjson
import requests

if __name__ == "__main__":

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
    parser.add_argument("-q", "--quiet", action="store_true", help="Don't print output")
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
        "-l", "--line", action="store_true", help="Uses JSONline instead of JSON"
    )
    parser.add_argument(
        "-S",
        "--sequential",
        action="store_true",
        help="Processes objects in JSONline mode in sequential order",
    )

    args = parser.parse_args(sys.argv[1:])

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
            res = requests.get(args.url[0])
            obj = res.json()
        elif args.file:
            with open(args.file[0]) as f:
                obj = orjson.loads(f.read())
        else:
            obj = orjson.loads(sys.stdin.read())

        if obj.__class__ == list:
            obj = {args.root_table[0]: obj}

        importer.create_schema(jsonobj=obj)
        if not args.noimport:
            importer.insertDataToSchema(jsonobj=obj)

    elif args.line:
        if args.url:
            res = requests.get(args.url[0])
            objs = [orjson.loads(l) for l in res.text.splitlines()]
        elif args.file:
            with open(args.file[0]) as f:
                objs = [orjson.loads(l) for l in f.readlines()]
        else:
            objs = [orjson.loads(l) for l in sys.stdin.readlines()]

        if args.sequential:
            for obj in objs:
                importer.create_schema(jsonobj=obj)
                if not args.noimport:
                    importer.insertDataToSchema(jsonobj=obj)

        else:
            importer.create_schema(jsonobj={importer.root_table: objs})
            if not args.noimport:
                importer.insertDataToSchema(jsonobj={importer.root_table: objs})
