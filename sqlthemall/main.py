#!/usr/bin/env python3
"""Python file to be called from the command line programm."""

import argparse
import sys
import traceback
import urllib.request
from urllib.error import URLError

try:
    import ujson as json
except ImportError:
    import json  # type: ignore

from typing import Optional, Union

from sqlthemall.json_importer import SQLThemAll

def parse_json(jsonstr: str, line=None) -> Optional[Union[str,list]]:
    """
    Parses JSON from a string or bytes object and returns a python object.

    Parameters:
        jsonstr (str_or_bytes): String or bytes to parse JSON from.

    Returns:
        dict or list: Parsed JSON input.
    """
    try:
        if not jsonstr:
            return None
        elif line is True:
            return [json.loads(s.strip()) for s in jsonstr.splitlines()]
        elif line is False:
            return json.loads(jsonstr.strip())
        else:
            try:
                return json.loads(jsonstr)
            except json.JSONDecodeError:
                return [json.loads(s.strip()) for s in jsonstr.splitlines()]
    except json.JSONDecodeError:
        traceback.print_exc()
    return None


def parse_args(args: list[str]) -> argparse.Namespace:
    """
    Parses the provided list of args.

    Args:
        args (list[str]): List of arguments to parse.

    Returns:
        argparse.Namespace: Namespace the arguments have been read in.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--databaseurl",
        nargs=1,
        dest="dburl",
        help="Database url to use",
        default=["sqlite://"],
    )
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "-u", "--url", nargs=1, dest="url", help="URL to read JSON from"
    )
    input_group.add_argument(
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
        "-L",
        "--loglevel",
        choices=("ERROR", "WARNING", "INFO", "DEBUG"),
        default=["INFO"],
        help="Set the log level",
        nargs=1,
        dest="loglevel",
    )
    parser.add_argument(
        "-p",
        "--no_progress",
        action="store_true",
        default=False,
        help="Do not print progress while importing",
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
        help="Name of the root table to import the root of the JSON object in.",
        default=["main"],
    )
    parser.add_argument(
        "-l",
        "--line",
        action="store_true",
        help="Uses JSONline instead of JSON",
        default=None,
    )
    parser.add_argument(
        "-S",
        "--sequential",
        action="store_true",
        help="Processes objects in JSONline mode in sequential order",
    )
    parser.add_argument(
        "--sql",
        action="store_true",
        help="Returns the generated schema as sql",
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Returns a description of the generated schema as json",
    )
    parser.add_argument(
        "-N",
        "--batch_size",
        nargs=1,
        type=int,
        dest="batch_size",
        default=[100],
        help="Number of objects processed per commit in JSONline mode",
    )

    return parser, parser.parse_args(args)


def main() -> None:
    """Main function."""
    parser, args = parse_args(sys.argv[1:])

    importer = SQLThemAll(
        dburl=args.dburl[0],
        loglevel=args.loglevel[0],
        progress=not args.no_progress,
        autocommit=args.autocommit,
        simple=args.simple,
        root_table=args.root_table[0],
        echo=args.echo,
    )

    if args.url:
        try:
            if args.url[0].startswith("http"):
                with urllib.request.urlopen(args.url[0], timeout=300) as res:
                    jsonstr = res.read()
        except URLError:
            traceback.print_exc()
            sys.exit(3)
    elif args.file:
        with open(args.file[0], encoding="utf-8") as f:
            jsonstr = f.read().strip()
    elif not sys.stdin.isatty():
        jsonstr = sys.stdin.read().strip()
    else:
        parser.print_help()
        exit(5)
    obj = parse_json(jsonstr, line=args.line)

    if isinstance(obj, list):
        obj = {args.root_table[0]: obj}

    if obj is not None:
        try:
            if args.sql is True:
                importer.create_schema(jsonobj=obj, no_write=True)
                print(importer.get_sql())
            elif args.describe is True:
                importer.create_schema(jsonobj=obj, no_write=True)
                print(json.dumps(importer.describe_schema(), indent=4))
            else:
                importer.create_schema(jsonobj=obj)
                if not args.noimport:
                    importer.insert_data_to_schema(jsonobj=obj)
        except BaseException:
            traceback.print_exc()


if __name__ == "__main__":
    main()
