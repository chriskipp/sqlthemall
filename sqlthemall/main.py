#!/usr/bin/env python3

"""This is the entry point for the command line script `sqlthemall`."""

import argparse
import sys
import traceback
import urllib.request
from http.client import HTTPResponse
from io import TextIOWrapper
from urllib.error import URLError

try:
    import ujson as json
except ImportError:
    import json  # type: ignore

from typing import Iterator, Optional, Union

from sqlthemall.json_importer import SQLThemAll


def parse_json(jsonstr: str, line=None) -> Optional[Union[str, list]]:
    """
    Parses JSON from a string or bytes object and returns a python object.

    Parameters:
        jsonstr (str or bytes): String or bytes to parse JSON from.

    Returns:
        dict or list: Parsed JSON input.
    """
    if not jsonstr:
        return None
    try:
        if not jsonstr:
            return None
        if line is True:
            return [json.loads(s.strip()) for s in jsonstr.splitlines()]
        if line is False:
            return json.loads(jsonstr.strip())
        try:
            return json.loads(jsonstr)
        except json.JSONDecodeError:
            return [json.loads(s.strip()) for s in jsonstr.splitlines()]
    except json.JSONDecodeError:
        traceback.print_exc()
    return None


def read_json(
    source_descriptor: Union[TextIOWrapper, HTTPResponse],
    lines: bool = False,
    batch_size: int = 100,
) -> Iterator[Union[dict, list]]:
    """
    Unifies reading JSON from different sources (url, file, stdin).

    Parameters:
        source_descriptor: Filedescriptor, Responsedescriptor or sys.stdin.
        lines (bool): Parse lines instead of complete source.
        batch_size (int): How many lines should be returned per yield.

    Returns:
        Iterator[dict|list]: Parsed JSON input.
    """
    if lines is False:
        yield parse_json(source_descriptor.read())
    else:
        while True:
            _lines: list = []
            for _n in range(batch_size):
                line = source_descriptor.readline()
                if not line:
                    break
                obj = parse_json(line.strip())
                if obj:
                    _lines.append(obj)
            if not _lines:
                break
            yield _lines


def gen_importer(args: argparse.Namespace) -> SQLThemAll:
    """
    Generates the Importer depending on the given arguments.

    Args:
        args (argparse.Namespace): List of arguments to parse.

    Returns:
        SQLThemAll: Importer.
    """
    return SQLThemAll(
        dburl=args.dburl[0],
        loglevel=args.loglevel[0],
        progress=not args.no_progress,
        autocommit=args.autocommit,
        simple=args.simple,
        root_table=args.root_table[0],
        echo=args.echo,
    )


def read_from_source(
    args: argparse.Namespace,
) -> Iterator[Union[dict, list]]:
    """
    Wrapper function for read_json which instantiates the source_descriptor.

    depending on the given arguments.

    Args:
        args (argparse.Namespace): List of arguments to parse.

    Returns:
        Iterator[dict|list]: Iterator over the objects
        provided in the sourde.
    """
    try:
        if args.url:
            with urllib.request.urlopen(args.url[0], timeout=300) as res:
                yield from read_json(
                    res, lines=args.line, batch_size=args.batch_size[0]
                )
        elif args.file:
            with open(args.file[0]) as f:
                yield from read_json(
                    f, lines=args.line, batch_size=args.batch_size[0]
                )
        else:
            yield from read_json(
                sys.stdin, lines=args.line, batch_size=args.batch_size[0]
            )
    except URLError:
        traceback.print_exc()
        sys.exit(3)
    except BaseException as e:
        traceback.print_exc()
        raise e


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
        help="Name of the root table to import JSON.",
        default=["main"],
    )
    parser.add_argument(
        "-l",
        "--line",
        action="store_true",
        help="Uses JSONline instead of JSON",
        default=False,
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
        sys.exit(5)
    obj = parse_json(jsonstr, line=args.line)

    if isinstance(obj, list):
        obj = {args.root_table[0]: obj}

    if obj is not None:
        try:
            if args.sql is True:
                importer.create_schema(jsonobj=obj, no_write=True)
                sys.stdout.write(importer.get_sql())
            elif args.describe is True:
                importer.create_schema(jsonobj=obj, no_write=True)
                sys.stdout.write(
                    json.dumps(importer.describe_schema(), indent=4)
                )
            else:
                importer.create_schema(jsonobj=obj)
                if not args.noimport:
                    importer.insert_data_to_schema(jsonobj=obj)
        except Exception:
            traceback.print_exc()


if __name__ == "__main__":
    main()
