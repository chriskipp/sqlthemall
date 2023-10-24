#!/usr/bin/env python3
import argparse
import sys
import traceback
import urllib.request
from urllib.error import URLError
from io import TextIOWrapper
from http.client import HTTPResponse


try:
    import ujson as json
except ImportError:
    import json  # type: ignore

from typing import Optional, TypeVar, Iterator, Union

from sqlthemall.json_importer import SQLThemAll


def parse_json(jsonstr: Union[str, bytes]) -> Optional[Union[dict,list]]:
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
        return json.loads(jsonstr)
    except json.JSONDecodeError:
        traceback.print_exc()
        return None

def read_json(source_descriptor: Union[TextIOWrapper, HTTPResponse], lines: bool = False, batch_size: int = 100) -> Iterator[Union[dict, list]]:
    """
    Unifies reading JSON from different sources (url, file, stdin).

    Parameters:
        source_descriptor: Filedescriptor, Responsedescriptor or sys.stdin.
        lines (bool): Parse lines instead of complete source.
        batch_size (int): How many lines should be returned per yield.

    Returns:
        Iterator[Union[dict,list]]: Parsed JSON input.
    """
    if lines is False:
        yield parse_json(source_descriptor.read())
    else:
        while True:
            lines: list = []
            for _n in range(batch_size):
                line = source_descriptor.readline()
                if not line:
                    break
                obj = parse_json(line.strip())
                if obj:
                    lines.append(obj)
            if not lines:
                break
            yield lines

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

def read_from_source(args: argparse.Namespace) -> Iterator[Union[dict,list]]:
    """
    Wrapper function for read_json which instantiates the source_descriptor
    depending on the given arguments.

    Args:
        args (argparse.Namespace): List of arguments to parse.

    Returns:
        Iterator[Union[dict,list]]: Iterator over the objects provided in the
        sourde.
    """
    try:
        if args.url:
            with urllib.request.urlopen(args.url[0], timeout=300) as res:
                for j in read_json(res, lines=args.line, batch_size=args.batch_size[0]):
                    yield j
        elif args.file:
            with open(args.file[0]) as f:
                for j in read_json(f, lines=args.line, batch_size=args.batch_size[0]):
                    yield j
        else:
            for j in read_json(sys.stdin, lines=args.line, batch_size=args.batch_size[0]):
                yield j
    except URLError:
        traceback.print_exc()
        sys.exit(3)
    except Exception:
        traceback.print_exc()
        sys.exit(1)

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
        default=["main"],
        help="Name of the root table to import tthe JSON object into",
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
        nargs=1,
        type=int,
        dest="batch_size",
        default=[100],
        help="Number of objects processed per commit in JSONline mode",
    )

    return parser.parse_args(args)


def main() -> None:
    """Main function."""
    args = parse_args(sys.argv[1:])

    importer: SQLThemAll = gen_importer(args=args)

    for j in read_from_source(args=args):
        importer.import_multi_json(j)


if __name__ == "__main__":
    main()

