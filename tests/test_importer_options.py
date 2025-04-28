#!/usr/bin/env python3

import pytest
from sqlalchemy import Connection, MetaData, create_engine

from sqlthemall.json_importer import SQLThemAll
from sqlthemall.main import gen_importer, parse_args, read_from_source


DEFAULT_ROOT_TABLE = "main"

required_args = ["-d", "sqlite://"]


@pytest.mark.parametrize("dburl", [None, "sqlite://", "sqlite:///test.sqlite"])
def test_importer_engine(dburl):
    """
    Tests various database url strings.

    Attributes:
        dburl (str): Database URL.
    """
    if dburl is None:
        importer = SQLThemAll()
        dburl = "sqlite://"
    else:
        parser, args = parse_args(["-d", dburl])
        importer = gen_importer(args)
    assert importer.engine.url == create_engine(dburl).url


@pytest.mark.parametrize(
    "root_table", [None, "main", "test", "table", "0", "1", "0.1"]
)
def test_importer_root_table(root_table):
    """
    Tests various names and other values as root_table names.

    Attributes:
        root_table (str): Name of the database root table.
    """
    if root_table is None:
        parser, args = parse_args(required_args)
        root_table = DEFAULT_ROOT_TABLE
    else:
        parser, args = parse_args(required_args + ["--root-table", root_table])
    importer = gen_importer(args)
    assert importer.root_table == str(root_table).lower()


@pytest.mark.parametrize("progress", [True, False])
def test_importer_progress(progress):
    """
    Tests various values of the progress option.

    Attributes:
        progress (obj): Value of the progress option.
    """
    if progress is False:
        parser, args = parse_args(required_args + ["--no_progress"])
    else:
        parser, args = parse_args(required_args)
    importer = gen_importer(args)
    assert importer.progress is progress


@pytest.mark.parametrize(
    "loglevel", [None, "INFO", "DEBUG", "WARNING", "ERROR"]
)
def test_importer_loglevel(loglevel):
    """
    Tests various values of the loglevel option.

    Attributes:
        loglevel (obj): Value of the loglevel option.
    """
    if loglevel is None:
        parser, args = parse_args(required_args)
        loglevel = "INFO"
    else:
        parser, args = parse_args(required_args + ["--loglevel", loglevel])
    importer = gen_importer(args)
    assert importer.loglevel == loglevel


@pytest.mark.parametrize("simple", [True, False])
def test_importer_simple(simple):
    """
    Tests various values of the simple database scheme option.

    Attributes:
        simple (obj): Value of the simple option.
    """
    if simple is False:
        parser, args = parse_args(required_args)
    else:
        parser, args = parse_args(required_args + ["--simple"])
    importer = gen_importer(args)
    assert importer.simple is simple


@pytest.mark.parametrize("url", ["https://restcountries.com/v2/all"])
def test_url_argument(url):
    """
    Tests "--url" argument to read JSON from.

    Attributes:
        url (str): URL to load JSON from.
    """
    parser, args = parse_args(required_args + ["--url", url])
    for obj in read_from_source(args):
        assert isinstance(obj, (dict, list))


@pytest.mark.parametrize("file", ["data/example.json"])
def test_file_argument(file):
    """
    Tests "--file" argument to read JSON from.

    Attributes:
        file (str): File path to load JSON from.
    """
    parser, args = parse_args(required_args + ["--file", file])
    for obj in read_from_source(args):
        assert isinstance(obj, (dict, list))


@pytest.mark.parametrize("file", ["data/json_lines.json"])
def test_line_argument(file):
    """
    Tests "--line" argument to read JSON lines.

    Attributes:
        file (str): File path to load JSON from.
    """
    parser, args = parse_args(required_args + ["--line", "--file", file])
    for obj in read_from_source(args):
        assert isinstance(obj, (dict, list))


def test_importer_metadata():
    """Tests the correctnes of the importer.metadata."""
    importer = SQLThemAll()
    engine = importer.engine
    metadata = MetaData()
    metadata.reflect(bind=engine)
    assert importer.metadata.sorted_tables == metadata.sorted_tables
