#!/usr/bin/env python3

import pytest
from sqlalchemy import MetaData, create_engine

from sqlthemall.json_importer import SQLThemAll


@pytest.mark.parametrize("dburl", [None, "sqlite://", "sqlite:///test.sqlite"])
def test_importer_engine(dburl):
    """
    Tests various database url connection strings.

    Attributes:
        dburl (str): Database URL.
    """
    if dburl is None:
        importer = SQLThemAll()
        dburl = "sqlite://"
    else:
        importer = SQLThemAll(dburl=dburl)
    assert importer.engine.url == create_engine(dburl).url


@pytest.mark.parametrize(
    "root_table", [None, "main", "test", "table", 0, 1, 0.1, True, False]
)
def test_importer_root_table(root_table):
    """
    Tests various names and other values as root_table names.

    Attributes:
        root_table (str): Name of the database root table.
    """
    if root_table is None:
        importer = SQLThemAll()
        root_table = "main"
    else:
        importer = SQLThemAll(root_table=root_table)
    assert importer.root_table == str(root_table)


@pytest.mark.parametrize("quiet", [None, True, False])
def test_importer_quiet(quiet):
    """
    Tests various values of the quiet option.

    Attributes:
        quiet (obj): Value of the quiet option.
    """
    if quiet is None:
        importer = SQLThemAll()
        quiet = True
    else:
        importer = SQLThemAll(quiet=quiet)
    assert importer.quiet == quiet


@pytest.mark.parametrize("verbose", [None, True, False])
def test_importer_verbose(verbose):
    """
    Tests various values of the verbose option.

    Attributes:
        verbose (obj): Value of the verbose option.
    """
    if verbose is None:
        importer = SQLThemAll()
        verbose = False
    else:
        importer = SQLThemAll(verbose=verbose)
    assert importer.verbose == verbose


@pytest.mark.parametrize("simple", [None, True, False])
def test_importer_simple(simple):
    """
    Tests various values of the simple database scheme option.

    Attributes:
        simple (obj): Value of the simple option.
    """
    if simple is None:
        importer = SQLThemAll()
        simple = False
    else:
        importer = SQLThemAll(simple=simple)
    assert importer.simple == simple


def test_importer_initial_connection():
    """
    Tests the initial status of the connetion attribute.
    """
    importer = SQLThemAll()
    assert importer.connection is None


def test_importer_metadata():
    """
    Tests the correctnes of the importer.metadata.
    """
    importer = SQLThemAll()
    engine = importer.engine
    metadata = MetaData(bind=engine)
    metadata.reflect()
    assert importer.metadata.sorted_tables == metadata.sorted_tables
