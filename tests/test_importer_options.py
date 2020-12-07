#!/usr/bin/env python3

import pytest
from sqlalchemy import MetaData, create_engine

from sqlthemall.json_importer import SQLThemAll


@pytest.mark.parametrize("dburl", [None, "sqlite://", "sqlite:///test.sqlite"])
def test_importer_engine(dburl):
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
    if root_table is None:
        importer = SQLThemAll()
        root_table = "main"
    else:
        importer = SQLThemAll(root_table=root_table)
    assert importer.root_table == str(root_table)


@pytest.mark.parametrize("quiet", [None, True, False])
def test_importer_quiet(quiet):
    if quiet is None:
        importer = SQLThemAll()
        quiet = True
    else:
        importer = SQLThemAll(quiet=quiet)
    assert importer.quiet == quiet


@pytest.mark.parametrize("verbose", [None, True, False])
def test_importer_verbose(verbose):
    if verbose is None:
        importer = SQLThemAll()
        verbose = False
    else:
        importer = SQLThemAll(verbose=verbose)
    assert importer.verbose == verbose


@pytest.mark.parametrize("simple", [None, True, False])
def test_importer_simple(simple):
    if simple is None:
        importer = SQLThemAll()
        simple = False
    else:
        importer = SQLThemAll(simple=simple)
    assert importer.simple == simple


@pytest.mark.parametrize("autocommit", [None, True, False])
def test_importer_autocommit(autocommit):
    if autocommit is None:
        importer = SQLThemAll()
        autocommit = False
    else:
        importer = SQLThemAll(autocommit=autocommit)
    assert importer.autocommit == autocommit


def test_importer_initial_connection():
    importer = SQLThemAll()
    assert importer.connection is False


def test_importer_metadata():
    importer = SQLThemAll()
    engine = importer.engine
    metadata = MetaData(bind=engine)
    metadata.reflect()
    assert importer.metadata.sorted_tables == metadata.sorted_tables
