#!/usr/bin/env python3

from pathlib import Path

import pytest

try:
    import ujson as json
except ImportError:
    import json  # type: ignore

from sqlalchemy.orm import Session

from sqlthemall.json_importer import SQLThemAll
from tests.utils import compare_obj, dbobj2obj


def readfile(inputfile):
    """
    Reads the content of the provided filename.

    Attributes:
        inputfile (str): Name or path of the file to read.
    """
    with open(inputfile) as f:
        return f.read().strip()


paths = [p.as_posix()[14:-5] for p in Path("data/testdata/").glob("*.json")]


@pytest.mark.parametrize("path", paths)
def test_schema_generation(path):
    """
    Tests the correcness of the generated db schema.

    Attributes:
        path (str): Path of a JSON file to create the schema from.
    """
    jsonobj = json.loads(readfile("data/testdata/" + path + ".json"))
    schema = readfile("data/testvalidate/" + path + ".schema")
    importer = SQLThemAll()
    importer.create_schema(jsonobj)
    assert str(importer.metadata.sorted_tables) == schema


@pytest.mark.parametrize("path", paths)
def test_schema_generation_simple(path):
    """
    Tests the correcness of the generated db schema (with simple db schema).

    Attributes:
        path (str): Path of a JSON file to create the schema from.
    """
    jsonobj = json.loads(readfile("data/testdata/" + path + ".json"))
    schema = readfile("data/testvalidate/" + path + ".simple_schema")
    importer = SQLThemAll(simple=True)
    importer.create_schema(jsonobj)
    assert str(importer.metadata.sorted_tables) == schema


objects = [
    json.loads(readfile("data/testdata/" + p + ".json"))
    for p in paths
    if p.startswith("object")
]
arrays = [
    json.loads(readfile("data/testdata/" + p + ".json"))
    for p in paths
    if p.startswith("array")
]
root_tables = ["main", "name", "test1", 1, True, False, None]


def tablename(c):
    """
    Returns table name of the provided orm object class.

    Attributes:
        c: Instance of the Sqlalchemy orm class.
    """
    return c.__table__.name


@pytest.mark.parametrize("obj", objects)
@pytest.mark.parametrize("simple", [True, False])
@pytest.mark.parametrize("root_table", root_tables)
def test_import_json(obj, simple, root_table):
    """
    Tests the correcness of the imported data in the database (with simple db schema).

    Attributes:
        path (str): Path of a JSON file import the data from.
    """
    importer = SQLThemAll(simple=simple, root_table=root_table)
    importer.import_json(obj)
    root_class = [
        c for c in importer.classes if tablename(c) == importer.root_table
    ][0]
    with Session(importer.engine) as session:
        if session.query(root_class).all():
            dbobj = session.query(root_class).all()[0]
            for a in [i for i in dir(dbobj) if i.endswith("_collection")]:
                # dbobj.__getattribute__(a)
                getattr(dbobj, a)
            assert compare_obj(dbobj2obj(dbobj), obj)


@pytest.mark.parametrize("array", arrays)
@pytest.mark.parametrize("simple", [True, False])
@pytest.mark.parametrize("root_table", root_tables)
def test_import_multi_json(array, simple, root_table):
    """
    Tests the correcness of the imported data in multi_import mode.

    Attributes:
        array (str): JSON array to import.
        simple (bool): Value of the simple db scheme option.
        root_table (str): Name of the root_table to use.
    """
    importer = SQLThemAll(simple=simple, root_table=root_table)
    importer.import_multi_json(array)
    root_class = [
        c for c in importer.classes if tablename(c) == importer.root_table
    ][0]
    with Session(importer.engine) as session:
        dbobjs = session.query(root_class).all()
        for dbobj, obj in zip(dbobjs, array):
            for a in [i for i in dir(dbobj) if i.endswith("_collection")]:
                getattr(dbobj, a)
            assert compare_obj(dbobj2obj(dbobj), obj)
