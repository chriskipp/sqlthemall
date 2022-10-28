#!/usr/bin/env python3

import json
from pathlib import Path

import pytest

from sqlthemall.json_importer import SQLThemAll
from tests.utils import compare_obj, dbobj2obj


def readfile(inputfile):
    with open(inputfile) as f:
        return f.read().strip()


paths = [p.as_posix()[14:-5] for p in Path("data/testdata/").glob("*.json")]


@pytest.mark.parametrize("path", paths)
def test_schema_generation(path):
    jsonobj = json.loads(readfile("data/testdata/" + path + ".json"))
    schema = readfile("data/testvalidate/" + path + ".schema")
    importer = SQLThemAll()
    importer.create_schema(jsonobj)
    assert str(importer.metadata.sorted_tables) == schema


@pytest.mark.parametrize("path", paths)
def test_schema_generation_simple(path):
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
    return c.__dict__["__table__"].__dict__["name"]


@pytest.mark.parametrize("obj", objects)
@pytest.mark.parametrize("simple", [True, False])
@pytest.mark.parametrize("root_table", root_tables)
def test_import_json(obj, simple, root_table):
    importer = SQLThemAll(simple=simple, root_table=root_table)
    importer.import_json(obj)
    root_class = [
        c for c in importer.classes if tablename(c) == importer.root_table
    ][0]
    session = importer.sessionmaker()
    if session.query(root_class).all():
        dbobj = session.query(root_class).all()[0]
        for a in [i for i in dbobj.__dir__() if i.endswith("_collection")]:
            dbobj.__getattribute__(a)
        assert compare_obj(dbobj2obj(dbobj), obj)


@pytest.mark.parametrize("array", arrays)
@pytest.mark.parametrize("simple", [True, False])
@pytest.mark.parametrize("root_table", root_tables)
def test_import_multi_json(array, simple, root_table):

    importer = SQLThemAll(simple=simple, root_table=root_table)
    importer.import_multi_json(array)
    root_class = [
        c for c in importer.classes if tablename(c) == importer.root_table
    ][0]
    session = importer.sessionmaker()
    dbobjs = session.query(root_class).all()
    for dbobj, obj in zip(dbobjs, array):
        for a in [i for i in dbobj.__dir__() if i.endswith("_collection")]:
            dbobj.__getattribute__(a)
        assert compare_obj(dbobj2obj(dbobj), obj)
