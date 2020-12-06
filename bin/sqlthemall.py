#!/usr/bin/env python3

import argparse
import sys

import orjson
from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker


class SQLThemAll:

    dburl = "sqlite://"
    output_mode = "quiet"
    simple = False
    autocommit = False
    metadata = MetaData()

    def __init__(
        self,
        dburl="sqlite://",
        quiet=True,
        verbose=False,
        simple=False,
        autocommit=False,
        root_table="main",
    ):
        self.dburl = dburl
        self.quiet = quiet
        self.verbose = verbose
        self.simple = simple
        self.autocommit = autocommit
        self.root_table = str(root_table)

        self.engine = create_engine(self.dburl)
        self.metadata.reflect(self.engine)

    def create_many_to_one(self, k, current_table):
        (self.quiet or print("creating table " + k))
        t = Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
        )
        return t

    def create_many_to_many(self, k, current_table):
        (self.quiet or print("creating table " + k))
        t = Table(k, self.metadata, Column("_id", Integer, primary_key=True))
        (self.quiet or print("creating bridge " + current_table.name + " - " + k))
        bridge = Table(
            "bridge_" + current_table.name + "_" + k,
            self.metadata,
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
            Column(k + "_id", ForeignKey(k + "._id")),
        )
        return t

    def create_one_to_one(self, k, current_table):
        (self.quiet or print("creating table " + k))
        t = Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
        )
        return t

    def create_one_to_many(self, k, current_table):
        (self.quiet or print("creating table " + k))
        t = Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
        )
        return t

    def create_schema(self, jsonobj, root_table=None):
        if root_table == None:
            root_table = self.root_table

        if not root_table in self.metadata.tables:
            current_table = Table(
                root_table, self.metadata, Column("_id", Integer, primary_key=True)
            )
        else:
            current_table = self.metadata.tables[root_table]

        def parse_dict(obj, current_table=current_table):

            if obj.__class__ == dict:
                for k, v in obj.items():
                    if k in (c.name for c in current_table.columns):
                        (
                            self.verbose
                            and print(
                                k + " already exists in table " + current_table.name
                            )
                        )
                        continue
                    else:
                        if v.__class__ == str:
                            current_table.append_column(Column(k, String()))
                            (
                                self.quiet
                                or print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                            )
                        elif v.__class__ == int:
                            current_table.append_column(Column(k, Integer()))
                            (
                                self.quiet
                                or print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                            )
                        elif v.__class__ == float:
                            current_table.append_column(Column(k, Float()))
                            (
                                self.quiet
                                or print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                            )
                        elif v.__class__ == dict:
                            if k not in self.metadata.tables:
                                if not self.simple:
                                    t = self.create_many_to_one(
                                        k=k, current_table=current_table
                                    )
                                else:
                                    t = self.create_one_to_one(
                                        k=k, current_table=current_table
                                    )
                            else:
                                t = self.metadata.tables[k]
                            parse_dict(obj=v, current_table=t)

                        elif v.__class__ == list:
                            if v:
                                v = [
                                    {"value": item} if item.__class__ != dict else item
                                    for item in v
                                ]
                                for item in v:
                                    if k not in self.metadata.tables:
                                        if not self.simple:
                                            t = self.create_many_to_many(
                                                k, current_table=current_table
                                            )
                                        else:
                                            t = self.create_one_to_many(
                                                k=k, current_table=current_table
                                            )
                                    else:
                                        t = self.metadata.tables[k]
                                    parse_dict(obj=item, current_table=t)

        parse_dict(jsonobj)

        Base = automap_base(metadata=self.metadata)
        Base.prepare(self.engine)

        con = self.engine.connect()
        Base.metadata.create_all(bind=con)

    def importDataToSchema(self, jsonobj):
        self.engine.connect()

        Base = automap_base()
        Base.prepare(self.engine, reflect=True)
        classes = Base.classes

        Session = sessionmaker(self.engine, autocommit=self.autocommit)
        self.session = Session()

        def make_relational_obj(name, objc):
            pre_ormobjc = dict()
            collectiondict = dict()
            for k, v in objc.items():
                if v.__class__ == dict:
                    collectiondict[k] = [make_relational_obj(k, v)]
                elif v.__class__ == list:
                    if v:
                        v = [i if i.__class__ == dict else {"value": i} for i in v]
                        collectiondict[k] = [make_relational_obj(k, i) for i in v]
                elif v == None:
                    continue
                else:
                    pre_ormobjc[k] = v
            if not self.quiet:
                if self.verbose:
                    print(pre_ormobjc)
                else:
                    sys.stdout.write(".")
                    sys.stdout.flush()
            if not self.simple:
                try:
                    in_session = (
                        self.session.query(classes[name]).filter_by(**pre_ormobjc).all()
                    )
                except:
                    in_session = False
            else:
                in_session = False
            if in_session:
                ormobjc = in_session[0]
                if collectiondict:
                    for k in collectiondict:
                        setattr(ormobjc, k.lower() + "_collection", collectiondict[k])
            else:
                try:
                    ormobjc = classes[name](**pre_ormobjc)
                except:
                    ormobjc = classes[name]()
                    for k, v in pre_ormobjc.items():
                        ormobjc.__setattr__(k, v)

                self.session.add(ormobjc)
                if collectiondict:
                    for k in collectiondict:
                        setattr(ormobjc, k.lower() + "_collection", collectiondict[k])
            return ormobjc

        o = make_relational_obj(name=self.root_table, objc=jsonobj)
        for c in self.metadata.tables:
            if not c.startswith("bridge"):
                instances = self.session.query(classes[c]).all()

        if not self.quiet:
            sys.stdout.write("\n")
        if not self.autocommit:
            self.session.commit()

        self.session.close()


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
        "-t",
        "--root-table",
        nargs=1,
        dest="root_table",
        help="Name of the table to import the outermost subobjects if JSON object is a array",
    )
    parser.add_argument(
        "-l", "--line", action="store_true", help="Uses JSONline instead of JSON"
    )

    args = parser.parse_args(sys.argv[1:])

    if not args.root_table:
        args.root_table = ["main"]

    importer = SQLThemAll(
        dburl=args.dburl[0],
        quiet=args.quiet,
        verbose=args.verbose,
        autocommit=args.autocommit,
        simple=args.simple,
        root_table=args.root_table[0],
    )

    if args.url and not args.line:
        res = requests.get(args.url[0])
        obj = res.json()
    elif args.url and args.line:
        res = requests.get(args.url[0])
        objs = [orjson.loads(l) for l in response.text.splitlines()]

    elif args.file and not args.line:
        with open(args.file[0]) as f:
            obj = orjson.loads(f.read())
    elif args.file and args.line:
        with open(args.file[0]) as f:
            objs = [orjson.loads(l) for l in f.readlines()]

    elif not args.line:
        obj = orjson.loads(sys.stdin.read())
    else:
        objs = (orjson.loads(l) for l in sys.stdin.readlines())

    if not args.line and obj.__class__ == list:
        obj = {args.root_table[0]: obj}

    if not args.line:
        importer.create_schema(jsonobj=obj)
    else:
        for obj in objs:
            importer.create_schema(jsonobj=obj)
            if not args.noimport:
                (args.quiet or print("\nImporting Objects"))
                importer.importDataToSchema(jsonobj=obj)

    if not args.noimport:
        (args.quiet or print("\nImporting Objects"))
        importer.importDataToSchema(jsonobj=obj)
