#!/usr/bin/env python3

import datetime
import sys

import alembic
from sqlalchemy import (Boolean, Column, Date, Float, ForeignKey, Integer,
                        MetaData, String, Table, create_engine)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker


class SQLThemAll:

    connection = False

    def __init__(
        self,
        dburl="sqlite://",
        quiet=True,
        verbose=False,
        simple=False,
        autocommit=False,
        root_table="main",
        echo=False,
    ):
        self.dburl = dburl
        self.quiet = quiet
        self.verbose = verbose
        self.echo = echo
        if self.echo:
            self.quiet = True
        self.simple = simple
        self.autocommit = autocommit
        self.root_table = str(root_table)

        self.engine = create_engine(self.dburl, echo=self.echo)
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect(self.engine, extend_existing=True, autoload_replace=True)
        self.sessionmaker = sessionmaker(self.engine, autocommit=self.autocommit)
        self.Base = automap_base(metadata=self.metadata)
        self.Base.prepare(self.engine, reflect=True)
        self.classes = self.Base.classes

    def create_many_to_one(self, k, current_table):
        if not self.quiet:
            print("creating table " + k)
        return Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
            extend_existing=True,
        )

    def create_many_to_many(self, k, current_table):
        if not self.quiet:
            print("creating table " + k)
        t = Table(k, self.metadata, Column("_id", Integer, primary_key=True))
        if not self.quiet:
            print("creating bridge " + current_table.name + " - " + k)
        bridge = Table(
            "bridge_" + current_table.name + "_" + k,
            self.metadata,
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
            Column(k + "_id", ForeignKey(k + "._id")),
            extend_existing=True,
        )
        return t

    def create_one_to_one(self, k, current_table):
        if not self.quiet:
            print("creating table " + k)
        return Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
            extend_existing=True,
        )

    def create_one_to_many(self, k, current_table):
        if not self.quiet:
            print("creating table " + k)
        return Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(current_table.name + "_id", ForeignKey(current_table.name + "._id")),
            extend_existing=True,
        )

    def create_schema(self, jsonobj, root_table=None, simple=None):
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()

        self.schema_changed = False
        if root_table is None:
            root_table = self.root_table
        if simple is None:
            simple = self.simple

        if root_table not in self.metadata.tables:
            self.schema_changed = True
            current_table = Table(
                root_table,
                self.metadata,
                Column("_id", Integer, primary_key=True),
                extend_existing=True,
            )
            current_table.create(self.engine)
        else:
            current_table = self.metadata.tables[root_table]

        def parse_dict(obj, current_table=current_table, simple=simple):

            if obj.__class__ == dict:
                if obj.__contains__("_id"):
                    obj["id"] = obj.pop("_id")
                for k, v in obj.items():
                    if k in (c.name for c in current_table.columns):
                        if self.verbose:
                            print(k + " already exists in table " + current_table.name)
                        continue
                    else:
                        if v.__class__ == datetime.date:
                            self.schema_changed = True
                            current_table.append_column(Column(k, Date()))
                            alembic.ddl.base.AddColumn(
                                current_table.name,
                                Column(k, Date()),
                            ).execute(self.engine)
                            if not self.quiet:
                                print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                        if v.__class__ == str:
                            self.schema_changed = True
                            current_table.append_column(Column(k, String()))
                            alembic.ddl.base.AddColumn(
                                current_table.name,
                                Column(k, String()),
                            ).execute(self.engine)
                            if not self.quiet:
                                print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                        elif v.__class__ == int:
                            self.schema_changed = True
                            current_table.append_column(Column(k, Integer()))
                            alembic.ddl.base.AddColumn(
                                current_table.name,
                                Column(k, Integer()),
                            ).execute(self.engine)
                            if not self.quiet:
                                print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                        elif v.__class__ == float:
                            self.schema_changed = True
                            current_table.append_column(Column(k, Float()))
                            alembic.ddl.base.AddColumn(
                                current_table.name,
                                Column(k, Float()),
                            ).execute(self.engine)
                            if not self.quiet:
                                print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                        elif v.__class__ == bool:
                            self.schema_changed = True
                            current_table.append_column(Column(k, Boolean()))
                            alembic.ddl.base.AddColumn(
                                current_table.name,
                                Column(k, Boolean()),
                            ).execute(self.engine)
                            if not self.quiet:
                                print(
                                    "  adding col "
                                    + k
                                    + " to table "
                                    + current_table.name
                                )
                        elif v.__class__ == dict:
                            if k not in self.metadata.tables:
                                self.schema_changed = True
                                if not simple:
                                    t = self.create_many_to_one(
                                        k=k, current_table=current_table
                                    )
                                    t.create(self.engine)
                                else:
                                    t = self.create_one_to_one(
                                        k=k, current_table=current_table
                                    )
                                    t.create(self.engine)
                            else:
                                t = self.metadata.tables[k]
                            parse_dict(obj=v, current_table=t)

                        elif v.__class__ == list:
                            if v:
                                if not [i for i in v if i is not None]:
                                    continue
                                v = [
                                    item if item.__class__ == dict else {"value": item}
                                    for item in v
                                ]
                                for item in v:
                                    if k not in self.metadata.tables:
                                        if not simple:
                                            t = self.create_many_to_many(
                                                k=k, current_table=current_table
                                            )
                                            t.create(self.engine)
                                        else:
                                            t = self.create_one_to_many(
                                                k=k, current_table=current_table
                                            )
                                            t.create(self.engine)
                                    else:
                                        t = self.metadata.tables[k]
                                    parse_dict(obj=item, current_table=t)

        if jsonobj.__class__ == list:
            jsonobj = {self.root_table: jsonobj}
        parse_dict(jsonobj)

        if self.schema_changed:
            self.Base = automap_base(metadata=self.metadata)
            # self.Base.prepare(self.engine, reflect=True)
            self.Base.prepare(self.engine)
            # self.Base.metadata.create_all(bind=self.connection, checkfirst=False)
            self.metadata.create_all(bind=self.connection)
            self.metadata = self.Base.metadata
            self.classes = self.Base.classes
        # print(self.metadata.sorted_tables)

    def insertDataToSchema(self, jsonobj):

        if self.schema_changed:
            self.Base = automap_base()
            self.Base.prepare(self.engine, reflect=True)
            self.classes = self.Base.classes

        self.session = self.sessionmaker()

        def make_relational_obj(name, objc):
            pre_ormobjc, collectiondict = {}, {}
            for k, v in objc.items():
                if v.__class__ == dict:
                    if objc.__contains__("_id"):
                        objc["id"] = objc.pop("_id")
                    collectiondict[k] = [make_relational_obj(k, v)]
                elif v.__class__ == list:
                    if v:
                        if not [i for i in v if i is not None]:
                            continue
                        v = [i if i.__class__ == dict else {"value": i} for i in v]
                        collectiondict[k] = [make_relational_obj(k, i) for i in v]
                elif v is None:
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
                        self.session.query(self.classes[name])
                        .filter_by(**pre_ormobjc)
                        .all()
                    )
                except BaseException:
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
                    ormobjc = self.classes[name](**pre_ormobjc)
                except BaseException:
                    ormobjc = self.classes[name]()
                    for k, v in pre_ormobjc.items():
                        ormobjc.__setattr__(k, v)

                self.session.add(ormobjc)
                if collectiondict:
                    for k in collectiondict:
                        setattr(ormobjc, k.lower() + "_collection", collectiondict[k])
            return ormobjc

        if jsonobj.__class__ == list:
            jsonobj = {self.root_table: jsonobj}

        make_relational_obj(name=self.root_table, objc=jsonobj)
        if not self.quiet:
            sys.stdout.write("\n")
        if not self.autocommit:
            self.session.commit()

    def importJSON(self, jsonobj):
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()

        self.create_schema(jsonobj)
        self.insertDataToSchema(jsonobj)

        self.connection.close()

    def importMultiJSON(self, jsonobjs):
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()

        jsonobj = {self.root_table: jsonobjs}
        self.create_schema(jsonobj)
        self.insertDataToSchema(jsonobj)

        self.connection.close()
