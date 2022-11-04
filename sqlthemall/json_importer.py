#!/usr/bin/env python3
"""
This module contains the main importer class `SQLThemAll`.
"""

import datetime
import sys
import traceback
from collections.abc import Iterable
from typing import Optional

import logging
import alembic
from sqlalchemy import (Boolean, Column, Date, Float, ForeignKey, Integer,
                        MetaData, String, Table, create_engine)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

# create logger
logger = logging.getLogger('json_exporter')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
#ch.setLevel(logging.DEBUG)
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

class SQLThemAll:

    """
    Class that provides the creationion of relational Database schemata
    from the structure of JSON as well as the import of the provided
    JSON data.

    Attributes:
        dburi (str): Database URI.
        loglevel (int): Set log level.
        root_table (str): The name of the table to import the JSON root.
        simple (bool): Create a simplified database schema.
        autocommit (bool): Open the database in autocommit mode.
        echo (bool): Echo the executed SQL statements.
    """

    connection: Optional[Engine] = None
    schema_changed: bool = False
    session = None

    def __init__(
        self,
        dburl: str = "sqlite://",
        quiet: bool = True,
        verbose: bool = False,
        simple: bool = False,
        autocommit: bool = False,
        root_table="main",
        echo: bool = False,
    ) -> None:
        """
        The contructor for SQLThemAll class.

        Parameters:
            dburi (str): Database URI.
            loglevel (int): Set log level.
            root_table (str): The name of the table to import the JSON root.
            simple (bool): Create a simplified database schema.
            autocommit (bool): Open the database in autocommit mode.
            echo (bool): Echo the executed SQL statements.
        """
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
        self.metadata.reflect(
            self.engine, extend_existing=True, autoload_replace=True
        )
        self.sessionmaker = sessionmaker(
            self.engine, autocommit=self.autocommit
        )
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare(self.engine, reflect=True)
        self.classes = self.base.classes

    def create_many_to_one(self, k: str, current_table: Table) -> Table:
        """
        Adds a many to one relationship to the schema.

        Parameters:
            name (str): New table name.
            current_table (Table): Current Table.
        Returns:
            Newly created Table schema.
        """
        logger.info('creating table %s', k)
        return Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            extend_existing=True,
        )

    def create_many_to_many(self, k: str, current_table: Table) -> Table:
        """
        Adds a many to many relationship to the schema.

        Parameters:
            name (str): New table name.
            current_table (Table): Current Table.
        Returns:
            Newly created Table schema.
        """
        logger.info('creating table %s', k)
        tbl = Table(k, self.metadata, Column("_id", Integer, primary_key=True))
        logger.info('creating bridge %s - %s', current_table.name, k)
        Table(
            "bridge_" + current_table.name + "_" + k,
            self.metadata,
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            Column(k + "_id", ForeignKey(k + "._id")),
            extend_existing=True,
        )
        return tbl

    def create_one_to_one(self, k: str, current_table: Table) -> Table:
        """
        Adds a one to one relationship to the schema.

        Parameters:
            name (str): New table name.
            current_table (Table): Current Table.
        Returns:
            Newly created Table schema.
        """
        logger.info('creating table %s', k)
        return Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            extend_existing=True,
        )

    def create_one_to_many(self, k: str, current_table: Table) -> Table:
        """
        Adds a one to many relationship to the schema.

        Parameters:
            name (str): New table name.
            current_table (Table): Current Table.
        Returns:
            Newly created Table schema.
        """
        logger.info('creating table %s', k)
        return Table(
            k,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            extend_existing=True,
        )

    def create_schema(
        self, jsonobj: dict, root_table: str = "", simple: bool = False
    ) -> None:
        """
        Creates table_schema from the structure of a given JSON object.

        Parameters:
            jsonobj (dict): jsonobj.
            root_table (str): Table name of the JSON object root.
            simple (bool): Create a simple database schema.
        """
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()
        if not root_table:
            root_table = self.root_table
        if not simple:
            simple = self.simple

        self.schema_changed = False

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

        def parse_dict(
            obj: dict,
            current_table: Table = current_table,
            simple: bool = simple,
        ) -> None:
            """
            Creates table_schema from the structure of a given JSON object.

            Parameters:
                obj (dict): Object to parse.
                current_table (Table) : The current_table.
                simple (bool): Create a simple database schema.
            """

            if obj.__class__ == dict:
                if "_id" in obj:
                    obj["id"] = obj.pop("_id")
                for k, val in obj.items():
                    if k in (c.name for c in current_table.columns):
                        logger.debug('%s already exists in table %s', k, current_table.name)
                        continue
                    col_types = {
                        datetime.date: Date,
                        str: String,
                        bool: Boolean,
                        int: Integer,
                        float: Float,
                    }
                    if val.__class__ in col_types:
                        current_table.append_column(Column(k, col_types[val.__class__]()))
                        alembic.ddl.base.AddColumn(
                            current_table.name,
                            Column(k, col_types[val.__class__]()),
                        ).execute(self.engine)
                        logger.info('adding col %s to table %s', k, current_table.name)
                    elif val.__class__ == dict:
                        if k not in self.metadata.tables:
                            self.schema_changed = True
                            if not simple:
                                tbl = self.create_many_to_one(
                                    k=k, current_table=current_table
                                )
                                tbl.create(self.engine)
                            else:
                                tbl = self.create_one_to_one(
                                    k=k, current_table=current_table
                                )
                                tbl.create(self.engine)
                        else:
                            tbl = self.metadata.tables[k]
                        parse_dict(obj=val, current_table=tbl)

                    elif val.__class__ == list:
                        if val:
                            if not [i for i in val if i is not None]:
                                continue
                            val = [
                                item
                                if item.__class__ == dict
                                else {"value": item}
                                for item in val
                            ]
                            for item in val:
                                if k not in self.metadata.tables:
                                    if not simple:
                                        tbl = self.create_many_to_many(
                                            k=k,
                                            current_table=current_table,
                                        )
                                        tbl.create(self.engine)
                                    else:
                                        tbl = self.create_one_to_many(
                                            k=k,
                                            current_table=current_table,
                                        )
                                        tbl.create(self.engine)
                                else:
                                    tbl = self.metadata.tables[k]
                                parse_dict(obj=item, current_table=tbl)

        if jsonobj.__class__ == list:
            jsonobj = {self.root_table: jsonobj}
        parse_dict(jsonobj)

        if self.schema_changed:
            self.base = automap_base(metadata=self.metadata)
            self.base.prepare(self.engine)
            self.metadata.create_all(bind=self.connection)
            self.metadata = self.base.metadata
            self.classes = self.base.classes

    def insert_data_to_schema(self, jsonobj: dict) -> None:
        """
        Inserts the given JSON object into the database creating
        the schema if not availible.

        Parameters:
            jsonobj (dict): Object to parse.
        """

        if self.schema_changed:
            self.base = automap_base()
            self.base.prepare(self.engine, reflect=True)
            self.classes = self.base.classes

        self.session = self.sessionmaker()

        def make_relational_obj(name, objc):
            """
            Generates a relational object which is insertable from
            a given JSON object.

            Parameters:
                name (str): Name of the table that will represent the object.
                objc (dict): Object to parse.
            """
            pre_ormobjc, collectiondict = {}, {}
            for k, val in objc.items():
                if val.__class__ == dict:
                    if "_id" in objc:
                        objc["id"] = objc.pop("_id")
                    collectiondict[k] = [make_relational_obj(k, val)]
                elif val.__class__ == list:
                    if val:
                        if not [i for i in val if i is not None]:
                            continue
                        val = [
                            i if i.__class__ == dict else {"value": i}
                            for i in val
                        ]
                        collectiondict[k] = [
                            make_relational_obj(k, i) for i in val
                        ]
                elif val is None:
                    continue
                else:
                    pre_ormobjc[k] = val
            logger.debug('%s', str(pre_ormobjc))
            if not self.quiet:
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
                    traceback.print_exc()
                    in_session = False
            else:
                in_session = False
            if in_session:
                ormobjc = in_session[0]
                if collectiondict:
                    for k, val in collectiondict.items():
                        setattr(
                            ormobjc,
                            k.lower() + "_collection",
                            val,
                        )
            else:
                if pre_ormobjc:
                    try:
                        ormobjc = self.classes[name](**pre_ormobjc)
                    except BaseException:
                        traceback.print_exc()
                        ormobjc = self.classes[name]()
                        for k, val in pre_ormobjc.items():
                            ormobjc.__setattr__(k, val)

                    self.session.add(ormobjc)
                    if collectiondict:
                        for k, val in collectiondict.items():
                            setattr(
                                ormobjc,
                                k.lower() + "_collection",
                                val,
                            )
                else:
                    return None

            return ormobjc

        if jsonobj.__class__ == list:
            jsonobj = {self.root_table: jsonobj}

        make_relational_obj(name=self.root_table, objc=jsonobj)
        if not self.quiet:
            sys.stdout.write("\n")
        if not self.autocommit:
            self.session.commit()

    def import_json(self, jsonobj: dict) -> None:
        """
        Inserts the given JSON object into the database creating
        the schema if not availible.

        Parameters:
            jsonobj (dict): Object to parse.
        """
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()

        self.create_schema(jsonobj)
        self.insert_data_to_schema(jsonobj)

        self.connection.close()

    def import_multi_json(self, jsonobjs: Iterable) -> None:
        """
        Inserts Array of JSON objects into the database creating
        the schema if not availible.

        Parameters:
            jsonobjs (dict): Object to parse.
        """
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()

        jsonobj = {self.root_table: jsonobjs}
        self.create_schema(jsonobj)
        self.insert_data_to_schema(jsonobj)

        self.connection.close()
