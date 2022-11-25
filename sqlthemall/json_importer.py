#!/usr/bin/env python3
"""
This module contains the main importer class `SQLThemAll`.
"""

import datetime
import logging
import sys
import traceback
from collections.abc import Iterable
from typing import Optional, Set

import logging
import alembic
from sqlalchemy import (Boolean, Column, Date, Float, ForeignKey, Integer,
                        MetaData, String, Table, create_engine)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

# create logger
logger = logging.getLogger('json_importer')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logger.info)

# create formatter
formatter = logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s')

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
        logger.info("Creating table %s", k)
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
        logger.info("Creating table %s", k)
        logger.info("Creating bridge %s - %s", current_table.name, k)
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
        return Table(
            k, self.metadata, Column("_id", Integer, primary_key=True)
        )

    def create_one_to_one(self, k: str, current_table: Table) -> Table:
        """
        Adds a one to one relationship to the schema.

        Parameters:
            name (str): New table name.
            current_table (Table): Current Table.
        Returns:
            Newly created Table schema.
        """
        logger.info("Creating table %s", k)
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
        logger.info("Creating table %s", k)
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
            self.metadata = MetaData(bind=self.engine)
            self.metadata.reflect(
                self.engine, extend_existing=True, autoload_replace=True
            )
            self.base = automap_base(metadata=self.metadata)
            self.base.prepare(self.engine, reflect=True)
            self.classes = self.base.classes
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
            exclude_props: Set = set(),
        ) -> None:
            """
            Creates table_schema from the structure of a given JSON object.

            Parameters:
                obj (dict): Object to parse.
                current_table (Table) : The current_table.
                simple (bool): Create a simple database schema.
                exclude_props(Set): Column names to ignore
            """
            if current_table.name in self.base.classes:
                cls = self.base.classes[current_table.name]
                props = set(cls.__dict__.keys())
            else:
                props = set()
            logger.debug('Forbinden col names: %s', props)

            if obj.__class__ == dict:
                if "_id" in obj:
                    obj["id"] = obj.pop("_id")
                for k, val in obj.items():
                    k = k.lower()
                    if k == '':
                        continue
                    if k in (c.name for c in current_table.columns):
                        has_vals = lambda v: v.__class__ == dict and v and True or v.__class__ == list and any(v) or False
                        if val.__class__ in {dict, list} and has_vals(val):
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
                            if val.__class__ == dict:
                                parse_dict(obj=val, current_table=tbl, exclude_props=set(current_table.name))
                            else:
                                for i in val:
                                    if i.__class__ == dict and i:
                                        parse_dict(obj=i, current_table=tbl, exclude_props=set(current_table.name))
                                    else:
                                        parse_dict(obj={"value": i}, current_table=tbl, exclude_props=set(current_table.name))
                        else:
                            logger.debug('%s already exists in table %s', k, current_table.name)
                            continue
                    else:
                        if k in props:
                            logger.info('Excluded Prop: %s', k)
                            continue
                        self.schema_changed = True
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
                            parse_dict(obj=val, current_table=tbl, exclude_props=set(current_table.name))

                        elif val.__class__ == list:
                            if val:
                                if not [i for i in val if i]:
                                    continue
                                val = [
                                    item
                                    if item.__class__ == dict
                                    else {"value": item}
                                    for item in val
                                ]
                                val = [i for i in val if i]
                                for item in val:
                                    if k not in self.metadata.tables:
                                        self.schema_changed = True
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
                                    parse_dict(obj=item, current_table=tbl, exclude_props=set(current_table.name))

        if jsonobj.__class__ == list:
            jsonobj = {self.root_table: jsonobj}
        parse_dict(obj=jsonobj)

        if self.schema_changed:
            self.metadata.create_all(self.engine)
            self.metadata.reflect(
                self.engine, extend_existing=True, autoload_replace=True
            )
            self.base = automap_base(metadata=self.metadata)
            self.base.prepare(self.engine, reflect=True)
            self.classes = self.base.classes
        self.schema_changed = False

    def insert_data_to_schema(self, jsonobj: dict) -> None:
        """
        Inserts the given JSON object into the database creating
        the schema if not availible.

        Parameters:
            jsonobj (dict): Object to parse.
        """

        def make_relational_obj(name, objc, session: Session):
            """
            Generates a relational object which is insertable from
            a given JSON object.

            Parameters:
                name (str): Name of the table that will represent the object.
                objc (dict): Object to parse.
                session (Session): Session to use.
            """
            name = name.lower()
            pre_ormobjc, collectiondict = {}, {}
            if "_id" in objc:
                objc["id"] = objc.pop("_id")
            for k, val in objc.items():
                k = k.lower()
                #if k not in {c.name for c in self.metadata.tables[name].columns}:
                #    logger.info('Cols: %s', {c.name for c in self.metadata.tables[name].columns})
                #    continue
                if val.__class__ in {dict, list}:
                    if val.__class__ == dict:
                        _collection = [i for i in [make_relational_obj(k, val,
                            session=session)] if i]
                        if _collection:
                            collectiondict[k] = _collection
                    elif val.__class__ == list:
                        if val:
                            val = [
                                i
                                if i.__class__ == dict and i or i is None
                                else {"value": i}
                                for i in val
                            ]
                            _collection = [
                                j
                                for j in
                                [make_relational_obj(k, i, session=session) for i in val]
                                if j and j != {"value": None}
                            ]
                            if not _collection:
                                continue
                            collectiondict[k] = _collection
                elif val is None:
                    continue
                else:
                    pre_ormobjc[k] = val
            if not pre_ormobjc:
                return None
            if not self.quiet:
                sys.stdout.write(".")
                sys.stdout.flush()
            logger.debug('%s', pre_ormobjc)
            if not self.simple:
                in_session = session.query(self.base.classes[name]).filter_by(**pre_ormobjc).first()
            else:
                in_session = False

            if in_session:
                ormobjc = in_session
                if collectiondict:
                    for k, val in collectiondict.items():
                        if val:
                            setattr(
                                ormobjc,
                                k.lower() + "_collection",
                                val,
                            )
            else:
                ormobjc = self.base.classes[name](**pre_ormobjc)
                #ormobjc = self.base.classes[name]()
                #for k, val in pre_ormobjc.items():
                #    if k in ormobjc.__dict__:
                #        ormobjc.__setattr__(k, val)
                if True:

                    if collectiondict:
                        for k, val in collectiondict.items():
                            setattr(
                                ormobjc,
                                k.lower() + "_collection",
                                val,
                            )

                if ormobjc:
                    session.add(ormobjc)
                    logger.debug('Adding %s to session', name)
                else:
                    return None

            return ormobjc

        if jsonobj.__class__ == list:
            jsonobj = {self.root_table: jsonobj}

        with Session(self.engine) as session:
            make_relational_obj(name=self.root_table, objc=jsonobj, session=session)
            if not self.quiet:
                sys.stdout.write("\n")
            try:
                session.commit()
            except Exception:
                traceback.print_exc()
                session.rollback()

    def import_json(self, jsonobj: dict) -> None:
        """
        Inserts the given JSON object into the database creating
        the schema if not availible.

        Parameters:
            jsonobj (dict): Object to parse.
        """
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()
            self.metadata = MetaData(bind=self.engine)
            self.metadata.reflect(
                self.engine, extend_existing=True, autoload_replace=True
            )
            self.base = automap_base(metadata=self.metadata)
            self.base.prepare(self.engine, reflect=True)
            self.classes = self.base.classes

        self.create_schema(jsonobj)
        self.insert_data_to_schema(jsonobj)

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

