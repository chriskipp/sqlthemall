#!/usr/bin/env python3
"""This module contains the main importer class `SQLThemAll`."""

import datetime
import logging
import sys
import traceback
from collections.abc import Iterable
from typing import Optional, TypeVar

import alembic
from sqlalchemy import (Boolean, Column, Date, Float, ForeignKey, Integer,
                        MetaData, String, Table, create_engine)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

tuple_or_set = TypeVar("tuple_or_set", tuple, set)


def create_logger(name: str, loglevel: str = "INFO") -> logging.Logger:
    """
    Initialises the default logger with given loglevel.

    Args:
        name (str): Name of the logger.
        loglevel (str): Log level to use (default "INFO").

    Returns:
        logger: The initialized logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(loglevel)

    ch = logging.StreamHandler()
    ch.setLevel(loglevel)

    formatter = logging.Formatter(
        "%(asctime)s - [%(name)s] - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger


class SQLThemAll:
    """
    Class that provides the creationion of relational Database schemata.

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
    loglevel: str = "INFO"
    progress: bool = True

    def __init__(
        self,
        dburl: str = "sqlite://",
        progress: bool = True,
        loglevel: str = "INFO",
        simple: bool = False,
        autocommit: bool = False,
        root_table="main",
        echo: bool = False,
    ) -> None:
        """
        The contructor for SQLThemAll class.

        Args:
            dburl (str): Database URI.
            progress (bool): Show import progress.
            loglevel (str): Set loglevel - one of "ERROR", "WARNING", "INFO", "DEBUG"(default "INFO")
            simple (bool): Create a simplified database schema.
            autocommit (bool): Open the database in autocommit mode.
            root_table (str): The name of the table to import the JSON root.
            echo (bool): Echo the executed SQL statements.
        """
        self.dburl = dburl
        self.progress = progress
        self.loglevel = loglevel
        self._logger = create_logger("sqlthemall", self.loglevel)
        self.echo = echo
        if self.echo:
            self.progress = False
        self.simple = simple
        self.autocommit = autocommit
        self.root_table = str(root_table).lower()

        self.engine = create_engine(self.dburl, echo=self.echo)
        self.metadata = MetaData()
        self.metadata.reflect(
            self.engine, extend_existing=True, autoload_replace=True
        )
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare(self.engine, reflect=True)
        self.classes = self.base.classes

    def create_many_to_one(self, name: str, current_table: Table) -> Table:
        """
        Adds a many to one relationship to the schema.

        Args:
            name (str): New table name.
            current_table (Table): Current Table.

        Returns:
            Table: Newly created Table.
        """
        self._logger.info("Creating table %s", name)
        return Table(
            name,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            extend_existing=True,
        )

    def create_many_to_many(self, name: str, current_table: Table) -> Table:
        """
        Adds a many to many relationship to the schema.

        Args:
            name (str): New table name.
            current_table (Table): Current Table.

        Returns:
            Table: Newly created Table.
        """
        self._logger.info("Creating table %s", name)
        self._logger.info("Creating bridge %s - %s", current_table.name, name)
        Table(
            "bridge_" + current_table.name + "_" + name,
            self.metadata,
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            Column(name + "_id", ForeignKey(name + "._id")),
            extend_existing=True,
        )
        return Table(
            name, self.metadata, Column("_id", Integer, primary_key=True)
        )

    def create_one_to_one(self, name: str, current_table: Table) -> Table:
        """
        Adds a one to one relationship to the schema.

        Args:
            name (str): New table name.
            current_table (Table): Current Table.

        Returns:
            Table: Newly created Table.
        """
        self._logger.info("Creating table %s", name)
        return Table(
            name,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            extend_existing=True,
        )

    def create_one_to_many(self, name: str, current_table: Table) -> Table:
        """
        Adds a one to many relationship to the schema.

        Args:
            name (str): New table name.
            current_table (Table): Current Table.

        Returns:
            Table: Newly created Table.
        """
        self._logger.info("Creating table %s", name)
        return Table(
            name,
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

        Args:
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
            exclude_props: set = None,
        ) -> None:
            """
            Creates table_schema from the structure of a given JSON object.

            Args:
                obj (dict): Object to parse.
                current_table (Table) : The current_table.
                simple (bool): Create a simple database schema.
                exclude_props : Column names to ignore
                #exclude_props (set): Column names to ignore
            """
            if current_table.name in self.base.classes:
                cls = self.base.classes[current_table.name]
                props = set(cls.__dict__.keys())
            else:
                props = set()
            self._logger.debug("Forbinden col names: %s", props)

            if obj.__class__ == dict:
                if "_id" in obj:
                    obj["id"] = obj.pop("_id")
                for k, val in obj.items():
                    k = k.lower()
                    if k == "":
                        continue
                    if k in (c.name for c in current_table.columns):
                        has_vals = (
                            lambda v: v.__class__ == dict
                            and v
                            and True
                            or v.__class__ == list
                            and any(v)
                            or False
                        )
                        if val.__class__ in {dict, list} and has_vals(val):
                            if k not in self.metadata.tables:
                                self.schema_changed = True
                                if not simple:
                                    tbl = self.create_many_to_one(
                                        name=k, current_table=current_table
                                    )
                                    tbl.create(self.engine)
                                else:
                                    tbl = self.create_one_to_one(
                                        name=k, current_table=current_table
                                    )
                                    tbl.create(self.engine)
                            else:
                                tbl = self.metadata.tables[k]
                            if val.__class__ == dict:
                                parse_dict(
                                    obj=val,
                                    current_table=tbl,
                                    exclude_props=set(current_table.name),
                                )
                            else:
                                for i in val:
                                    if i.__class__ == dict and i:
                                        parse_dict(
                                            obj=i,
                                            current_table=tbl,
                                            exclude_props=set(
                                                current_table.name
                                            ),
                                        )
                                    else:
                                        parse_dict(
                                            obj={"value": i},
                                            current_table=tbl,
                                            exclude_props=set(
                                                current_table.name
                                            ),
                                        )
                        else:
                            self._logger.debug(
                                "%s already exists in table %s",
                                k,
                                current_table.name,
                            )
                            continue
                    else:
                        if k in props:
                            self._logger.info("Excluded Prop: %s", k)
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
                            current_table.append_column(
                                Column(k, col_types[val.__class__]())
                            )
                            alembic.ddl.base.AddColumn(
                                current_table.name,
                                Column(k, col_types[val.__class__]()),
                            ).execute(self.engine)
                            self._logger.info(
                                "adding col %s to table %s",
                                k,
                                current_table.name,
                            )
                        elif val.__class__ == dict:
                            if k not in self.metadata.tables:
                                if not simple:
                                    tbl = self.create_many_to_one(
                                        name=k, current_table=current_table
                                    )
                                    tbl.create(self.engine)
                                else:
                                    tbl = self.create_one_to_one(
                                        name=k, current_table=current_table
                                    )
                                    tbl.create(self.engine)
                            else:
                                tbl = self.metadata.tables[k]
                            parse_dict(
                                obj=val,
                                current_table=tbl,
                                exclude_props=set(current_table.name),
                            )

                        elif val.__class__ == list:
                            if val:
                                if not [i for i in val if i]:
                                    continue
                                val = [
                                    item.__class__ == dict
                                    and item
                                    or {"value": item}
                                    for item in val
                                ]
                                val = [i for i in val if i]
                                for item in val:
                                    if k not in self.metadata.tables:
                                        self.schema_changed = True
                                        if not simple:
                                            tbl = self.create_many_to_many(
                                                name=k,
                                                current_table=current_table,
                                            )
                                            tbl.create(self.engine)
                                        else:
                                            tbl = self.create_one_to_many(
                                                name=k,
                                                current_table=current_table,
                                            )
                                            tbl.create(self.engine)
                                    else:
                                        tbl = self.metadata.tables[k]
                                    parse_dict(
                                        obj=item,
                                        current_table=tbl,
                                        exclude_props=set(current_table.name),
                                    )

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
        Inserts the given JSON object into the database creating.

        the schema if not availible.

        Args:
            jsonobj (dict): Object to parse.
        """

        def make_relational_obj(name, objc, session: Session):
            """
            Generates a relational object which is insertable from.

            a given JSON object.

            Args:
                name (str): Name of the table that will represent the object.
                objc (dict): Object to parse.
                session (Session): Session to use.

            Returns:
                ormobject: Object defined by the object relational model.
            """
            name = name.lower()
            pre_ormobjc, collectiondict = {}, {}
            if objc.__class__ != dict:
                return None
            if "_id" in objc:
                objc["id"] = objc.pop("_id")
            for k, val in objc.items():
                k = k.lower()
                if val.__class__ in {dict, list}:
                    if val.__class__ == dict:
                        _collection = [
                            i
                            for i in [
                                make_relational_obj(k, val, session=session)
                            ]
                            if i
                        ]
                        if _collection:
                            collectiondict[k] = _collection
                    elif val.__class__ == list:
                        if val:
                            val = [
                                (i.__class__ == dict or i is None)
                                and i
                                or {"value": i}
                                for i in val
                            ]
                            _collection = [
                                j
                                for j in [
                                    make_relational_obj(k, i, session=session)
                                    for i in val
                                ]
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
            if self.progress:
                sys.stdout.write(".")
                sys.stdout.flush()
            self._logger.debug("%s", pre_ormobjc)
            if not self.simple:
                query = session.query(self.base.classes[name])
                in_session = query.filter_by(**pre_ormobjc).first()
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

                if collectiondict:
                    for k, val in collectiondict.items():
                        setattr(
                            ormobjc,
                            k.lower() + "_collection",
                            val,
                        )

                if ormobjc:
                    session.add(ormobjc)
                    self._logger.debug("Adding %s to session", name)
                else:
                    return None

            return ormobjc

        if jsonobj.__class__ == list:
            jsonobj = {self.root_table: jsonobj}

        with Session(self.engine) as session:
            make_relational_obj(
                name=self.root_table, objc=jsonobj, session=session
            )
            if self.progress:
                sys.stdout.write("\n")
            try:
                session.commit()
            except Exception:
                traceback.print_exc()
                session.rollback()

    def import_json(self, jsonobj: dict) -> None:
        """
        Inserts the given JSON object into the database creating.

        the schema if not availible.

        Args:
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
        Inserts Array of JSON objects into the database creating.

        the schema if not availible.

        Args:
            jsonobjs (Iterable): Object to parse.
        """
        if not self.connection or self.connection.closed:
            self.connection = self.engine.connect()

        jsonobj = {self.root_table: jsonobjs}
        self.create_schema(jsonobj)
        self.insert_data_to_schema(jsonobj)
