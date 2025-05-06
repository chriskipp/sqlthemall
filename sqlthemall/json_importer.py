#!/usr/bin/env python3
"""This module contains the main importer class `SQLThemAll`."""

import datetime
import logging
import sys
import traceback
from collections.abc import Iterable
from typing import Union, Optional

from alembic.autogenerate import produce_migrations
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.operations.ops import ModifyTableOps, UpgradeOps
from sqlalchemy import (Boolean, Column, Date, Engine, Float, ForeignKey,
                        Index, Integer, MetaData, String, Table, create_engine,
                        create_mock_engine)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from jinja2 import Template
import os


def convert_to_dicts(
    input_list: Union[dict, list], generic_value_string="val"
) -> Union[dict, list]:
    """
    Converts the given input to a dict or list of dicts.

    This function is mainly needed to convert lists of scalars
    into a list of dicts with the scalar value assigned to the
    key `generic_value_string` which will bi used as the column
    name in of the resulting table.

    Args:
        input_list (dict, list): Input to convert.
        generic_value_string (str): Key to assign scalar
            values to (defaults to "val").

    Returns:
        Union[dict, list]
    """
    if isinstance(input_list, dict):
        return input_list
    output_list = []
    for item in input_list:
        if isinstance(item, dict) and item:
            output_list.append(item)
        elif isinstance(item, list) and item:
            output_list.extend(convert_to_dicts(item))
        elif isinstance(item, str) and item:
            output_list.append({generic_value_string: item})
        elif isinstance(item, (float, int, bool)):
            output_list.append({generic_value_string: item})
    return output_list


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

    if not logger.handlers:
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

    schema_changed: bool = False
    session = None
    loglevel: str = "INFO"
    progress: bool = True
    echo: bool = False
    sql: str = ""
    no_write: bool = False
    root_table: str = "main"
    simple: bool = False
    autocommit: bool = False

    def __init__(
        self,
        dburl: str = "sqlite://",
        simple: bool = False,
        root_table: str = "main",
        **kwargs,
    ) -> None:
        """
        The contructor for SQLThemAll class.

        Args:
            dburl (str): Database URI.
            progress (bool): Show import progress.
            loglevel (str): Set loglevel.
              Choice from "ERROR", "WARNING", "INFO", "DEBUG" (default "INFO").
            simple (bool): Create a simplified database schema.
            autocommit (bool): Open the database in autocommit mode.
            root_table (str): The name of the table to import the JSON root.
            echo (bool): Echo the executed SQL statements.
        """
        self.dburl = dburl
        for k in kwargs:
            if k == "loglevel":
                self.loglevel = kwargs["loglevel"]
            if k == "progress":
                self.progress = kwargs["progress"]
            if k == "echo":
                self.echo = kwargs["echo"]
                if self.echo:
                    self.progress = False
            if k == "no_write":
                self.no_write = kwargs["no_write"]
            if k == "autocommit":
                self.autocommit = kwargs["autocommit"]

        self.root_table = str(root_table).lower()
        self.simple = simple
        self._logger = create_logger("sqlthemall", self.loglevel)

        self.engine = create_engine(self.dburl, echo=self.echo)
        if self.autocommit:
            self.engine.execution_options(isolation_level="AUTOCOMMIT")
        self.metadata = MetaData()
        self.metadata.reflect(
            self.engine, extend_existing=True, autoload_replace=True
        )
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare(self.engine)
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
        self._logger.info(f"Creating table {name}")
        return Table(
            name,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            Index(
                f"idx_{name}_{current_table.name}_id",
                current_table.name + "_id",
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
        self._logger.info(f"Creating table {name}")
        self._logger.info(f"Creating bridge {current_table.name} - {name}")
        Table(
            "bridge_" + current_table.name + "_" + name,
            self.metadata,
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            Column(name + "_id", ForeignKey(name + "._id")),
            Index(
                f"idx_{name}_{current_table.name}_id",
                name + "_id",
            ),
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
        self._logger.info(f"Creating table {name}")
        return Table(
            name,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            Index(
                f"idx_{name}_{current_table.name}_id",
                current_table.name + "_id",
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
        self._logger.info(f"Creating table {name}")
        return Table(
            name,
            self.metadata,
            Column("_id", Integer, primary_key=True),
            Column(
                current_table.name + "_id",
                ForeignKey(current_table.name + "._id"),
            ),
            Index(
                f"idx_{name}_{current_table.name}_id",
                current_table.name + "_id",
            ),
            extend_existing=True,
        )

    def create_schema(
        self,
        jsonobj: dict,
        root_table: Optional[str] = None,
        simple: Optional[bool] = None,
        no_write: Optional[bool] = None,
    ) -> None:
        """
        Creates table_schema from the structure of a given JSON object.

        Args:
            jsonobj (dict): jsonobj.
            root_table (str): Table name of the JSON object root.
            simple (bool): Create a simple database schema.
            no_write (bool): Do not write the created schema to the database.
        """
        if root_table is None:
            root_table = self.root_table
        if simple is None:
            simple = self.simple
        if no_write is None:
            no_write = self.no_write

        self.schema_changed = False

        if root_table not in self.metadata.tables:
            self.schema_changed = True
            current_table = Table(
                root_table,
                self.metadata,
                Column("_id", Integer, primary_key=True),
                extend_existing=True,
            )
        else:
            current_table = self.metadata.tables[root_table]

        def parse_dict(
            obj: dict,
            current_table: Table = current_table,
            simple: bool = simple,
        ) -> None:
            """
            Creates table_schema from the structure of a given JSON object.

            Args:
                obj (dict): Object to parse.
                current_table (Table) : The current_table.
                simple (bool): Create a simple database schema.
            """

            def get_table(k, current_table, simple, simple_reltype=True):
                if k not in self.metadata.tables:
                    self.schema_changed = True
                    if not simple:
                        if simple_reltype:
                            tbl = self.create_many_to_one(
                                name=k, current_table=current_table
                            )
                        else:
                            tbl = self.create_many_to_many(
                                name=k, current_table=current_table
                            )
                    else:
                        if simple_reltype:
                            tbl = self.create_one_to_one(
                                name=k, current_table=current_table
                            )
                        else:
                            tbl = self.create_one_to_many(
                                name=k, current_table=current_table
                            )
                    return tbl
                return self.metadata.tables[k]

            col_types = {
                datetime.date: Date,
                str: String,
                bool: Boolean,
                int: Integer,
                float: Float,
            }

            for k, val in obj.items():
                if isinstance(val, (dict, list)):
                    val = convert_to_dicts(val)
                    if not val:
                        continue
                    tbl = get_table(
                        k=k,
                        current_table=current_table,
                        simple=simple,
                        simple_reltype=isinstance(val, dict),
                    )
                    if isinstance(val, dict):
                        parse_dict(obj=val, current_table=tbl, simple=simple)
                    else:
                        for i in val:
                            parse_dict(obj=i, current_table=tbl, simple=simple)
                elif k not in current_table.columns and type(val) in col_types:
                    self._logger.info(
                        f"Adding col {k} to table {current_table.name}"
                    )
                    self.schema_changed = True
                    current_table.append_column(
                        Column(k, col_types[val.__class__]())
                    )

        if isinstance(jsonobj, list):
            jsonobj = {self.root_table: jsonobj}
        parse_dict(obj=jsonobj)

        if self.schema_changed:
            self.base = automap_base(metadata=self.metadata)
            self.base.prepare(self.engine)
            self.classes = self.base.classes

            if no_write is not True:
               self.write_schema()

    def _reflect(self, engine: Optional[Engine] = None) -> MetaData:
        """
        Writes the given metadata object to the database.

        Args:
            metadata (MetaData): Metadata tp write.
        """
        if engine is None:
            engine = self.engine
        metadata = MetaData()
        metadata.reflect(engine, extend_existing=True, autoload_replace=True)
        return metadata

    def read_schema(self) -> None:
        """
        Writes the given metadata object to the database.

        Args:
            metadata (MetaData): Metadata tp write.
        """
        self.metadata = self._reflect()

    def write_schema(self) -> None:
        """
        Writes the given metadata object to the database.

        Args:
            metadata (MetaData): Metadata tp write.
        """
        with self.engine.connect() as conn:
            mc = MigrationContext.configure(connection=conn)
            migrations = produce_migrations(mc, self.metadata)
            operations = Operations(mc)

            use_batch = self.engine.name == "sqlite"
            stack = [migrations.upgrade_ops]
            while stack:
                elem = stack.pop(0)

                if use_batch and isinstance(elem, ModifyTableOps):
                    with operations.batch_alter_table(
                        elem.table_name, schema=elem.schema
                    ) as batch_ops:
                        for table_elem in elem.ops:
                            batch_ops.invoke(table_elem)
                elif hasattr(elem, 'ops'):
                    stack.extend(elem.ops)
                else:
                    operations.invoke(elem)
            try:
                conn.commit()
            except Exception:
                traceback.print_exc()

            self.base = automap_base(metadata=self.metadata)
            self.base.prepare(self.engine)
            self.classes = self.base.classes
            self.schema_changed = False

    def insert_data_to_schema(self, jsonobj: dict) -> None:
        """
        Inserts the given JSON object into the database creating.

        the schema if not availible.

        Args:
            jsonobj (dict): Object to parse.
        """

        def make_relational_obj(
            name, objc, session: Session, skip_empty: bool = True
        ):
            """
            Generates a relational object which is insertable from.

            a given JSON object.

            Args:
                name (str): Name of the table that will represent the object.
                objc (dict): Object to parse.
                session (Session): Session to use.
                skip_empty (bool): Skipts objects without any information.

            Returns:
                ormobject: Object defined by the object relational model.
            """
            if not isinstance(objc, dict) or not objc:
                return None
            pre_ormobjc, collectiondict = {}, {}
            for k, val in objc.items():
                if val is None or val == [] or val == {}:
                    if skip_empty is True:
                        continue
                if val is None:
                    continue
                if isinstance(val, dict):
                    collectiondict[k] = [
                        i
                        for i in [make_relational_obj(k, val, session=session)]
                        if i
                    ]
                elif isinstance(val, list):
                    val = convert_to_dicts(val)
                    if val is None:
                        continue
                    collectiondict[k] = [
                        make_relational_obj(k, i, session=session) for i in val
                    ]
                else:
                    if hasattr(self.base.classes[name], k):
                        pre_ormobjc[k] = val

            if self.progress:
                sys.stdout.write(".")
                sys.stdout.flush()
            self._logger.debug(f"{pre_ormobjc}")
            in_session = False
            if not self.simple:
                query = session.query(self.base.classes[name])
                in_session = query.filter_by(**pre_ormobjc).first()

            if in_session:
                ormobjc = in_session
                for k, val in collectiondict.items():
                    setattr(
                        ormobjc,
                        k.lower() + "_collection",
                        val,
                    )
            else:
                self._logger.debug(f"pre_ormobjc: {pre_ormobjc}")
                ormobjc = self.base.classes[name](**pre_ormobjc)
                for k, val in collectiondict.items():
                    setattr(
                        ormobjc,
                        k.lower() + "_collection",
                        val,
                    )

                session.add(ormobjc)
                self._logger.debug(f"Adding %s to session {name}")

            return ormobjc

        self.metadata = MetaData()
        self.metadata.reflect(
            self.engine, extend_existing=True, autoload_replace=True
        )
        self.base = automap_base(metadata=self.metadata)
        self.base.prepare(self.engine)
        self.classes = self.base.classes

        if isinstance(jsonobj, list):
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
        self.create_schema(jsonobj, no_write=False)
        self.insert_data_to_schema(jsonobj)

    def import_multi_json(self, jsonobjs: Iterable) -> None:
        """
        Inserts Array of JSON objects into the database creating.

        the schema if not availible.

        Args:
            jsonobjs (Iterable): Object to parse.
        """
        jsonobj = {self.root_table: jsonobjs}
        self.create_schema(jsonobj, no_write=False)
        self.insert_data_to_schema(jsonobj)

    def get_sql(self, engine=None, checkfirst=True):
        def dump(sql, *multiparams, **params):
            self.sql += sql.compile(dialect=engine.dialect).string

        if engine is None:
            engine = self.engine
        mock_engine = create_mock_engine(f"{engine.url.drivername}://", dump)
        self.sql = ""
        self.metadata.create_all(bind=mock_engine, checkfirst=checkfirst)
        return self.sql

    def describe_schema(self, metadata: Optional[MetaData] = None) -> str:
        """
        Describes the provided metadata.

        Original code taken from sadisplay:
        https://pypi.org/project/sadisplay

        Args:
            metadata (MetaData): MetaData to describe.
        """

        def describe_table(table):
            def get_indexes(table):
                indexes = []

                for index in table.indexes:
                    if not isinstance(index, Index):
                        continue

                    indexes.append(
                        {
                            "name": index.name,
                            "cols": get_columns_of_index(index),
                        }
                    )

                return indexes

            def get_columns(table):
                def column_type(column):
                    try:
                        return str(column.type)
                    except Exception:
                        # https://bitbucket.org/estin/sadisplay/issues/17/cannot-render-json-column-type
                        return type(column.type).__name__.upper()

                def column_role(column):
                    if column.primary_key:
                        return "pk"
                    if column.foreign_keys:
                        return "fk"
                    return None

                def column_compare(c):
                    prefix = {
                        "pk": "0",
                        "fk": "1",
                    }
                    return prefix.get(c[2], "2") + c[1]

                columns = []

                for col in table.columns:
                    columns.append(
                        (column_type(col), col.name, column_role(col))
                    )

                return sorted(columns, key=lambda c: column_compare(c))

            def get_columns_of_index(index):
                return [c.name for c in index.columns if isinstance(c, Column)]

            return {
                "name": table.name,
                "schema": table.schema,
                "indexes": get_indexes(table),
                "cols": get_columns(table),
            }

        # Detect relations by ForeignKey
        def get_fkeys(table):
            fkeys = []
            for col in table.columns:
                for fk in col.foreign_keys:
                    try:
                        fkeys.append(
                            {
                                "from": table.name,
                                "by": col.name,
                                "to": fk.column.table.name,
                                "to_col": fk.column.name,
                            }
                        )
                    except AttributeError:
                        traceback.print_exc()
            return fkeys

        if metadata is None:
            metadata = self.metadata

        tables, fkeys = [], []

        for table in metadata.sorted_tables:
            tables.append(describe_table(table))
            fkeys.extend(get_fkeys(table))

        return {"schema": {"tables": tables, "fkeys": fkeys}}

    def render_dot(self, opts={}) -> str:
        """
        Renders current MetaData as dot.

        """
        with open(os.path.dirname(__file__) + '/sadisplay.dot.j2') as f:
            t = Template(f.read())
        return t.render(self.describe_schema(), opts={"bgcolor": "lightyellow"})

