#!/usr/bin/env python3

import argparse
import json
import sys
import requests
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, Integer, String, BLOB, Float
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker


def createSimpleSchema(jsonobj, dburl):

    def parse_dict(obj, current_table):
    
        tables = metadata.sorted_tables
        tablenames = {t for t in tables}
    
        if obj.__class__ == dict:
            for k, v in obj.items():
                if k in {c.name for c in current_table.columns}:
                    if args.verbose and not args.quiet:
                        print(k + 'already exists in table ' + current_table.name)
                    continue
                else:
                    if v.__class__ == str:
                        current_table.append_column(Column(k, String()))
                        if not args.quiet:
                            print('  adding col ' + k)
                    elif v.__class__ == int:
                        current_table.append_column(Column(k, Integer()))
                        if not args.quiet:
                            print('  adding col ' + k)
                    elif v.__class__ == float:
                        current_table.append_column(Column(k, Float()))
                        if not args.quiet:
                            print('  adding col ' + k)
                    elif v.__class__ == dict:
                        if k not in {t for t in metadata.sorted_tables}:
                            if not args.quiet:
                                print('createing table ' + k)
                            t = Table(k, metadata,
                                      Column('_id', Integer, primary_key=True),
                                      extend_existing=True)
                            parse_dict(obj=v, current_table=t)
                            if t.name + '_key' not in {c.name for c in current_table.columns}:
                                current_table.append_column(
                                    Column(k + '_key', ForeignKey(k + '._id')))
                    elif v.__class__ == list:
                        v = [{'value': item} if item.__class__ !=
                             dict else item for item in v]
                        for item in v:
                            if k not in {t.name for t in metadata.sorted_tables}:
                                t = Table(k, metadata,
                                          Column('_id', Integer, primary_key=True))
                                parse_dict(obj=item, current_table=t)
                                if t.name + '_key' not in {c.name for c in current_table.columns}:
                                    current_table.append_column(
                                        Column(k + '_key', ForeignKey(k + '._id')))

    engine = create_engine(dburl)

    global metadata

    metadata = MetaData()
    metadata.reflect(engine)

    if not'main' in {t for t in metadata.tables}:
        current_table = Table('main', metadata,
                          Column('_id', Integer, primary_key=True))
    else:
        current_table = metadata.tables['main']

    parse_dict(jsonobj, current_table=current_table)

    Base = automap_base(metadata=metadata)
    
    Base.prepare()
    
    con = engine.connect()
    Base.metadata.create_all(bind=con)
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--databaseurl', nargs=1, dest='databaseurl', required=True, help='database url to use')
    parser.add_argument('-u', '--url', nargs=1, dest='url', help='url to read JSON from')
    parser.add_argument('-f', '--file', nargs=1, dest='file', help='file to read JSON from (default: stdin)')
    parser.add_argument('-v', '--verbose', action="store_true", help='print verbose output')
    parser.add_argument('-q', '--quiet', action="store_true", help="don't print output")

    args = parser.parse_args(sys.argv[1:])

    if args.url:
        res = requests.get(args.url[0])
        obj = res.json()
        if obj.__class__ == list:
            obj = {'main_array': obj}
        print(obj)
    elif args.file:
        with open(args.file[0]) as f:
            obj = json.loads(f.read())
    else:
        obj = json.loads(sys.stdin.read())
            
    createSimpleSchema(jsonobj=obj, dburl=args.databaseurl[0])   

