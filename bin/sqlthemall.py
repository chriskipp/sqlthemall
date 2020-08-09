#!/usr/bin/env python3

# Imports
import json
import sys
import argparse
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, Integer, String, BLOB, Float
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
import requests


def sqlthemall(jsonobj, dburl):
    engine = create_engine(dburl)
    
    global metadata
    
    metadata = MetaData()
    metadata.reflect(engine)
    
    if not'main' in {t for t in metadata.tables}:
        current_table = Table('main', metadata,
                              Column('_id', Integer, primary_key=True))
    else:
        current_table = metadata.tables['main']
    

    def parse_dict(obj, current_table=current_table):
    
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
                        if k not in metadata.tables:
                            if not args.quiet:
                                print('createing table ' + k)
                            t = Table(k, metadata,
                                      Column('_id', Integer, primary_key=True),
                                      Column(current_table.name + '_id', ForeignKey(current_table.name + '._id'))
                                     )
                        else:
                            t = metadata.tables[k]
                        parse_dict(obj=v, current_table=t)
                        
                    elif v.__class__ == list:
                        if v:
                            v = [{'value': item} if item.__class__ !=
                                 dict else item for item in v]
                            for item in v:
                                if k not in metadata.tables:
                                    bridge = Table('bridge_' + current_table.name + '_' + k, metadata,
                                                   Column(current_table.name + '_id',
                                                         ForeignKey(current_table.name + '._id')
                                                         ),
                                                   Column(k + '_id',
                                                         ForeignKey(k + '._id')
                                                         )
                                                  )
                                    t = Table(k, metadata,
                                              Column('_id', Integer, primary_key=True)
                                             )
                                else:
                                    t = metadata.tables[k]
                                parse_dict(obj=item, current_table=t)
    
    parse_dict(jsonobj)

    Base = automap_base(metadata=metadata)
    Base.prepare(engine)

    con = engine.connect()
    Base.metadata.create_all(bind=con)


def importDataToSchema(jsonobj, dburl):
    engine = create_engine(dburl)
    engine.connect()
    
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    classes = Base.classes
    
    Session = sessionmaker(engine, autocommit=args.autocommit)
    global session
    session = Session()
    
    def make_relational_obj(name, objc):
        pre_ormobjc = dict()
        collectiondict = dict()
        for k, v in objc.items():
            if v.__class__ == dict().__class__:
                collectiondict[k] = [make_relational_obj(k, v)]
            elif v.__class__ == list().__class__:
                if v:
                    v = [i if i.__class__ == dict().__class__ else {'value':i} for i in v]
                    collectiondict[k] = [make_relational_obj(k, i) for i in v]
            elif v == None:
                continue
            else:
                pre_ormobjc[k] = v
        if not args.quiet:
            if args.verbose:
                print(pre_ormobjc)
            else:
                sys.stdout.write('.')
                sys.stdout.flush()
        try:
            in_session = session.query(classes[name]).filter_by(**pre_ormobjc).all()
        except:
            in_session = False
        if in_session:
            ormobjc = in_session[0]
            if collectiondict:
                for k in collectiondict:
                    setattr(ormobjc, k.lower() + '_collection', collectiondict[k])
        else:
            try:
                ormobjc = classes[name](**pre_ormobjc)
            except:
                ormobjc = classes[name]()
                for k, v in pre_ormobjc.items():
                    ormobjc.__setattr__(k, v)

            if collectiondict:
                for k in collectiondict:
                    setattr(ormobjc, k.lower() + '_collection', collectiondict[k])
        return ormobjc
    
    o = make_relational_obj(name='main', objc=jsonobj)
    for c in metadata.tables:
        if not c.startswith('bridge'):
            instances = session.query(classes[c]).all()

    if not args.autocommit:
        session.commit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--databaseurl', nargs=1, dest='databaseurl', required=True, help='database url to use')
    parser.add_argument('-u', '--url', nargs=1, dest='url', help='url to read JSON from')
    parser.add_argument('-f', '--file', nargs=1, dest='file', help='file to read JSON from (default: stdin)')
    parser.add_argument('-n', '--noimport', action="store_true", help='only creates database schema, skips import')
    parser.add_argument('-a', '--autocommit', action="store_true", help='opens database in autocommit mode')
    parser.add_argument('-v', '--verbose', action="store_true", help='print verbose output')
    parser.add_argument('-q', '--quiet', action="store_true", help="don't print output")
    args = parser.parse_args(sys.argv[1:])

    if args.url:
        res = requests.get(args.url[0])
        obj = res.json()
        if obj.__class__ == list:
            obj = {'main_array': obj}
    elif args.file:
        with open(args.file[0]) as f:
            obj = json.loads(f.read())
    else:
        obj = json.loads(sys.stdin.read())
            
    sqlthemall(jsonobj=obj, dburl=args.databaseurl[0])   
    if not args.noimport:
        print('\nImporting Objects')
        importDataToSchema(jsonobj=obj, dburl=args.databaseurl[0])   
        print()


