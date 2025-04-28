#!/usr/bin/env python3

from sqlalchemy.orm.collections import InstrumentedList


def dbobj2obj(dbobj, parent_class=None):
    """
    Converts a sqlalchemy.orm object to a python dictionary.

    mapping all defined columns to key/value pairs.

    Attributes:
        dbobj: Sqlalchemy ORM object.
        parent_class (type): Type of the parent_class of the object.
    """
    obj2 = {}
    for k, v in dbobj.__dict__.items():
        if k not in {"_sa_instance_state", "_id"} and not k.endswith("_id"):
            if v.__class__ in {str, int, float, bool}:
                obj2[k.lower()] = v
            elif v.__class__ == InstrumentedList:
                if v:
                    if v[0].__class__ != parent_class:
                        obj2[k.lower().replace("_collection", "")] = [
                            dbobj2obj(i, parent_class=dbobj.__class__)
                            for i in v
                        ]
    obj3 = {k: obj2[k] for k in sorted(obj2.keys())}
    if "value" in obj3.keys():
        return obj3["value"]
    return obj3


def compare_obj(obj1, obj2):
    """
    Compares two objects recursively.

    To be validated as equal
    all keys and values of the object itself as well as of
    subobjects arrays ect. must be equal.

    Attributes:
        obj1 (dict): A python object.
        obj2 (dict): A python object to be compared with.
    """

    def normalize(o):
        """
        Normalizes the provided object.

        Attributes:
            o (dict): A python object to normalize.
        """
        obj2, obj3 = {}, {}
        for k in o.keys():
            obj2[k.lower()] = o[k]
        for k in sorted(obj2.keys()):
            obj3[k] = obj2[k]
        return obj3

    def compare_val(val1, val2):
        """
        Compares two values provided.

        Attributes:
            val1: A python object.
            val2: A python object to be compared.
        """
        if isinstance(val1, (str, int, float)):
            return val1 == val2
        if isinstance(val1, bool):
            return val1 is val2
        if isinstance(val1, dict) and isinstance(val2, dict):
            val1, val2 = normalize(val1), normalize(val2)
            if not compare_obj(val1, val2):
                return False
        elif {val1.__class__, val2.__class__} == {dict, list}:
            if val1.__class__ == list and len(val1) == 1:
                val1 = val1[0]
            elif val2.__class__ == list and len(val2) == 1:
                val2 = val2[0]
            val1, val2 = normalize(val1), normalize(val2)
            if not compare_obj(val1, val2):
                return False
        elif val1.__class__ == list and val2.__class__ == list:
            for sval1, sval2 in zip(val1, val2):
                if not compare_val(sval1, sval2):
                    return False
        elif isinstance(val1, (list, dict)) and isinstance(val2, (list, dict)):
            if isinstance(val1, list) and len(val1) == 1:
                val1 = val1[0]
            elif isinstance(val2, list) and len(val2) == 1:
                val2 = val2[0]
            val1, val2 = normalize(val1), normalize(val2)
            return compare_obj(val1, val2)
        return True

    if isinstance(obj1, dict) and isinstance(obj2, dict):
        obj1, obj2 = normalize(obj1), normalize(obj2)

        for k in obj1:
            if obj1[k].__class__ == list and obj2[k].__class__ == list:
                for i in range(len(obj1[k])):
                    if not compare_val(obj1[k][i], obj2[k][i]):
                        return False
            if not compare_val(obj1[k], obj2[k]):
                return False
        return True

    if not compare_val(obj1, obj2):
        return False
    return True
