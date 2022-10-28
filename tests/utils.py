#!/usr/bin/env python3

from sqlalchemy.orm.collections import InstrumentedList


def dbobj2obj(o, parent_class=None):
    o2 = {}
    for k, v in o.__dict__.items():
        if k not in {"_sa_instance_state", "_id"} and not k.endswith("_id"):
            if v.__class__ in {str, int, float, bool}:
                o2[k.lower()] = v
            elif v.__class__ == InstrumentedList:
                if v:
                    if v[0].__class__ != parent_class:
                        o2[k.lower().replace("_collection", "")] = [
                            dbobj2obj(i, parent_class=o.__class__) for i in v
                        ]
    o3 = {k: o2[k] for k in sorted(o2.keys())}
    if "value" in o3.keys():
        return o3["value"]
    else:
        return o3


def compare_obj(o1, o2):
    def normalize(o):
        o2, o3 = {}, {}
        for k in o.keys():
            o2[k.lower()] = o[k]
        for k in sorted(o2.keys()):
            o3[k] = o2[k]
        return o3

    def compare_val(v1, v2):
        if v1.__class__ in {str, int, float, bool}:
            if v1 != v2:
                return False
        elif v1.__class__ == dict and v2.__class__ == dict:
            v1, v2 = normalize(v1), normalize(v2)
            if not compare_obj(v1, v2):
                return False
        elif {v1.__class__, v2.__class__} == {dict, list}:
            if v1.__class__ == list and len(v1) == 1:
                v1 = v1[0]
            elif v2.__class__ == list and len(v2) == 1:
                v2 = v2[0]
            v1, v2 = normalize(v1), normalize(v2)
            if not compare_obj(v1, v2):
                return False
        elif v1.__class__ == list and v2.__class__ == list:
            for sv1, sv2 in zip(v1, v2):
                if not compare_val(sv1, sv2):
                    return False
        return True

    if o1.__class__ == dict and o2.__class__ == dict:
        o1, o2 = normalize(o1), normalize(o2)

        for k in o1.keys():
            if o1[k].__class__ == list and o2[k].__class__ == list:
                for i in range(len(o1[k])):
                    if not compare_val(o1[k][i], o2[k][i]):
                        return False
            else:
                if not compare_val(o1[k], o2[k]):
                    return False
        return True

    else:
        if not compare_val(o1, o2):
            return False
    return True
