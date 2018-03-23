from pymongo import MongoClient
import pandas as pd
from collections import Iterable
import numpy as np


def read(collection, filters=None, projection=None):
    return pd.DataFrame(get_docs(collection, filters, projection)).set_index("datetime")


def get_docs(collection, filters=None, projection=None, fill=np.NaN):
    dct = {}
    if isinstance(projection, dict):
        projection['_id'] = 0
        projection["_l"] = 1
    elif isinstance(projection, Iterable):
        projection = dict.fromkeys(projection, 1)
        projection["_id"] = 0
        projection["_l"] = 1
    else:
        projection = {"_id": 0}
    cursor = collection.find(filters, projection)
    LENGTH = 0
    for doc in cursor:
        l = doc.pop('_l')
        LENGTH += l
        for key, values in doc.items():
            if isinstance(values, list) and (len(values) == l):
                dct.setdefault(key, []).extend(values)
        for values in dct.values():
            if len(values) != LENGTH:
                values.extend([fill]*l)
    return dct


if __name__ == '__main__':
    client = MongoClient("192.168.0.102")
    print(read(client["Stock_1M"]["000001.XSHE"]))