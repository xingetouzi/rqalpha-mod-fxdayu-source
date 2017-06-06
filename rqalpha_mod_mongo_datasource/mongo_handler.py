# encoding:utf-8
from pymongo.mongo_client import database
import pandas as pd
import pymongo


class DataHandler(object):

    def write(self, *args, **kwargs):
        pass

    def read(self, *args, **kwargs):
        pass

    def inplace(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

    def table_names(self, *args, **kwargs):
        pass


class MongoHandler(DataHandler):
    def __init__(self, host='localhost', port=27017, users=None, db=None, **kwargs):
        self.client = pymongo.MongoClient(host, port, **kwargs)
        self.db = self.client[db] if db else None

        if isinstance(users, dict):
            for db in users:
                self.client[db].authenticate(users[db]['id'], users[db]['password'])

    def _locate(self, collection, db=None):
        if isinstance(collection, database.Collection):
            return collection
        else:
            if db is None:
                return self.db[collection]
            elif isinstance(db, database.Database):
                return db[collection]
            else:
                return self.client[db][collection]

    def write(self, data, collection, db=None, index=None):
        """

        :param data(DataFrame|list(dict)): 要存的数据
        :param collection(str): 表名
        :param db(str): 数据库名
        :param index(str): 以index值建索引, None不建索引
        :return:
        """
        collection = self._locate(collection, db)
        data = self.normalize(data, index)
        collection.insert_many(data)
        if index:
            collection.create_index(index)
        return {'collection': collection.name, 'start': data[0], 'end': data[-1]}

    def read(self, collection, db=None, index='datetime', start=None, end=None, length=None, **kwargs):
        """

        :param collection(str): 表名
        :param db(str): 数据库名
        :param index(str): 读取索引方式
        :param start(datetime):
        :param end(datetime):
        :param length(int):
        :param kwargs:
        :return:
        """

        if index:
            if start:
                fter = {index: {'$gte': start}}
                if end:
                    fter[index]['$lte'] = end
                elif length:
                    kwargs['limit'] = length
                kwargs['filter'] = fter
            elif length:
                kwargs['sort'] = [(index, -1)]
                kwargs['limit'] = length
                if end:
                    kwargs['filter'] = {index: {'$lte': end}}
            elif end:
                kwargs['filter'] = {index: {'$lte': end}}

        db = self.db if db is None else self.client[db]

        if isinstance(collection, str):
            # print(collection)
            return self._read(db[collection], index, **kwargs)
        if isinstance(collection, database.Collection):
            return self._read(collection, index, **kwargs)
        elif isinstance(collection, (list, tuple)):
            panel = {}
            for col in collection:
                try:
                    if isinstance(col, database.Collection):
                        panel[col.name] = self._read(col, index, **kwargs)
                    else:
                        panel[col] = self._read(db[col], index, **kwargs)
                except KeyError as ke:
                    if index in str(ke):
                        pass
                    else:
                        raise ke
            return pd.Panel.from_dict(panel)
        else:
            return self._read(db[collection], index, **kwargs)

    @staticmethod
    def _read(collection, index=None, **kwargs):
        data = list(collection.find(**kwargs))

        for key, value in kwargs.get('sort', []):
            if value < 0:
                data.reverse()
        data = pd.DataFrame(data)
        if index:
            data.index = data.pop(index)

        if len(data):
            data.pop('_id')

        return data

    def inplace(self, data, collection, db=None, index='datetime'):
        """
        以替换的方式存(存入不重复)

        :param data(DataFrame|list(dict)): 要存的数据
        :param collection(str): 表名
        :param db(str): 数据库名
        :param index(str): 默认以datetime为索引替换
        :return:
        """

        collection = self._locate(collection, db)
        data = self.normalize(data, index)

        collection.delete_many({index: {'$gte': data[0][index], '$lte': data[-1][index]}})
        collection.insert_many(data)
        collection.create_index(index)
        return {'collection': collection.name, 'start': data[0], 'end': data[-1]}

    def update(self, data, collection, db=None, index='datetime', how='$set'):
        collection = self._locate(collection, db)

        if isinstance(data, pd.DataFrame):
            if index in data.columns:
                data.index = data[index]
            for name, doc in data.iterrows():
                collection.update_one({index: name}, {how: doc.to_dict()})
        else:
            for doc in data:
                collection.update_one({index: doc.pop(index)}, doc)

    def delete(self, filter, collection, db=None):
        collection = self._locate(collection, db)
        collection.delete_many(filter)

    def normalize(self, data, index=None):
        if isinstance(data, pd.DataFrame):
            if index and (index not in data.columns):
                data[index] = data.index
            return [doc[1].to_dict() for doc in data.iterrows()]
        elif isinstance(data, dict):
            key, value = list(map(lambda *args: args, *data.iteritems()))
            return list(map(lambda *args: dict(map(lambda x, y: (x, y), key, args)), *value))
        elif isinstance(data, pd.Series):
            if data.name is None:
                raise ValueError('name of series: data is None')
            name = data.name
            if index is not None:
                return list(map(lambda k, v: {index: k, name: v}, data.index, data))
            else:
                return list(map(lambda v: {data.name: v}, data))
        else:
            return data

    def table_names(self, db=None):
        if not db:
            return self.db.collection_names()
        else:
            return self.client[db].collection_names()