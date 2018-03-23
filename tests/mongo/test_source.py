# encoding: utf-8
import os
import pathlib
import unittest

from rqalpha_mod_fxdayu_source.data_source.mongo import MongoDataSource
from tests.common.source import TestDataSourceMixin


class TestMongoDataSource(unittest.TestCase, TestDataSourceMixin):
    # 初始化工作
    def setUp(self):
        path = pathlib.Path("~/.rqalpha/bundle").expanduser()
        self.source = MongoDataSource(str(path), os.environ.get("MONGO_URL"))

    def test_index(self):
        pass

if __name__ == '__main__':
    unittest.main()
