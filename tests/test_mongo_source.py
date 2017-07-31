# encoding: utf-8

import pathlib
import unittest
from datetime import datetime, timedelta

import pandas as pd

from rqalpha_mod_fxdayu_source.data.mongo import MongoDataSource


class TestMongoDataSource(unittest.TestCase):
    # 初始化工作
    def setUp(self):
        path = pathlib.Path("e:/Users/BurdenBear/.rqalpha/bundle")
        self.source = MongoDataSource(str(path), mongo_url="mongodb://192.168.0.103:30000")
        self._instrument = self.source.get_all_instruments()[0]

    # 退出清理工作
    def tearDown(self):
        pass

    def test_instrument(self):
        print(self._instrument)

    def test_data_range(self):
        print(self.source.available_data_range("1m"))

    def test_get_bar(self):
        print(type(self.source.get_bar(self._instrument, datetime.now() - timedelta(days=2), "1d")))
        print(self.source.get_bar(self._instrument, datetime.now(), "1m"))

    def test_history_bar(self):
        data = self.source.history_bars(self._instrument, 10, "1m",
                                        ["datetime", "close", "low", "high", "open", "volume"],
                                        datetime.now())
        print(pd.DataFrame(data))
    

if __name__ == '__main__':
    unittest.main()
