# encoding: utf-8

import datetime
import unittest

import pandas as pd

from rqalpha_mod_fxdayu_source.event_source import IntervalEventSource


class TestEventSource(unittest.TestCase):
    # 初始化工作
    def setUp(self):
        pass

    # 退出清理工作
    def tearDown(self):
        pass

    def test_trading_points(self):
        date = datetime.datetime.now().date()
        data = pd.DataFrame(sorted(list(IntervalEventSource._get_stock_trading_points(date, "13m"))))
        print(data)

if __name__ == '__main__':
    unittest.main()
