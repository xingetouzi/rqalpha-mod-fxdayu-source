import unittest
import datetime

import pandas as pd

from rqalpha_mod_mongo_datasource.event_source import _date_range, IntervalEventSource


class TestEventSource(unittest.TestCase):
    # 初始化工作
    def setUp(self):
        pass

    # 退出清理工作
    def tearDown(self):
        pass

    def test_date_range(self):
        date = datetime.datetime.now().date()
        data = pd.DataFrame(sorted(list(IntervalEventSource._get_stock_trading_points(date, "13m"))))
        print(data)


if __name__ == '__main__':
    unittest.main()
