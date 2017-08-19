import os
import unittest
from datetime import datetime, timedelta, time

from rqalpha.utils.datetime_func import convert_dt_to_int

from rqalpha_mod_fxdayu_source.data.mongo import MongoDataSource
from rqalpha_mod_fxdayu_source.data.redis import RedisDataSource

RQALPHA_ROOT = os.environ.get("RQALPHA_ROOT")
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = os.environ.get("REDIS_PORT")


class mytest(unittest.TestCase):
    # 初始化工作
    def setUp(self):
        path = os.path.join(RQALPHA_ROOT, "bundle")
        history_source = self.source = MongoDataSource(str(path), mongo_url="mongodb://192.168.0.101:27017")
        self.source = RedisDataSource(path, REDIS_HOST, REDIS_PORT, history_source)
        self.instrument = self.source.get_all_instruments()[0]

    def test_raw_history_bars_with_history(self):
        end = datetime.now()
        ei = convert_dt_to_int(end)
        frequency = "1m"
        df = self.source.raw_history_bars(self.instrument, frequency, end_dt=end,
                                          length=300)
        print(df)

    def test_raw_history_bars_without_history(self):
        end = datetime.now()
        ei = convert_dt_to_int(end)
        frequency = "1m"
        df = self.source.raw_history_bars(self.instrument, frequency, end_dt=end,
                                          length=2)
        print(df)

    def test_start_history_bars_start_end(self):
        end = datetime.now()
        start = datetime.combine(end.date(), time=time(hour=9, minute=33))
        print(start)
        frequency = "1m"
        df = self.source.raw_history_bars(self.instrument, frequency, start_dt=start, end_dt=end)
        print(df)

if __name__ == '__main__':
    unittest.main()
