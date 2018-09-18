import os
import pathlib
import unittest
from datetime import datetime, timedelta

import pandas as pd
from dateutil.parser import parse
from rqalpha.utils.datetime_func import convert_int_to_datetime, convert_dt_to_int

from rqalpha_mod_fxdayu_source.data_source.mongo import MongoCacheDataSource


class mytest(unittest.TestCase):
    # 初始化工作
    def setUp(self):
        self.path = pathlib.Path("~/.rqalpha/bundle").expanduser()
        self.mongo_url = os.environ.get("MONGO_URL")
        self._instrument = MongoCacheDataSource(self.path, self.mongo_url).get_all_instruments()[0]

    # 退出清理工作
    def tearDown(self):
        pass

    def test_instrument(self):
        print(self._instrument)

    def test_data_range(self):
        source = MongoCacheDataSource(self.path, self.mongo_url)
        print(source.available_data_range("1m"))

    def test_get_bar(self):
        source = MongoCacheDataSource(self.path, self.mongo_url)
        print(type(source.get_bar(self._instrument, datetime.now() - timedelta(days=2), "1d")))
        print(source.get_bar(self._instrument, datetime.now(), "1m"))

    def test_history_bars(self):
        source = MongoCacheDataSource(self.path, self.mongo_url)
        data = source.history_bars(self._instrument, 10, "1m",
                                   ["datetime", "close", "low", "high", "open", "volume"],
                                   datetime.now())
        source.clear_cache()
        print(pd.DataFrame(data))

    def get_cache_info(self, source, frequency):
        cache = source._caches[(self._instrument.order_book_id, frequency)]
        return cache._data[0]["datetime"], cache._data[-1]["datetime"], len(cache._data)

    def test_raw_history_bars(self):
        source = MongoCacheDataSource(self.path, self.mongo_url)
        start = parse("2012-06-01 9:31:00")
        si = convert_dt_to_int(start)
        frequency = "1m"
        first = source.raw_history_bars(self._instrument, frequency, start_dt=start,
                                        length=source.CACHE_LENGTH)
        s, e, l = self.get_cache_info(source, frequency)
        assert s == si and l == source.CACHE_LENGTH
        data = source.raw_history_bars(self._instrument, frequency,
                                       end_dt=convert_int_to_datetime(first["datetime"][-1]),
                                       length=source.CACHE_LENGTH)
        s, e, l = self.get_cache_info(source, frequency)
        assert s == si and l == source.CACHE_LENGTH
        next_ = source.raw_history_bars(self._instrument, frequency,
                                        start_dt=convert_int_to_datetime(first["datetime"][5]),
                                        length=source.CACHE_LENGTH)
        s, e, l = self.get_cache_info(source, frequency)
        assert s == si and l == source.CACHE_LENGTH * 2
        assert (first == data).all()
        print(pd.DataFrame(next_))


if __name__ == '__main__':
    unittest.main()
