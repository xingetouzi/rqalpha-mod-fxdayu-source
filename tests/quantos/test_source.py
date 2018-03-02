# encoding: utf-8

import pathlib
import unittest
import itertools
from functools import lru_cache
from datetime import datetime, date, time

import numpy as np
import pandas as pd
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.model.instrument import Instrument
from rqalpha.utils.datetime_func import convert_dt_to_int

from rqalpha_mod_fxdayu_source.data_source.quantos import QuantOsSource

indexes = {
    "000300.XSHG",  # 中小板指
    "000016.XSHG",  # 上证50
    "000905.XSHG",  # 中证500
    "399006.XSHE",  # 创业板指
    "399005.XSHE",  # 中小板指
}

lengths = [10, 100]
frequencies = ["1d", "1m", "5m", "15m"]


class TestQuantOsDataSource(unittest.TestCase):
    # 初始化工作
    def setUp(self):
        path = pathlib.Path("~/.rqalpha/bundle").expanduser()
        self.source = QuantOsSource(str(path))

    # 退出清理工作
    def tearDown(self):
        pass

    @lru_cache(None)
    def get_stock(self):
        for i in self.source.get_all_instruments():
            if i.enum_type == INSTRUMENT_TYPE.CS:
                return i

    @lru_cache(None)
    def get_letv(self):
        for i in self.source.get_all_instruments():
            if i.order_book_id == "300104.XSHE":
                return i

    @lru_cache(None)
    def get_indexes(self):
        result = []
        for i in self.source.get_all_instruments():
            if i.enum_type == INSTRUMENT_TYPE.INDX and i.order_book_id in indexes:
                result.append(i)
                if len(result) == len(indexes):
                    break
        return result

    @lru_cache(None)
    def get_last_trading_day(self):
        dates = self.source.get_trading_calendar()
        d = dates[np.searchsorted(dates, datetime.now()) - 2]
        return datetime.combine(d, time=time(hour=15))

    def test_instrument(self):
        i1 = self.get_stock()
        i2 = self.get_letv()
        i3 = self.get_indexes()
        assert isinstance(i1, Instrument) and i1.enum_type == INSTRUMENT_TYPE.CS
        assert isinstance(i2, Instrument) and i2.order_book_id == "300104.XSHE"
        assert len(i3) == len(indexes)

    def test_data_range(self):
        start, end = self.source.available_data_range("1m")
        print(start, end)
        assert isinstance(start, date) and isinstance(end, date)

    def test_get_bar(self):
        instrument = self.get_stock()
        dt = self.get_last_trading_day()
        a1 = self.source.get_bar(instrument, dt, "1d")
        print(a1)
        a2 = self.source.get_bar(instrument, dt, "1m")
        print(a2)
        assert convert_dt_to_int(dt) == a1[0]
        assert convert_dt_to_int(dt) == a2[0]

    def test_stock(self):
        fields = ["datetime", "close", "low", "high", "open", "volume"]
        instrument = self.get_stock()
        dt = self.get_last_trading_day()
        for l, f in itertools.product(lengths, frequencies):
            data = self.source.history_bars(instrument, l, f, fields, dt, adjust_type=None)
            df = pd.DataFrame(data)
            print(df)
            assert set(df.columns) == set(fields)
            assert len(df) == l
            assert convert_dt_to_int(dt) == df["datetime"].iloc[-1]

    def test_suspended(self):
        fields = ["datetime", "close", "low", "high", "open", "volume"]
        instrument = self.get_letv()
        dt = datetime(year=2018, month=1, day=24, hour=15)
        for l, f in itertools.product(lengths, frequencies):
            data = self.source.history_bars(instrument, l, f, fields, dt, adjust_type=None)
            df = pd.DataFrame(data)
            print(df)
            assert set(df.columns) == set(fields)
            assert len(df) == l
            assert convert_dt_to_int(dt) == df["datetime"].iloc[-1]

    def test_index(self):
        fields = ["datetime", "close", "low", "high", "open", "volume"]
        instruments = self.get_indexes()
        dt = datetime(year=2018, month=1, day=24, hour=15)
        for i, l, f in itertools.product(instruments, lengths, frequencies):
            data = self.source.history_bars(i, l, f, fields, dt, adjust_type=None)
            df = pd.DataFrame(data)
            print(df)
            assert set(df.columns) == set(fields)
            assert len(df) == l
            assert convert_dt_to_int(dt) == df["datetime"].iloc[-1]


if __name__ == '__main__':
    unittest.main()
