import datetime

import numpy as np
import pandas as pd
from rqalpha.const import DEFAULT_ACCOUNT_TYPE
from rqalpha.data.converter import StockBarConverter
from rqalpha.utils import get_account_type
from rqalpha.utils.datetime_func import convert_dt_to_int, convert_int_to_datetime


class DataFrameConverter(object):
    @staticmethod
    def df2np(df, fields=None):
        if fields is None:
            fields = ["datetime", "open", "high", "low", "close", "volume"]
        dtypes = [(f, StockBarConverter.field_type(f, df[f].dtype)) if f != "datetime" else ('datetime', np.uint64)
                  for f in fields]
        if "datetime" in fields:
            dt = df["datetime"]
            df["datetime"] = np.empty(len(df), dtype=np.uint64)
        result = df[fields].values.ravel().view(dtype=np.dtype(dtypes))
        if "datetime" in fields:
            result["datetime"] = dt.apply(convert_dt_to_int)
        return result[fields]

    @staticmethod
    def empty(fields=None):
        if fields is None:
            fields = ["datetime", "open", "high", "low", "close", "volume"]
        dtypes = [(f, StockBarConverter.field_type(f, np.float64)) if f != "datetime" else ('datetime', np.uint64)
                  for f in fields]
        return np.empty((0, ), dtype=dtypes)

    @staticmethod
    def np2df(np_arr):
        df = pd.DataFrame(np_arr)
        df["datetime"] = df["datetime"].apply(convert_int_to_datetime)
        return df


class Singleton(type):
    SINGLETON_ENABLED = True

    def __init__(cls, *args, **kwargs):
        cls._instance = None
        super(Singleton, cls).__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if cls.SINGLETON_ENABLED:
            if cls._instance is None:
                cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
                return cls._instance
            else:
                return cls._instance
        else:
            return super(Singleton, cls).__call__(*args, **kwargs)


_freq_map = {
    "m": "T",
    "h": "H",
    "d": "D"
}


def _cal_date_range(start, end, freq):
    unit_freq = freq[-1]
    dates = pd.date_range(start, end, freq=freq[:-1] + _freq_map[unit_freq]) - pd.Timedelta(minutes=1)
    dates = dates.to_pydatetime()
    if dates.size:
        dates = dates[1:]
    if not dates.size or dates[-1] != end:
        dates = np.concatenate([dates, [end]])
    return dates


class InDayTradingPointIndexer(object):
    @staticmethod
    def get_a_stock_trading_points(trading_date, frequency):
        trading_points = set()
        current_dt = datetime.datetime.combine(trading_date, datetime.time(9, 31))
        am_end_dt = current_dt.replace(hour=11, minute=30)
        pm_start_dt = current_dt.replace(hour=13, minute=1)
        pm_end_dt = current_dt.replace(hour=15, minute=0)
        sessions = [(current_dt, am_end_dt), (pm_start_dt, pm_end_dt)]
        for start, end in sessions:
            trading_points.update(_cal_date_range(start, end, frequency))
        return trading_points

    @staticmethod
    def get_future_trading_points(env, trading_date, frequency):
        if frequency == "1m":
            trading_minutes = set()
            universe = env.get_universe()
            for order_book_id in universe:
                if get_account_type(order_book_id) == DEFAULT_ACCOUNT_TYPE.STOCK:
                    continue
                trading_minutes.update(env.data_proxy.get_trading_minutes_for(order_book_id, trading_date))
            return set([convert_int_to_datetime(minute) for minute in trading_minutes])
        # TODO future hours
        return set()
