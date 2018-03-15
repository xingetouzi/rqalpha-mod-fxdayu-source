from datetime import datetime

import numpy as np
import pandas as pd
from rqalpha.data.converter import StockBarConverter
from rqalpha.utils.datetime_func import convert_dt_to_int, convert_int_to_datetime


class DataFrameConverter(object):
    @classmethod
    def df2np(cls, df, fields=None):
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

    @classmethod
    def empty(cls, fields=None):
        if fields is None:
            fields = ["datetime", "open", "high", "low", "close", "volume"]
        dtypes = [(f, StockBarConverter.field_type(f, np.float64)) if f != "datetime" else ('datetime', np.uint64)
                  for f in fields]
        return np.empty((0,), dtype=dtypes)

    @classmethod
    def np2df(cls, np_arr):
        df = pd.DataFrame(np_arr)
        df["datetime"] = df["datetime"].apply(convert_int_to_datetime)
        return df


class QuantOsConverter(DataFrameConverter):
    @classmethod
    def df2np(cls, df, fields=None):
        # daily bar
        if "time" not in df or (df["time"] == 0).all():
            df["time"] = 150000
        df["datetime"] = (df["trade_date"] * 1000000 + df["time"]).astype("int64").apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d%H%M%S")
        )
        return super(QuantOsConverter, cls).df2np(df, fields)
