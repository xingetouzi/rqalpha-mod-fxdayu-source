import numpy as np
import pandas as pd
from rqalpha.data.converter import StockBarConverter
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
    def np2df(np_arr):
        df = pd.DataFrame(np_arr)
        df["datetime"] = df["datetime"].apply(convert_int_to_datetime)
        return df
