# encoding: utf-8
import re
import functools
from datetime import date

import numpy as np
import six
from dateutil.relativedelta import relativedelta
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.data.converter import StockBarConverter
from rqalpha.utils.datetime_func import convert_dt_to_int, convert_int_to_datetime

from .cache import CacheMixin

INSTRUMENT_TYPE_MAP = {
    INSTRUMENT_TYPE.CS: "stock",
    INSTRUMENT_TYPE.INDX: "stock",
}


class MongoConverter(object):
    @staticmethod
    def convert(df, fields=None):
        if fields is None:
            fields = ["datetime", "open", "high", "low", "close", "volume"]
        dt = df["datetime"]
        df["datetime"] = np.empty(len(df), dtype=np.uint64)
        dtype = np.dtype([('datetime', np.uint64)] +
                         [(f, StockBarConverter.field_type(f, df[f].dtype))
                          for f in fields if f != "datetime"])
        result = df[fields].values.ravel().view(dtype=dtype)
        result["datetime"] = dt.apply(lambda x: convert_dt_to_int(x))
        return result


class MongoDataSource(BaseDataSource):
    def __init__(self, path, mongo_url):
        super(MongoDataSource, self).__init__(path)
        from rqalpha_mod_mongo_datasource.mongo_handler import MongoHandler
        self._handler = MongoHandler(mongo_url)
        self._db_map = self._get_frequency_db_map()

    def _get_frequency_db_map(self):
        map_ = self._handler.client.get_database("meta").get_collection("db_map").find()
        dct = {item["type"]: item["map"] for item in map_}
        return dct

    def _get_db(self, instrument_type, frequency):
        try:
            type_ = INSTRUMENT_TYPE_MAP[instrument_type]
            time_frame = re.findall("[0-9]*(.+)", frequency)[0]
            return self._db_map[type_][time_frame]
        except KeyError:
            raise RuntimeWarning("MongoDB 中没有对应品种数据")
        except IndexError:
            raise RuntimeWarning("MongoDB 不支持的time_frame")

    @staticmethod
    def _get_code(instrument):
        order_book_id = instrument.order_book_id
        code = order_book_id.split(".")[0]
        if instrument.enum_type in {INSTRUMENT_TYPE.CS, INSTRUMENT_TYPE.INDX}:
            # 由数据库里的collection名确定
            if code[0] in {"0", "3"}:
                code = "sz" + code
            elif code[0] in {"6"}:
                code = "sh" + code
        return code

    def _get_k_data(self, instrument, frequency, fields=None, start_dt=None, end_dt=None, length=None):
        # 转换到自建mongodb结构
        code = self._get_code(instrument)
        db = self._get_db(instrument.enum_type, frequency)
        data = self._handler.read(code, db=db, start=start_dt, end=end_dt, length=length).reset_index()
        if data is not None:
            if fields is not None:
                if isinstance(fields, six.string_types):
                    fields = [fields]
                fields = [field for field in fields if field in data.columns]
            return MongoConverter.convert(data, fields)

    def _get_date_range(self, frequency):
        db = self._get_db(INSTRUMENT_TYPE.CS, frequency)
        from pymongo import DESCENDING
        try:
            start = self._handler.client.get_database(db).get_collection("sh600000").find() \
                .sort("_id").limit(1)[0]["datetime"]
            end = self._handler.client.get_database(db).get_collection("sh600000").find() \
                .sort("_id", direction=DESCENDING).limit(1)[0]["datetime"]
        except IndexError:
            raise RuntimeError("无法从MongoDb获取数据时间范围")
        return start.date(), end.date()

    def get_bar(self, instrument, dt, frequency):
        if frequency in {'1d'}:  # 日线从默认数据源拿
            return super(MongoDataSource, self).get_bar(instrument, dt, frequency)
        bar_data = self._get_k_data(instrument, frequency, end_dt=dt, length=1)
        if bar_data is None or not bar_data.size:
            return super(MongoDataSource, self).get_bar(instrument, dt, frequency)
        else:
            return bar_data[0]

    def history_bars(self, instrument, bar_count, frequency, fields, dt, skip_suspended=True, **kwargs):
        # TODO include_now = True
        bar_data = self._get_k_data(instrument, frequency, fields, end_dt=dt, length=bar_count)
        if bar_data is None or not bar_data.size:
            return super(MongoDataSource, self).history_bars(self, instrument, bar_count, frequency, fields, dt,
                                                             skip_suspended, **kwargs)
        else:
            return bar_data

    def current_snapshot(self, instrument, frequency, dt):
        pass

    def available_data_range(self, frequency):
        if frequency.endswith("d"):
            return date(2012, 6, 1), date.today() - relativedelta(days=1)
        return self._get_date_range(frequency)


class MongoCacheDataSource(MongoDataSource, CacheMixin):
    def __init__(self, path, mongo_url, cache_length=CacheMixin.CHUNK_LENGTH):
        MongoDataSource.__init__(self, path, mongo_url)
        CacheMixin.set_cache_length(cache_length)
        CacheMixin.__init__(self)

    def get_new_cache(self, instrument, frequency, dt, count):
        result = self._get_k_data(instrument, frequency, start_dt=dt, length=count + 1)
        dti = convert_dt_to_int(dt)
        if result["datetime"][0] == dti:
            return result[1:]
        else:
            return result[:count]
