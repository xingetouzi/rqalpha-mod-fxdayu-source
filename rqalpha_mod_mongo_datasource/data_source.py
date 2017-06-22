# encoding: utf-8
import re
from datetime import date

import six
from dateutil.relativedelta import relativedelta
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.model.instrument import Instrument
from rqalpha.utils.datetime_func import convert_dt_to_int
from rqalpha.utils.py2 import lru_cache

from rqalpha_mod_mongo_datasource.module.cache import CacheMixin
from rqalpha_mod_mongo_datasource.module.odd import OddFrequencyDataSource
from rqalpha_mod_mongo_datasource.utils import DataFrameConverter

INSTRUMENT_TYPE_MAP = {
    INSTRUMENT_TYPE.CS: "stock",
    INSTRUMENT_TYPE.INDX: "stock",
}


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


class NoneDataError(BaseException):
    pass


class MongoDataSource(OddFrequencyDataSource, BaseDataSource):
    __metaclass__ = Singleton

    def __init__(self, path, mongo_url):
        super(MongoDataSource, self).__init__(path)
        from rqalpha_mod_mongo_datasource.mongo_handler import MongoHandler
        self._handler = MongoHandler(mongo_url)
        self._db_map = self._get_frequency_db_map()

    def _get_frequency_db_map(self):
        map_ = self._handler.client.get_database("meta").get_collection("db_map").find()
        dct = {item["type"]: item["map"] for item in map_}
        return dct

    def _get_db(self, instrument, frequency):
        try:
            if isinstance(instrument, Instrument):
                instrument_type = instrument.enum_type
            else:
                instrument_type = instrument
            type_ = INSTRUMENT_TYPE_MAP[instrument_type]
            return self._db_map[type_][frequency]
        except KeyError:
            raise NoneDataError("MongoDB 中没有品种%s的%s数据" % (instrument.order_book_id, frequency))

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

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        # 转换到自建mongodb结构
        code = self._get_code(instrument)
        db = self._get_db(instrument, frequency)
        data = self._handler.read(code, db=db, start=start_dt, end=end_dt, length=length).reset_index()
        if data is not None and data.size:
            return DataFrameConverter.df2np(data)

    def is_base_frequency(self, instrument, frequency):
        if isinstance(instrument, Instrument):
            instrument_type = instrument.enum_type
        else:
            instrument_type = instrument
        type_ = INSTRUMENT_TYPE_MAP[instrument_type]
        return type_ in self._db_map and frequency in self._db_map[type_]

    def get_bar(self, instrument, dt, frequency):
        if frequency in {'1d'}:  # 日线从默认数据源拿
            return super(MongoDataSource, self).get_bar(instrument, dt, frequency)
        bar_data = self.raw_history_bars(instrument, frequency, end_dt=dt, length=1)
        if bar_data is None or not bar_data.size:
            return super(MongoDataSource, self).get_bar(instrument, dt, frequency)
        else:
            dti = convert_dt_to_int(dt)
            return bar_data[0] if bar_data[0]["datetime"] == dti else None

    def current_snapshot(self, instrument, frequency, dt):
        pass

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

    @lru_cache(maxsize=10)
    def available_data_range(self, frequency):
        if frequency.endswith("d"):
            return date(2012, 6, 1), date.today() - relativedelta(days=1)
        return self._get_date_range(frequency)


class MongoCacheDataSource(MongoDataSource, CacheMixin):
    def __init__(self, path, mongo_url):
        MongoDataSource.__init__(self, path, mongo_url)
        CacheMixin.__init__(self)

    def get_new_cache(self, instrument, frequency, dt, count):
        bar_data = super(MongoCacheDataSource, self).raw_history_bars(instrument, frequency, start_dt=dt, length=count)
        return bar_data
