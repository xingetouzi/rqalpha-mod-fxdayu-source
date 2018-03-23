# encoding: utf-8
import asyncio
from datetime import date, datetime, time

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.model.instrument import Instrument
from rqalpha.utils.datetime_func import convert_date_to_int, convert_int_to_datetime
from rqalpha.utils.py2 import lru_cache
import motor.motor_asyncio

from rqalpha_mod_fxdayu_source.data_source.common import CacheMixin
from rqalpha_mod_fxdayu_source.data_source.common.minite import MiniteBarDataSourceMixin
from rqalpha_mod_fxdayu_source.data_source.common.odd import OddFrequencyBaseDataSource
from rqalpha_mod_fxdayu_source.utils import Singleton
from rqalpha_mod_fxdayu_source.utils.converter import DataFrameConverter

INSTRUMENT_TYPE_MAP = {
    INSTRUMENT_TYPE.CS: "stock",
    INSTRUMENT_TYPE.INDX: "stock",
}


class NoneDataError(BaseException):
    pass


class MongoDataSource(OddFrequencyBaseDataSource, MiniteBarDataSourceMixin):
    __metaclass__ = Singleton

    def __init__(self, path, mongo_url):
        super(MongoDataSource, self).__init__(path)
        from rqalpha_mod_fxdayu_source.share.mongo_handler import MongoHandler
        self._handler = MongoHandler(mongo_url)
        self._client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)
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
            message = instrument.order_book_id if isinstance(instrument, Instrument) else instrument
            raise NoneDataError("MongoDB 中没有品种%s的%s数据" % (message, frequency))

    async def _do_get_bars(self, db, collection, filters, projection, fill=np.NaN):
        dct = {}
        l = 0
        async for doc in self._client[db][collection].find(filters, projection):
            _l = doc.pop('_l')
            l += _l
            for key, values in doc.items():
                if isinstance(values, list) and (len(values) == _l):
                    dct.setdefault(key, []).extend(values)
            for values in dct.values():
                if len(values) != l:
                    values.extend([fill] * l)
        df = pd.DataFrame(dct)
        if df.size:
            return df.sort_values("datetime")
        else:
            return None

    def _get_bars_in_days(self, instrument, frequency, params):
        s_date = params[0]["trade_date"]
        e_date = params[-1]["trade_date"]
        s_time = params[0]["start_time"] if "start_time" in params[0] else 0
        e_time = params[-1]["end_time"] if "end_time" in params[-1] else 150000
        s_dt_int = convert_date_to_int(s_date) + s_time
        e_dt_int = convert_date_to_int(e_date) + e_time
        db = self._get_db(instrument=instrument, frequency=frequency)
        collection = instrument.order_book_id
        filters = {"_d": {"$gte": datetime.combine(s_date, time=time()), "$lte": datetime.combine(e_date, time=time())}}
        projection = {"_id": 0, "_d": 0}
        loop = asyncio.get_event_loop()
        bars = loop.run_until_complete(self._do_get_bars(db, collection, filters, projection))
        if bars is not None and bars.size:
            bars = DataFrameConverter.df2np(bars)
        else:
            bars = DataFrameConverter.empty()
        s_pos = np.searchsorted(bars["datetime"], s_dt_int)
        e_pos = np.searchsorted(bars["datetime"], e_dt_int, side="right")
        return bars[s_pos:e_pos]

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        # 转换到自建mongodb结构s
        if frequency.endswith("m"):
            return MiniteBarDataSourceMixin.raw_history_bars(
                self, instrument, frequency, start_dt=start_dt, end_dt=end_dt, length=length)
        else:
            code = instrument.order_book_id
            db = self._get_db(instrument, frequency)
            data = self._handler.read(code, db=db, start=start_dt, end=end_dt, length=length, sort=[("datetime", 1)]). \
                reset_index()
            if data is not None and data.size:
                return DataFrameConverter.df2np(data)
            else:
                return DataFrameConverter.empty()

    def is_base_frequency(self, instrument, frequency):
        if isinstance(instrument, Instrument):
            instrument_type = instrument.enum_type
        else:
            instrument_type = instrument
        type_ = INSTRUMENT_TYPE_MAP[instrument_type]
        return type_ in self._db_map and frequency in self._db_map[type_]

    def current_snapshot(self, instrument, frequency, dt):
        pass

    def _get_date_range(self, frequency):
        from pymongo import DESCENDING
        try:
            db = self._get_db(INSTRUMENT_TYPE.CS, frequency)
        except NoneDataError:
            db = self._get_db(INSTRUMENT_TYPE.CS, "1" + frequency[-1])
        key = "_d" if frequency.endswith("m") else "datetime"
        try:
            start = self._handler.client.get_database(db).get_collection("600000.XSHG").find() \
                .sort(key).limit(1)[0][key]
            end = self._handler.client.get_database(db).get_collection("600000.XSHG").find() \
                .sort(key, direction=DESCENDING).limit(1)[0][key]
        except IndexError:
            raise RuntimeError("无法从MongoDb获取数据时间范围")
        return start.date(), end.date()

    @lru_cache(maxsize=10)
    def available_data_range(self, frequency):
        if frequency.endswith("d") or frequency.endswith("h"):
            return date(2012, 6, 1), date.today() - relativedelta(days=1)
        return self._get_date_range(frequency)


class MongoCacheDataSource(MongoDataSource, CacheMixin):
    def __init__(self, path, mongo_url):
        super(MongoCacheDataSource, self).__init__(path, mongo_url)
        CacheMixin.__init__(self)
