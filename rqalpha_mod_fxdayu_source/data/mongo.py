# encoding: utf-8

from datetime import date

from dateutil.relativedelta import relativedelta
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.model.instrument import Instrument
from rqalpha.utils.py2 import lru_cache

from rqalpha_mod_fxdayu_source.module.cache import CacheMixin
from rqalpha_mod_fxdayu_source.module.odd import OddFrequencyDataSource
from rqalpha_mod_fxdayu_source.utils import DataFrameConverter, Singleton

INSTRUMENT_TYPE_MAP = {
    INSTRUMENT_TYPE.CS: "stock",
    INSTRUMENT_TYPE.INDX: "stock",
}


class NoneDataError(BaseException):
    pass


class MongoDataSource(OddFrequencyDataSource):
    __metaclass__ = Singleton

    def __init__(self, path, mongo_url):
        super(MongoDataSource, self).__init__(path)
        from rqalpha_mod_fxdayu_source.share.mongo_handler import MongoHandler
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
            message = instrument.order_book_id if isinstance(instrument, Instrument) else instrument
            raise NoneDataError("MongoDB 中没有品种%s的%s数据" % (message, frequency))

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        # 转换到自建mongodb结构s
        code = instrument.order_book_id
        db = self._get_db(instrument, frequency)
        data = self._handler.read(code, db=db, start=start_dt, end=end_dt, length=length, sort=[("datetime", 1)]).\
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

        try:
            start = self._handler.client.get_database(db).get_collection("600000.XSHG").find() \
                .sort("datetime").limit(1)[0]["datetime"]
            end = self._handler.client.get_database(db).get_collection("600000.XSHG").find() \
                .sort("datetime", direction=DESCENDING).limit(1)[0]["datetime"]
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
        super(MongoCacheDataSource, self).__init__(path, mongo_url)
        CacheMixin.__init__(self)
