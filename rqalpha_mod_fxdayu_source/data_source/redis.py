from bisect import bisect_left, bisect_right
from collections import OrderedDict
from datetime import datetime, time

import numpy as np
from dateutil.parser import parse
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.data.converter import StockBarConverter
from rqalpha.utils import Singleton
from rqalpha.utils.datetime_func import convert_dt_to_int
from rqalpha.utils.logger import system_log

from rqalpha_mod_fxdayu_source.data_source.common import OddFrequencyDataSource
from rqalpha_mod_fxdayu_source.utils import InDayTradingPointIndexer

EMPTY_BARS = None
CONVERTER = StockBarConverter


class InDayIndexCache(object):
    __metaclass__ = Singleton

    def __init__(self):
        self._index = {}
        self._index_date = None

    def _trans_order_book_id(self, order_book_id):
        return "STOCK"
        # TODO need rqalpha environment to be create first
        # if get_account_type(order_book_id) == DEFAULT_ACCOUNT_TYPE.STOCK:
        #     return "STOCK"
        # else:
        #     return order_book_id

    def _ensure_index(self, frequency, order_book_id):
        today = datetime.now().date()
        if self._index_date != today:
            self._index.clear()
            self._index_date = today
        order_book_id = self._trans_order_book_id(order_book_id)
        if order_book_id not in self._index:
            self._index[order_book_id] = {}
        if frequency not in self._index:
            if order_book_id == "STOCK":
                self._index[order_book_id][frequency] = \
                    sorted(InDayTradingPointIndexer.get_a_stock_trading_points(today, frequency))
            else:
                raise RuntimeError("Future not support now")
        return self._index[order_book_id][frequency]

    def get_index(self, frequency, order_book_id):
        return self._ensure_index(frequency, order_book_id)


class RedisClient(object):
    __metaclass__ = Singleton

    def __init__(self, redis_url):
        import redis
        self._client = redis.from_url(redis_url)

    def get(self, order_book_id, frequency):
        return RedisBars(self._client, order_book_id, frequency)


class RedisBars(object):
    ALL_FIELDS = [
        "datetime", "open", "high", "low", "close", "volume"
    ]

    def __init__(self, client, order_book_id, frequency, indexer=None):
        """

        Parameters
        ----------
        client: redis.Redis
           redis connection
        order_book_id: str
           order book id of instruments
        frequency:
           frequency of data
        """
        self._client = client
        self._order_book_id = order_book_id
        self._frequency = frequency
        self._indexer = None
        self._converter = CONVERTER

    def _get_redis_key(self, key):
        return ":".join([self._order_book_id, key])

    @property
    def index(self):
        if self._indexer:
            return self._indexer.get_index(self._frequency, self._order_book_id)
        else:
            return [parse(item) for item in self._client.lrange(self._get_redis_key("datetime"), 0, -1)]

    def bars(self, l, r, fields=None):
        if fields is None:
            fields = self.ALL_FIELDS
        dtype = OrderedDict([(f, np.uint64 if f == "datetime" else np.float64) for f in fields])
        length = r - l
        result = np.empty(shape=(length,), dtype=list(dtype.items()))
        if not length:
            return result
        result.fill(np.nan)
        for field in fields:
            value = self._client.lrange(self._get_redis_key(field), l, r - 1)
            if field == "datetime":
                value = list(map(lambda x: convert_dt_to_int(parse(x.decode())), value))
            else:
                value = np.array(list(map(lambda x: x.decode(), value)), dtype=np.str)
                value = value.astype(np.float64)
            result[:len(value)][field] = value[:]
        return result

    def __len__(self):
        return

    def start(self):
        return

    def end(self):
        return

    def find(self, date, side="right"):
        dts = self.index
        if side == "left":
            index = bisect_left(dts, date)
        elif side == "right":
            index = bisect_right(dts, date)
        else:
            raise RuntimeError("unsupported side of find method, please use [left, right]")
        return index


class RedisDataSource(OddFrequencyDataSource, BaseDataSource):
    def __init__(self, path, redis_url, datasource=None):
        super(RedisDataSource, self).__init__(path)
        if not (redis_url.startswith("redis://") or redis_url.startswith("tcp://")):
            redis_url = "redis://" + redis_url.splits("//")[-1]
        self._history_datasource = datasource
        system_log.info("Connected to Redis on: %s" % redis_url)
        self._client = RedisClient(redis_url)

    def set_history_datasource(self, datasource):
        self._history_datasource = datasource

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        today = datetime.now().date()
        bars = self._client.get(instrument.order_book_id, frequency)
        history_bars = EMPTY_BARS
        today_bars = EMPTY_BARS
        if end_dt:
            if end_dt.date() >= today:
                idx_end = bars.find(end_dt, side="right")
                if start_dt:
                    if start_dt.date() > today or start_dt > end_dt:
                        return EMPTY_BARS  # 确定控制的返回形式
                    idx_start = bars.find(start_dt, side="left")
                    if start_dt.date() < today:
                        history_bars = self._history_datasource.raw_history_bars(
                            instrument,
                            frequency,
                            start_dt,
                            datetime.combine(today, time=time(hour=0, minute=0)),
                            None
                        )
                elif length:
                    idx_start = max(0, idx_end - length)
                    left = max(0, length - idx_end)
                    if left:
                        history_bars = self._history_datasource.raw_history_bars(
                            instrument,
                            frequency,
                            None,
                            datetime.combine(today, time=time(hour=0, minute=0)),
                            left
                        )
                today_bars = bars.bars(idx_start, idx_end)
            else:
                return self._history_datasource.raw_history_bars(instrument, frequency, start_dt, end_dt, length)
        elif start_dt and length:
            if start_dt.date() > today:
                return EMPTY_BARS
            elif start_dt.date() == today:
                idx_start = bars.find(start_dt, side="left")
                return bars.bars(idx_start, idx_start + length)
            else:
                history_bars = self._history_datasource.raw_history_bars(instrument, frequency, start_dt, end_dt,
                                                                         length)
                left = length - len(history_bars)
                if left:
                    today_bars = bars.bars(0, left)
        if history_bars is not None and today_bars is not None:
            return np.concatenate((history_bars, today_bars))
        elif history_bars is not None:
            return history_bars
        else:
            return today_bars

    def available_data_range(self, frequency):
        start, end = self._history_datasource.available_data_range(frequency)
        end = datetime.now().date()
        return start, end
