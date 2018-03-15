from bisect import bisect_left, bisect_right
from collections import OrderedDict
from datetime import datetime

import numpy as np
from dateutil.parser import parse
from rqalpha.data.converter import StockBarConverter
from rqalpha.utils import Singleton
from rqalpha.utils.datetime_func import convert_int_to_datetime, convert_dt_to_int
from rqalpha.utils.logger import system_log

from rqalpha_mod_fxdayu_source.inday_bars.base import AbstractIndayBars
from rqalpha_mod_fxdayu_source.utils import InDayTradingPointIndexer

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

    def find(self, date, side="left"):
        dts = self.index
        if side == "left":
            index = bisect_left(dts, date)
        elif side == "right":
            index = bisect_right(dts, date)
        else:
            raise RuntimeError("unsupported side of find method, please use [left, right]")
        return index


class RedisIndayBars(AbstractIndayBars):
    def __init__(self, redis_url):
        super(AbstractIndayBars, self).__init__()
        if not (redis_url.startswith("redis://") or redis_url.startswith("tcp://")):
            redis_url = "redis://" + redis_url.splits("//")[-1]
        system_log.info("Connected to Redis on: %s" % redis_url)
        self._client = RedisClient(redis_url)

    def get_bars(self, instrument, frequency, trade_date=None, start_time=None, end_time=None):
        start_time = 0 if start_time is None else start_time
        end_time = 235959 if end_time is None else end_time
        start_dt = convert_int_to_datetime(trade_date * 1000000 + start_time)
        end_dt = convert_int_to_datetime(trade_date * 1000000 + end_time)
        bars = self._client.get(instrument.order_book_id, frequency)
        start_pos = bars.find(start_dt)
        end_pos = bars.find(end_dt)
        return bars.bars(start_pos, end_pos)
