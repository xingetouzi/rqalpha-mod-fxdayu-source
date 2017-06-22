# encoding: utf-8
import logging
import functools
from weakref import proxy
from datetime import timedelta

import numpy as np
from lru import LRU

from rqalpha.utils.datetime_func import convert_dt_to_int, convert_int_to_datetime
from rqalpha.utils.logger import system_log


class Cache(object):
    def __init__(self, source, chunk, instrument, frequency):
        self._source = proxy(source)
        self._data = None
        self._finished = False
        self._chunk = chunk
        self._instrument = instrument
        self._frequency = frequency

    def raw_history_bars(self, start_dt=None, end_dt=None, length=None, updated=False):
        bars = self._data
        if bars is not None:
            if end_dt:
                end_dti = np.uint64(convert_dt_to_int(end_dt))
                end_pos = bars["datetime"].searchsorted(end_dti, side="right")
            if start_dt:
                start_dti = np.uint64(convert_dt_to_int(start_dt))
                start_pos = bars["datetime"].searchsorted(start_dti, side="left")
            if start_dt and end_dt:
                if end_pos < len(bars) or bars[-1]["datetime"] == end_dti:
                    if start_pos == 0 and bars[0]["datetime"] != start_dti:  # start datetime is early than cache
                        return None
                    else:
                        return bars[start_pos:end_pos]
                # else update the cache
            elif length is not None:
                if end_dt:
                    if end_pos < len(bars) or bars[-1]["datetime"] == end_dti:
                        if end_pos - length < 0:
                            return None
                        else:
                            return bars[end_pos - length: end_pos]
                    # else update the cache
                elif start_dt:
                    if start_pos == 0 and bars[0]["datetime"] != start_dti:
                        return None
                    if start_pos + length <= len(bars):
                        return bars[start_pos: start_pos + length]
                        # else update the cache
        # update the cache
        system_log.debug("缓存更新")
        if not self._finished and not updated:
            if bars is not None and len(bars):
                last = convert_int_to_datetime(bars[-1]["datetime"]) + timedelta(seconds=1)
            else:
                last = end_dt or start_dt
            self._source.update_cache(self._instrument, self._frequency, last, self._chunk)
            return self.raw_history_bars(start_dt, end_dt, length, updated=True)
        return None

    def update_bars(self, bars, count):
        old = self._data
        if old is not None and bars is not None:
            self._data = np.concatenate((self._data, bars), axis=0)
        else:
            if old is not None:
                self._data = old
            elif bars is not None:
                self._data = bars
                # self._data should never be None
        if self._data is not None and len(self._data) > self._chunk * 2:  # 保留两倍缓存长度的空间到内存
            left = len(self._data) - self._chunk * 2
            self._data = self._data[left:]
        self._finished = bars is None or len(bars) < count
        # import pandas as pd
        # system_log.debug(pd.DataFrame(self._data))


class CacheMixin(object):
    MAX_CACHE_SPACE = 40000000
    CACHE_LENGTH = 10000

    def __init__(self):
        self._caches = None
        self.init_cache()
        self.raw_history_bars = self.decorator_raw_history_bars(self.raw_history_bars)

    @classmethod
    def set_cache_length(cls, value):
        cls.CACHE_LENGTH = value

    @classmethod
    def set_max_cache_space(cls, value):
        cls.MAX_CACHE_SPACE = value

    def init_cache(self):
        if self._caches is None:
            self._caches = LRU(self.MAX_CACHE_SPACE // self.CACHE_LENGTH)
        else:
            self._caches.clear()

    def get_new_cache(self, instrument, frequency, dt, count):
        raise NotImplementedError

    def update_cache(self, instrument, frequency, dt, count):
        bars = self.get_new_cache(instrument, frequency, dt, count)
        key = (instrument.order_book_id, frequency)
        if key not in self._caches:
            self._caches[key] = Cache(self, self.CACHE_LENGTH, instrument, frequency)
        self._caches[key].update_bars(bars, count)

    def decorator_raw_history_bars(self, func):
        @functools.wraps(func)
        def wrapped(instrument, frequency, start_dt=None, end_dt=None, length=None):
            key = (instrument.order_book_id, frequency)
            if key not in self._caches:
                self._caches[key] = Cache(self, self.CACHE_LENGTH, instrument, frequency)
            data = self._caches[key].raw_history_bars(start_dt, end_dt, length)
            if data is not None:
                return data
            else:
                system_log.debug("缓存未命中")
                return func(instrument, frequency, start_dt=start_dt, end_dt=end_dt, length=length)

        return wrapped

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        raise NotImplementedError
