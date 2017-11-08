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

    def __len__(self):
        return len(self._data) if self._data is not None else 0

    @property
    def last_dt(self):
        if len(self):
            return convert_int_to_datetime(self._data[-1]["datetime"])
        else:
            return None

    @property
    def chunk(self):
        return self._chunk

    @property
    def instrument(self):
        return self._instrument

    @property
    def frequency(self):
        return self._frequency

    @property
    def finished(self):
        return self._finished

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
        if not self._finished and not updated:
            self._source.update_cache(self, end_dt or start_dt)
            return self.raw_history_bars(start_dt, end_dt, length, updated=True)
        return None

    def update_bars(self, bars, count):
        system_log.debug("缓存更新,品种:[{}],时间:[{}, {}]".format(self.instrument.order_book_id,
                                                           bars["datetime"][0], bars["datetime"][-1]))
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

    def close(self):
        self._finished = True


class CacheMixin(object):
    MAX_CACHE_SPACE = 40000000
    CACHE_LENGTH = 10000

    def __init__(self, *args, **kwargs):
        self._caches = None
        self.clear_cache()
        self._raw_history_bars = self.raw_history_bars
        self.raw_history_bars = self.decorator_raw_history_bars(self.raw_history_bars)

    @classmethod
    def set_cache_length(cls, value):
        cls.CACHE_LENGTH = value

    @classmethod
    def set_max_cache_space(cls, value):
        cls.MAX_CACHE_SPACE = value

    def clear_cache(self):
        if self._caches is None:
            self._caches = LRU(self.MAX_CACHE_SPACE // self.CACHE_LENGTH)
        else:
            self._caches.clear()

    def update_cache(self, cache, dt):
        if len(cache):
            last = cache.last_dt + timedelta(seconds=1)
        else:
            bar_data = self._raw_history_bars(cache.instrument, cache.frequency,
                                              end_dt=dt - timedelta(seconds=1), length=cache.chunk)
            if bar_data is not None and len(bar_data):
                cache.update_bars(bar_data, len(bar_data))
            last = dt
        bar_data = self._raw_history_bars(cache.instrument, cache.frequency, start_dt=last, length=cache.chunk)
        if bar_data is not None and len(bar_data):
            cache.update_bars(bar_data, cache.chunk)
        else:
            cache.close()

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
