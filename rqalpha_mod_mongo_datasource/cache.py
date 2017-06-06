import numpy as np
import functools
from weakref import proxy
from datetime import timedelta

from rqalpha.utils.datetime_func import convert_dt_to_int, convert_int_to_datetime


class Cache(object):
    def __init__(self, source, chunk, frequency):
        self._source = proxy(source)
        self._data = {}
        self._finish = {}
        self._chunk = chunk
        self._frequency = "1" + frequency

    def get_bar(self, instrument, dt):
        code = instrument.order_book_id
        update_bar = False
        bars = None
        if code not in self._data:
            update_bar = True
        else:
            bars = self._data[code]
            dti = np.uint64(convert_dt_to_int(dt))
            pos = bars["datetime"].searchsorted(dti)
            if pos >= len(bars):
                update_bar = True
            else:
                if bars["datetime"][pos] == dti:
                    return bars[pos]
                else:
                    return None
        if update_bar and not self._finish.get(code, False):
            if bars is not None and len(bars):
                last = convert_int_to_datetime(bars[-1]["datetime"])
            else:
                last = dt - timedelta(seconds=1)
            self._source.update_cache(instrument, self._frequency, last, self._chunk)
            return self.get_bar(instrument, dt)
        return None

    def history_bars(self, instrument, bar_count, fields, dt):
        code = instrument.order_book_id
        if code not in self._data:
            return None
        bars = self._data[code]
        dt = np.uint64(convert_dt_to_int(dt))
        pos = bars["datetime"].searchsorted(dt, side="right")
        if pos >= bar_count:
            left = pos - bar_count
            bars = bars[left:pos]
            return bars if fields is None else bars[fields]
        else:
            return None

    def update_bars(self, instrument, bars, count):
        code = instrument.order_book_id
        old = self._data.get(code, None)
        if old is not None and bars is not None:
            self._data[code] = np.concatenate((old, bars), axis=0)
        else:
            self._finish[code] = bars is None
            if old is not None:
                self._data[code] = old
            elif bars is not None:
                self._data[code] = bars
                # self._data[code] should never be None
        if code in self._data and len(self._data[code]) > self._chunk * 2:  # 保留两倍缓存长度的空间到内存
            left = len(self._data[code]) - self._chunk * 2
            self._data[code] = self._data[code][left:]
        if len(bars) < count:
            self._finish[code] = True


class CacheMixin(object):
    CHUNK_LENGTH = 1000
    ALLOW_CACHE = {"m", "d", "h"}

    def __init__(self):
        self._caches = {tf: Cache(self, self.CHUNK_LENGTH, tf) for tf in self.ALLOW_CACHE}
        self.get_bar = self.get_bar_decorator(self.get_bar)
        self.history_bars = self.history_bars_decorator(self.history_bars)

    @classmethod
    def set_cache_length(cls, value):
        cls.CHUNK_LENGTH = value

    def get_new_cache(self, instrument, frequency, dt, count):
        raise NotImplementedError

    def update_cache(self, instrument, frequency, dt, count):
        bars = self.get_new_cache(instrument, frequency, dt, count)
        cache = self._caches.get(frequency[-1], None)
        if cache:
            cache.update_bars(instrument, bars, count)

    def get_bar_decorator(self, func):
        @functools.wraps(func)
        def wrapped(instrument, dt, frequency):
            cache = self._caches.get(frequency[-1], None)
            if cache:
                data = cache.get_bar(instrument, dt)
                if data is not None:
                    return data
            return func(instrument, dt, frequency)

        return wrapped

    def history_bars_decorator(self, func):
        @functools.wraps(func)
        def wrapped(instrument, bar_count, frequency, fields, dt, skip_suspended=True, **kwargs):
            cache = self._caches.get(frequency[-1], None)
            if cache:
                data = cache.history_bars(instrument, bar_count, fields, dt)
                if data is not None and len(data):
                    return data
                return func(instrument, bar_count, frequency, fields,
                            dt, skip_suspended, **kwargs)

        return wrapped

    def get_bar(self, instruments, dt, frequency):
        raise NotImplementedError

    def history_bars(self, instrument, bar_count, frequency, fields, dt, skip_suspended=True, **kwargs):
        raise NotImplementedError
