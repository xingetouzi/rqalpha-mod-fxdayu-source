import asyncio
from datetime import date, datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.utils import lru_cache
from rqalpha.utils.datetime_func import convert_date_to_int, convert_int_to_date
from rqalpha.utils.logger import system_log

from rqalpha_mod_fxdayu_source.data_source.common import CacheMixin
from rqalpha_mod_fxdayu_source.data_source.common.odd import OddFrequencyBaseDataSource
from rqalpha_mod_fxdayu_source.utils import Singleton
from rqalpha_mod_fxdayu_source.utils.converter import QuantOsConverter
from rqalpha_mod_fxdayu_source.utils.instrument import instrument_to_tushare
from rqalpha_mod_fxdayu_source.utils.quantos import QuantOsDataApiMixin


def parse_time_int(n):
    hour, n = n // 10000, n % 10000
    minuter, second = n // 100, n % 100
    return hour, minuter, second


def bar_count_in_section(start, end, base=60, offset=0):
    hs, ms, ss = parse_time_int(start)
    he, me, se = parse_time_int(end)
    n_s = (hs * 3600 + ms * 60 + ss - 1 + offset) // base + 1
    n_e = (he * 3600 + me * 60 + offset) // base
    return max(n_e - n_s, 0)


def safe_searchsorted(a, v, side='left', sorter=None):
    if not len(a):
        raise RuntimeError("Can't search in a empty array!")
    pos = np.searchsorted(a, v, side=side, sorter=sorter)
    if pos >= len(a):
        system_log.warning(RuntimeWarning(
            "Value to search [%s] beyond array range [ %s - %s ], there may be some data missing."
            % (v, a[0], a[-1])
        ))
        return len(a)
    return pos


class QuantOsSource(OddFrequencyBaseDataSource, QuantOsDataApiMixin):
    __metaclass__ = Singleton

    def __init__(self, path, api_url=None, user=None, token=None):
        super(QuantOsSource, self).__init__(path)
        QuantOsDataApiMixin.__init__(self, api_url, user, token)

    def get_bar_count_in_day(self, instrument, frequency, trade_date=None, start_time=0, end_time=150000):
        """
        Get bar count of given instrument and frequency in a signle trading day,
        supporting frequency of Xm and Xh.

        Parameters
        ----------
        instrument: rqalpha.model.instrument.Instrument
            Instrument to query.
        frequency:
            Frequency to query.
        trade_date: date
            Trade date to query.
        start_time: int
            Int to represent start time, inf format "HHMMSS".
        end_time: int
            Int to represent end time, inf format "HHMMSS".
        Returns
        -------
        int: Return bar count in a single trading day.
        """

        if instrument.enum_type in [INSTRUMENT_TYPE.CS, INSTRUMENT_TYPE.INDX]:
            unit = frequency[-1]
            number = int(frequency[:-1])
            if unit == "m":
                offset = 0
                factor = 60
            else:
                offset = 30 * 60  # 30min
                factor = 60 * 60
            start_time = max(93000, start_time)
            end_time = min(150000, end_time)
            if start_time > 113000:
                return bar_count_in_section(start_time, end_time, number * factor, offset)
            elif end_time <= 113000:
                return bar_count_in_section(start_time, end_time, number * factor, offset)
            else:
                end_time = max(end_time, 130000)
                return bar_count_in_section(start_time, 113000, number * factor, offset) + \
                       bar_count_in_section(130000, end_time, number * factor, offset)
        else:
            raise RuntimeError("Only stock is supported!")

    async def _get_bars_in_day(self, symbol, freq, trade_date, start_time=0, end_time=150000):
        # TODO retry when net error occurs
        trade_date = convert_date_to_int(trade_date) // 1000000
        start_time = max(start_time, 80000)
        end_time = min(end_time, 160000)
        return self._api.bar(symbol=symbol, freq=freq[:-1] + freq[-1].upper(), trade_date=trade_date,
                             start_time=start_time, end_time=end_time)

    def _get_bars_in_day_parallel(self, params):
        loop = asyncio.get_event_loop()
        tasks = [self._get_bars_in_day(**param) for param in params]
        results = loop.run_until_complete(asyncio.gather(*tasks))
        dfs, msgs = zip(*results)
        for msg in msgs:
            if msg and msg != "0,":
                raise RuntimeError(msg)
        return pd.concat(dfs, axis=0)

    def _filtered_dates(self, instrument):
        bars = self._filtered_day_bars(instrument)
        dts = bars["datetime"]
        return dts

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        symbol = instrument_to_tushare(instrument)
        if frequency in ["1d"]:
            if start_dt and end_dt:
                s_date_int = convert_date_to_int(start_dt.date())
                e_date_int = convert_date_to_int(end_dt.date())
            elif start_dt and length:
                dates = self._filtered_dates(instrument)
                s_date_int = convert_date_to_int(start_dt.date())
                s_pos = safe_searchsorted(dates, s_date_int)
                e_date_int = int(dates[min(s_pos + length - 1, len(dates) - 1)])
            elif end_dt and length:
                dates = self._filtered_dates(instrument)
                e_date_int = convert_date_to_int(end_dt.date())
                e_pos = safe_searchsorted(dates, e_date_int)
                s_date_int = int(dates[max(e_pos - length + 1, 0)])
            else:
                raise RuntimeError("At least two of [start_dt,end_dt,length] should be given.")
            data, msg = self._api.daily(symbol, freq=frequency, adjust_mode=None,
                                        start_date=s_date_int // 1000000,
                                        end_date=e_date_int // 1000000)
            if isinstance(data, pd.DataFrame) and data.size:
                data = data[data["volume"] > 0]  # TODO sikp_suspended?
                return QuantOsConverter.df2np(data)
            else:
                if msg:
                    system_log.warning(msg)
                return QuantOsConverter.empty()
        else:
            tasks = []
            base_dict = {
                "symbol": symbol,
                "freq": frequency,
            }
            if start_dt and end_dt:
                s_date, s_time = start_dt.date(), start_dt.time()
                e_date, e_time = end_dt.date(), end_dt.time()
                s_date_int = convert_date_to_int(s_date)
                e_date_int = convert_date_to_int(e_date)
                dates = self._filtered_dates(instrument)
                s_pos = safe_searchsorted(dates, s_date_int)
                e_pos = safe_searchsorted(dates, e_date_int)
                if s_pos == e_pos:
                    tasks.append(dict(trade_date=convert_int_to_date(dates[s_pos]),
                                      start_time=s_time, end_time=e_time,
                                      **base_dict))
                else:
                    tasks.append(dict(trade_date=convert_int_to_date(dates[s_pos]),
                                      start_time=s_time, **base_dict))
                    tasks.extend(map(
                        lambda x: dict(trade_date=convert_int_to_date(x), **base_dict),
                        dates[s_pos + 1: e_pos - 1]))
                    tasks.append(dict(trade_date=convert_int_to_date(dates[e_pos]),
                                      end_time=e_time, **base_dict))
                post_handler = lambda x: x
            elif start_dt and length:
                s_date, s_time = start_dt.date(), int(start_dt.strftime("%H%M%S"))
                dates = self._filtered_dates(instrument)
                s_date_int = convert_date_to_int(s_date)
                s_pos = safe_searchsorted(dates, s_date_int)
                s_bar_count = self.get_bar_count_in_day(instrument, frequency,
                                                        trade_date=s_date, start_time=s_time)
                total_bar_count = self.get_bar_count_in_day(instrument, frequency)
                extra_days = (max(length - s_bar_count, 0) - 1) // total_bar_count + 1
                tasks.append(dict(trade_date=s_date, start_time=s_time, **base_dict))
                tasks.extend(map(
                    lambda x: dict(trade_date=convert_int_to_date(x), **base_dict),
                    dates[s_pos + 1: s_pos + extra_days + 1]))
                post_handler = lambda x: x[:length]
            elif end_dt and length:
                e_date, e_time = end_dt.date(), int(end_dt.strftime("%H%M%S"))
                dates = self._filtered_dates(instrument)
                e_date_int = convert_date_to_int(e_date)
                e_pos = safe_searchsorted(dates, e_date_int)
                e_bar_count = self.get_bar_count_in_day(instrument, frequency,
                                                        trade_date=e_date, end_time=e_time)
                total_bar_count = self.get_bar_count_in_day(instrument, frequency)
                extra_days = (max(length - e_bar_count, 0) - 1) // total_bar_count + 1
                tasks.extend(map(
                    lambda x: dict(trade_date=convert_int_to_date(x), **base_dict),
                    dates[max(e_pos - extra_days, 0): e_pos]))
                tasks.append(dict(trade_date=e_date, end_time=e_time, **base_dict))
                post_handler = lambda x: x[-length:]
            else:
                raise RuntimeError("At least two of [start_dt,end_dt,length] should be given.")
            data = post_handler(self._get_bars_in_day_parallel(tasks))
            if data is not None and data.size:
                return QuantOsConverter.df2np(data)
            else:
                return QuantOsConverter.empty()

    def is_base_frequency(self, instrument, frequency):
        return frequency in ["1d", "1m", "5m", "15m"]

    def current_snapshot(self, instrument, frequency, dt):
        pass

    @lru_cache(maxsize=10)
    def available_data_range(self, frequency):
        return date(2012, 6, 1), date.today() - relativedelta(days=1)


class QuantOsCacheSource(QuantOsSource, CacheMixin):
    def __init__(self, *args, **kwargs):
        super(QuantOsCacheSource, self).__init__(*args, **kwargs)
        CacheMixin.__init__(self)
