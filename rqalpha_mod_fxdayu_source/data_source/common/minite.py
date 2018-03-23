import asyncio

import numpy as np
import pandas as pd
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.utils.datetime_func import convert_date_to_int, convert_int_to_date
from rqalpha.utils.logger import system_log


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
    assert side in ["left", "right"]
    if not len(a):
        raise RuntimeError("Can't search in a empty array!")
    pos = np.searchsorted(a, v, side=side, sorter=sorter)
    if pos >= len(a):
        system_log.warning(RuntimeWarning(
            "Value to search [%s] beyond array range [ %s - %s ], there may be some data missing."
            % (v, a[0], a[-1])
        ))
        return len(a) - 1 if side == "left" else len(a)
    return pos


class MiniteBarDataSourceMixin(BaseDataSource):
    def _dates_index(self, instrument, skip_suspend=True):
        if skip_suspend:
            bars = self._filtered_day_bars(instrument)
        else:
            bars = self._all_day_bars_of(instrument)
        dts = bars["datetime"]
        return dts

    def _get_bars_in_days(self, instrument, frequency, days):
        raise NotImplementedError

    def _post_handle_bars(self, bars):
        return bars

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
            elif unit == "h":
                offset = 30 * 60  # 30min for A stock
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

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        if frequency[-1] == "m":
            days = []
            if start_dt and end_dt:
                assert start_dt <= end_dt, "start datetime later then end datetime!"
                s_date, s_time = start_dt.date(), start_dt.time()
                e_date, e_time = end_dt.date(), end_dt.time()
                s_date_int = convert_date_to_int(s_date)
                e_date_int = convert_date_to_int(e_date)
                dates = self._dates_index(instrument)
                s_pos = safe_searchsorted(dates, s_date_int)
                e_pos = safe_searchsorted(dates, e_date_int, side="right") - 1
                if s_pos == e_pos:
                    days.append(dict(
                        trade_date=convert_int_to_date(dates[s_pos]),
                        start_time=s_time, end_time=e_time,
                    ))
                else:
                    days.append(dict(trade_date=convert_int_to_date(dates[s_pos]), start_time=s_time))
                    days.extend(map(
                        lambda x: dict(trade_date=convert_int_to_date(x)),
                        dates[s_pos + 1: e_pos]))
                    days.append(dict(trade_date=convert_int_to_date(dates[e_pos]), end_time=e_time))
                post_handler = lambda x: x
            elif start_dt and length:
                s_date, s_time = start_dt.date(), int(start_dt.strftime("%H%M%S"))
                dates = self._dates_index(instrument)
                s_date_int = convert_date_to_int(s_date)
                s_pos = safe_searchsorted(dates, s_date_int)
                s_bar_count = self.get_bar_count_in_day(instrument, frequency,
                                                        trade_date=s_date, start_time=s_time)
                total_bar_count = self.get_bar_count_in_day(instrument, frequency)
                extra_days = (max(length - s_bar_count, 0) - 1) // total_bar_count + 1
                days.append(dict(trade_date=s_date, start_time=s_time))
                days.extend(map(
                    lambda x: dict(trade_date=convert_int_to_date(x)),
                    dates[s_pos + 1: s_pos + 1 + extra_days]))
                post_handler = lambda x: x[:length]
            elif end_dt and length:
                e_date, e_time = end_dt.date(), int(end_dt.strftime("%H%M%S"))
                dates = self._dates_index(instrument)
                e_date_int = convert_date_to_int(e_date)
                e_pos = safe_searchsorted(dates, e_date_int, side="right") - 1
                e_bar_count = self.get_bar_count_in_day(instrument, frequency,
                                                        trade_date=e_date, end_time=e_time)
                total_bar_count = self.get_bar_count_in_day(instrument, frequency)
                extra_days = (max(length - e_bar_count, 0) - 1) // total_bar_count + 1
                days.extend(map(
                    lambda x: dict(trade_date=convert_int_to_date(x)),
                    dates[max(e_pos - extra_days, 0): e_pos]))
                days.append(dict(trade_date=e_date, end_time=e_time))
                post_handler = lambda x: x[-length:]
            else:
                raise RuntimeError("At least two of [start_dt,end_dt,length] should be given.")
            data = post_handler(self._get_bars_in_days(instrument, frequency, days))
            return data
        else:
            return None
