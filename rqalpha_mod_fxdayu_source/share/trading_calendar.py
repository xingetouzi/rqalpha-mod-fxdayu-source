# encoding: utf-8

import os
from datetime import time
from lru import LRU

import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset
from pytz import timezone
from rqalpha.data.base_data_source import BaseDataSource
from zipline.utils.calendars import register_calendar
from zipline.utils.calendars.trading_calendar import TradingCalendar, days_at_time

RQALPHA_ROOT = os.environ.get("RQALPHA_ROOT", "~/.rqalpha")
RQALPHA_BUNDLE_PATH = os.path.join(RQALPHA_ROOT, "bundle")
_CALENDAR_NAME = "ASTOCK"

start_default = pd.Timestamp('2012-05-01', tz='UTC')
end_base = pd.Timestamp('today', tz='UTC')
# Give an aggressive buffer for logic that needs to use the next trading
# day or minute.
end_default = end_base + pd.Timedelta(days=365)


class RqalphaAStockTradingCalendar(TradingCalendar):
    def __init__(self, start=start_default, end=end_default, path=RQALPHA_BUNDLE_PATH):
        super(RqalphaAStockTradingCalendar, self).__init__()
        self._data_source = BaseDataSource(path)
        _all_days = self._data_source.get_trading_calendar()
        _all_days = _all_days[_all_days.slice_indexer(start, end)]
        # `DatetimeIndex`s of standard opens/closes for each day.
        self._opens = days_at_time(_all_days, self.open_time, self.tz,
                                   self.open_offset)
        self._closes = days_at_time(
            _all_days, self.close_time, self.tz, self.close_offset
        )

        # In pandas 0.16.1 _opens and _closes will lose their timezone
        # information. This looks like it has been resolved in 0.17.1.
        # http://pandas.pydata.org/pandas-docs/stable/whatsnew.html#datetime-with-tz  # noqa
        self.schedule = pd.DataFrame(
            index=_all_days,
            columns=['market_open', 'market_close'],
            data={
                'market_open': self._opens,
                'market_close': self._closes,
            },
            dtype='datetime64[ns]',
        )

        # Simple cache to avoid recalculating the same minute -> session in
        # "next" mode. Analysis of current zipline code paths show that
        # `minute_to_session_label` is often called consecutively with the same
        # inputs.
        self._minute_to_session_label_cache = LRU(1)

        self.market_opens_nanos = self.schedule.market_open.values. \
            astype(np.int64)

        self.market_closes_nanos = self.schedule.market_close.values. \
            astype(np.int64)

        self._trading_minutes_nanos = self.all_minutes.values. \
            astype(np.int64)

        self.first_trading_session = _all_days[0]
        self.last_trading_session = _all_days[-1]

    @property
    def name(self):
        return _CALENDAR_NAME

    @property
    def tz(self):
        return timezone("UTC")

    @property
    def open_time(self):
        return time(9, 31)

    @property
    def close_time(self):
        return time(15, 00)


ASTOCK_TRADING_CALENDAR = RqalphaAStockTradingCalendar()
register_calendar(_CALENDAR_NAME, ASTOCK_TRADING_CALENDAR)
