from datetime import date

import asyncio
import pandas as pd
from dateutil.relativedelta import relativedelta
from rqalpha.utils import lru_cache
from rqalpha.utils.datetime_func import convert_date_to_int
from rqalpha.utils.logger import system_log

from rqalpha_mod_fxdayu_source.data_source.common import CacheMixin
from rqalpha_mod_fxdayu_source.data_source.common.minite import safe_searchsorted, MiniteBarDataSourceMixin
from rqalpha_mod_fxdayu_source.data_source.common.odd import OddFrequencyBaseDataSource
from rqalpha_mod_fxdayu_source.utils import Singleton
from rqalpha_mod_fxdayu_source.utils.converter import QuantOsConverter
from rqalpha_mod_fxdayu_source.utils.instrument import instrument_to_tushare
from rqalpha_mod_fxdayu_source.utils.quantos import QuantOsDataApiMixin


class QuantOsSource(OddFrequencyBaseDataSource, MiniteBarDataSourceMixin, QuantOsDataApiMixin):
    __metaclass__ = Singleton

    def __init__(self, path, api_url=None, user=None, token=None):
        super(QuantOsSource, self).__init__(path)
        QuantOsDataApiMixin.__init__(self, api_url, user, token)

    async def _get_bars_in_day(self, instrument=None, frequency=None, trade_date=None, start_time=0, end_time=150000):
        # TODO retry when net error occurs
        symbol = instrument_to_tushare(instrument)
        trade_date = convert_date_to_int(trade_date) // 1000000
        start_time = max(start_time, 80000)
        end_time = min(end_time, 160000)
        return self._api.bar(symbol=symbol, freq=frequency[:-1] + frequency[-1].upper(),
                             trade_date=trade_date, start_time=start_time, end_time=end_time)

    def _get_bars_in_days(self, instrument, frequency, days):
        loop = asyncio.get_event_loop()
        tasks = [self._get_bars_in_day(instrument=instrument, frequency=frequency, **day) for day in days]
        results = loop.run_until_complete(asyncio.gather(*tasks))
        dfs, msgs = zip(*results)
        for msg in msgs:
            if msg and msg != "0,":
                raise RuntimeError(msg)
        bars = pd.concat(dfs, axis=0)
        if bars is not None and bars.size:
            return QuantOsConverter.df2np(bars)
        else:
            return QuantOsConverter.empty()

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        symbol = instrument_to_tushare(instrument)
        if frequency in ["1d"]:
            if start_dt and end_dt:
                s_date_int = convert_date_to_int(start_dt.date())
                e_date_int = convert_date_to_int(end_dt.date())
            elif start_dt and length:
                dates = self._dates_index(instrument)
                s_date_int = convert_date_to_int(start_dt.date())
                s_pos = safe_searchsorted(dates, s_date_int)
                s_date_int = int(dates[s_pos])
                e_date_int = int(dates[min(s_pos + length, len(dates)) - 1])
            elif end_dt and length:
                dates = self._dates_index(instrument)
                e_date_int = convert_date_to_int(end_dt.date())
                e_pos = safe_searchsorted(dates, e_date_int, side="right")
                s_date_int = int(dates[max(e_pos - length, 0)])
                e_date_int = int(dates[e_pos - 1])
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
            return MiniteBarDataSourceMixin.raw_history_bars(
                self, instrument, frequency, start_dt=start_dt, end_dt=end_dt, length=length
            )

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
