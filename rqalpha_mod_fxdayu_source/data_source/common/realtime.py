from datetime import datetime, time

import numpy as np
from rqalpha.environment import Environment
from rqalpha.utils.datetime_func import convert_dt_to_int, convert_date_to_date_int

from rqalpha_mod_fxdayu_source.data_source.common import OddFrequencyDataSource
from rqalpha_mod_fxdayu_source.data_source.common.odd import CompleteAbstractDataSource

EMPTY_BARS = None


class RealtimeDataSource(OddFrequencyDataSource, CompleteAbstractDataSource):
    def is_suspended(self, order_book_id, dates):
        return self._hist_source.is_suspended(order_book_id, dates)

    def is_st_stock(self, order_book_id, dates):
        return self._hist_source.is_st_stock(order_book_id, dates)

    def get_trading_calendar(self):
        return self._hist_source.get_trading_calendar()

    def get_trading_minutes_for(self, instrument, trading_dt):
        return self._hist_source.get_trading_minutes_for(instrument, trading_dt)

    def get_all_instruments(self):
        return self._hist_source.get_all_instruments()

    def get_merge_ticks(self, order_book_id_list, trading_date, last_dt=None):
        return self._hist_source.get_merge_ticks(order_book_id_list, trading_date, last_dt)

    def current_snapshot(self, instrument, frequency, dt):
        return self._hist_source.current_snapshot(instrument, frequency, dt)

    def get_yield_curve(self, start_date, end_date, tenor=None):
        return self._hist_source.get_yield_curve(start_date, end_date, tenor)

    def get_settle_price(self, instrument, date):
        return self._hist_source.get_settle_price(instrument, date)

    def get_margin_info(self, instrument):
        return self._hist_source.get_margin_info(instrument)

    def get_split(self, order_book_id):
        return self._hist_source.get_split(order_book_id)

    def get_commission_info(self, instrument):
        return self._hist_source.get_commission_info(instrument)

    def get_dividend(self, order_book_id):
        return self._hist_source.get_dividend(order_book_id)

    def get_ex_cum_factor(self, order_book_id):
        return self._hist_source.get_ex_cum_factor(order_book_id)

    def __init__(self, inday_bars, hist_source):
        """

        Parameters
        ----------
        inday_bars:
        hist_source: rqalpha_mod_fxdayu_source.data_source.common.OddFrequencyBaseDataSource
        """
        super(RealtimeDataSource, self).__init__()
        self._inday_bars = inday_bars
        self._hist_source = hist_source

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        env = Environment.get_instance()
        now = env.calendar_dt
        today = now.date()
        today_int = convert_date_to_date_int(today)
        yesterday = datetime.combine(env.data_proxy.get_previous_trading_date(today),
                                     time=time(hour=23, minute=59, second=59))
        history_bars = EMPTY_BARS
        today_bars = EMPTY_BARS
        if end_dt and start_dt:
            end_dt = min(now, end_dt)
            if start_dt > end_dt:
                return EMPTY_BARS
            if end_dt.date == today:
                start_time = convert_dt_to_int(start_dt) % 1000000 if start_dt.date() == today else None
                end_time = convert_dt_to_int(end_dt) % 1000000
                today_bars = self._inday_bars.bars(instrument, frequency, today_int,
                                                   start_time, end_time)
            if start_dt.date() < today:
                history_bars = self._hist_source.raw_history_bars(
                    instrument, frequency,
                    start_dt=start_dt,
                    end_dt=min(end_dt, yesterday)
                )
        elif start_dt and length:
            if start_dt.date() > today:
                return EMPTY_BARS
            if start_dt.date() < today:
                history_bars = self._hist_source.raw_history_bars(
                    instrument, frequency, start_dt=start_dt, length=length)
            left = length - len(history_bars) if history_bars is not None else length
            start_time = convert_dt_to_int(start_dt) % 1000000 if start_dt.date() == today else None
            today_bars = self._inday_bars.get_bars(instrument, frequency,
                                                   today_int, start_time)[:left]
        elif end_dt and length:
            end_dt = min(now, end_dt)
            if end_dt.date() == today:
                end_time = convert_dt_to_int(end_dt) % 1000000
                today_bars = self._inday_bars.get_bars(instrument, frequency, today_int,
                                                       end_time=end_time)[-length:]
            left = length - len(today_bars) if today_bars is not None else length
            if left > 0:
                history_bars = self._hist_source.raw_history_bars(
                    instrument, frequency, end_dt=min(end_dt, yesterday), length=left)
        else:
            raise RuntimeError
        if history_bars is not None and today_bars is not None:
            return np.concatenate([history_bars, today_bars])
        elif history_bars is not None:
            return history_bars
        else:
            return today_bars

    # TODO logic of include_now was write in OddFrequencyDataSource and only support 1X frequencies
    # def is_base_frequency(self, instrument, freq):
    #     return self._hist_source.is_base_frequency(instrument, freq)

    def available_data_range(self, frequency):
        start, end = self._hist_source.available_data_range(frequency)
        end = datetime.now().date()
        return start, end
