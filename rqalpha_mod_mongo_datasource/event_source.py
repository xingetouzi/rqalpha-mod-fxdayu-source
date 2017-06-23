import datetime
import re

import numpy as np
import pandas as pd
from rqalpha.const import ACCOUNT_TYPE
from rqalpha.events import Event, EVENT
from rqalpha.interface import AbstractEventSource
from rqalpha.mod.rqalpha_mod_sys_simulation.simulation_event_source import SimulationEventSource
from rqalpha.utils import get_account_type
from rqalpha.utils.datetime_func import convert_int_to_datetime
from rqalpha.utils.i18n import gettext as _

_unit_freq_template = re.compile("[^0-9]+")
_freq_template = re.compile("[0-9]+(?P<freq1>h|m|d)|(?P<freq2>tick)")
_freq_map = {
    "m": "T",
    "h": "H",
    "d": "D"
}


def _date_range(start, end, freq):
    unit_freq = freq[-1]
    dates = pd.date_range(start, end, freq=freq[:-1] + _freq_map[unit_freq]) - pd.Timedelta(minutes=1)
    dates = dates.to_pydatetime()
    if dates.size:
        dates = dates[1:]
    if not dates.size or dates[-1] != end:
        dates = np.concatenate([dates, [end]])
    return dates


class IntervalEventSource(SimulationEventSource):
    def __init__(self, env, account_list):
        super(IntervalEventSource, self).__init__(env, account_list)

    @staticmethod
    def _get_stock_trading_points(trading_date, frequency):
        trading_points = set()
        current_dt = datetime.datetime.combine(trading_date, datetime.time(9, 31))
        am_end_dt = current_dt.replace(hour=11, minute=30)
        pm_start_dt = current_dt.replace(hour=13, minute=1)
        pm_end_dt = current_dt.replace(hour=15, minute=0)
        sessions = [(current_dt, am_end_dt), (pm_start_dt, pm_end_dt)]
        for start, end in sessions:
            trading_points.update(_date_range(start, end, frequency))
        return trading_points

    def _get_future_trading_points(self, trading_date, frequency):
        if frequency == "1m":
            trading_minutes = set()
            universe = self._get_universe()
            for order_book_id in universe:
                if get_account_type(order_book_id) == ACCOUNT_TYPE.STOCK:
                    continue
                trading_minutes.update(self._env.data_proxy.get_trading_minutes_for(order_book_id, trading_date))
            return set([convert_int_to_datetime(minute) for minute in trading_minutes])
        # TODO future hours
        return set()

    def _get_trading_points(self, trading_date, frequency):
        trading_hours = set()
        for account_type in self._account_list:
            if account_type == ACCOUNT_TYPE.STOCK:
                trading_hours.update(self._get_stock_trading_hours(trading_date, frequency))
            elif account_type == ACCOUNT_TYPE.FUTURE:
                trading_hours.update(self._get_future_trading_hours(trading_date, frequency))
        return sorted(list())

    def _get_events_for_d(self, start_date, end_date, frequency):
        num = int(frequency[:-1])
        count = 0
        for day in self._env.data_proxy.get_trading_dates(start_date, end_date):
            date = day.to_pydatetime()
            dt_before_trading = date.replace(hour=0, minute=0)
            dt_bar = date.replace(hour=15, minute=0)
            dt_after_trading = date.replace(hour=15, minute=30)
            dt_settlement = date.replace(hour=17, minute=0)
            yield Event(EVENT.BEFORE_TRADING, calendar_dt=dt_before_trading, trading_dt=dt_before_trading)
            yield Event(EVENT.BAR, calendar_dt=dt_bar, trading_dt=dt_bar)

            yield Event(EVENT.AFTER_TRADING, calendar_dt=dt_after_trading, trading_dt=dt_after_trading)
            yield Event(EVENT.SETTLEMENT, calendar_dt=dt_settlement, trading_dt=dt_settlement)

    def _get_events_in_day(self, start_date, end_date, frequency):
        for day in self._env.data_proxy.get_trading_dates(start_date, end_date):
            before_trading_flag = True
            date = day.to_pydatetime()
            last_dt = None
            done = False

            dt_before_day_trading = date.replace(hour=8, minute=30)

            while True:
                if done:
                    break
                exit_loop = True
                trading_points = self._get_trading_points(date, frequency)
                for calendar_dt in trading_points:
                    if last_dt is not None and calendar_dt < last_dt:
                        continue

                    if calendar_dt < dt_before_day_trading:
                        trading_dt = calendar_dt.replace(year=date.year,
                                                         month=date.month,
                                                         day=date.day)
                    else:
                        trading_dt = calendar_dt
                    if before_trading_flag:
                        before_trading_flag = False
                        yield Event(EVENT.BEFORE_TRADING,
                                    calendar_dt=calendar_dt - datetime.timedelta(minutes=30),
                                    trading_dt=trading_dt - datetime.timedelta(minutes=30))
                    if self._universe_changed:
                        self._universe_changed = False
                        last_dt = calendar_dt
                        exit_loop = False
                        break
                    # yield handle bar
                    yield Event(EVENT.BAR, calendar_dt=calendar_dt, trading_dt=trading_dt)
                if exit_loop:
                    done = True

            dt = date.replace(hour=15, minute=30)
            yield Event(EVENT.AFTER_TRADING, calendar_dt=dt, trading_dt=dt)

            dt = date.replace(hour=17, minute=0)
            yield Event(EVENT.SETTLEMENT, calendar_dt=dt, trading_dt=dt)

    def _get_events_for_h(self, start_date, end_date, frequency):
        return self._get_events_in_day(start_date, end_date, frequency)

    def _get_events_for_m(self, start_date, end_date, frequency):
        return self._get_events_in_day(start_date, end_date, frequency)

    def _get_events_for_tick(self, start_date, end_date, frequency):
        data_proxy = self._env.data_proxy
        for day in data_proxy.get_trading_dates(start_date, end_date):
            date = day.to_pydatetime()
            last_tick = None
            last_dt = None
            dt_before_day_trading = date.replace(hour=8, minute=30)
            while True:
                for tick in data_proxy.get_merge_ticks(self._get_universe(), date, last_dt):
                    # find before trading time
                    if last_tick is None:
                        last_tick = tick
                        dt = tick.datetime
                        before_trading_dt = dt - datetime.timedelta(minutes=30)
                        yield Event(EVENT.BEFORE_TRADING, calendar_dt=before_trading_dt,
                                    trading_dt=before_trading_dt)

                    dt = tick.datetime

                    if dt < dt_before_day_trading:
                        trading_dt = dt.replace(year=date.year, month=date.month, day=date.day)
                    else:
                        trading_dt = dt

                    yield Event(EVENT.TICK, calendar_dt=dt, trading_dt=trading_dt, tick=tick)

                    if self._universe_changed:
                        self._universe_changed = False
                        last_dt = dt
                        break
                else:
                    break

            dt = date.replace(hour=15, minute=30)
            yield Event(EVENT.AFTER_TRADING, calendar_dt=dt, trading_dt=dt)

            dt = date.replace(hour=17, minute=0)
            yield Event(EVENT.SETTLEMENT, calendar_dt=dt, trading_dt=dt)

    def events(self, start_date, end_date, frequency):
        try:
            result = _freq_template.match(frequency)
            freq = result.group("freq1") or result.group("freq2")
            return getattr(self, "_get_events_for_" + freq)(start_date, end_date, frequency)
        except Exception:
            raise NotImplementedError(_("Frequency {} is not support.").format(frequency))
