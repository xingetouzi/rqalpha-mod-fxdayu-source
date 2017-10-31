# encoding: utf-8
import time
import datetime
import re
from itertools import islice

from rqalpha.const import DEFAULT_ACCOUNT_TYPE
from rqalpha.events import Event, EVENT
from rqalpha.mod.rqalpha_mod_sys_simulation.simulation_event_source import SimulationEventSource
from rqalpha.mod.rqalpha_mod_sys_stock_realtime.event_source import RealtimeEventSource
from rqalpha.mod.rqalpha_mod_sys_stock_realtime.utils import is_holiday_today, is_tradetime_now
from rqalpha.utils.i18n import gettext as _

from rqalpha_mod_fxdayu_source.utils import InDayTradingPointIndexer

_unit_freq_template = re.compile("[^0-9]+")
_freq_template = re.compile("[0-9]+(?P<freq1>h|m|d)|(?P<freq2>tick)")


class IntervalEventSource(SimulationEventSource):
    def __init__(self, env):
        super(IntervalEventSource, self).__init__(env)
        self._indexer = InDayTradingPointIndexer()

    def _get_trading_points(self, trading_date, frequency):
        indexer = self._indexer
        trading_points = set()
        for account_type in self._config.base.accounts:
            if account_type == DEFAULT_ACCOUNT_TYPE.STOCK.name:
                trading_points.update(indexer.get_a_stock_trading_points(trading_date, frequency))
            elif account_type == DEFAULT_ACCOUNT_TYPE.FUTURE.name:
                trading_points.update(indexer.get_future_trading_points(self._env, trading_date, frequency))
        return sorted(list(trading_points))

    def _get_events_for_d(self, start_date, end_date, frequency):
        num = int(frequency[:-1])
        for day in islice(self._env.data_proxy.get_trading_dates(start_date, end_date), None, None, num):
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


class RealTimeEventSource(RealtimeEventSource):
    def clock_worker(self):
        while True:
            # time.sleep(self.fps)

            if is_holiday_today():
                time.sleep(60)
                continue

            dt = datetime.datetime.now()

            if dt.strftime("%H:%M:%S") >= "08:30:00" and dt.date() > self.before_trading_fire_date:
                self.event_queue.put((dt, EVENT.BEFORE_TRADING))
                self.before_trading_fire_date = dt.date()
            elif dt.strftime("%H:%M:%S") >= "15:10:00" and dt.date() > self.after_trading_fire_date:
                self.event_queue.put((dt, EVENT.AFTER_TRADING))
                self.after_trading_fire_date = dt.date()
            elif dt.strftime("%H:%M:%S") >= "15:10:00" and dt.date() > self.settlement_fire_date:
                self.event_queue.put((dt, EVENT.SETTLEMENT))
                self.settlement_fire_date = dt.date()

            if is_tradetime_now():
                self.event_queue.put((dt, EVENT.BAR))

            time.sleep(self.fps)
