# encoding: utf-8
from datetime import datetime

from rqalpha.const import RUN_TYPE, PERSIST_MODE
from rqalpha.interface import AbstractMod
from rqalpha.mod.rqalpha_mod_sys_stock_realtime.direct_data_source import DirectDataSource
from rqalpha.utils.disk_persist_provider import DiskPersistProvider
from rqalpha.utils.i18n import gettext as _
from rqalpha.utils.logger import user_system_log, system_log

from rqalpha_mod_fxdayu_source.const import DataSourceType
from rqalpha_mod_fxdayu_source.data_source.common import CacheMixin
from rqalpha_mod_fxdayu_source.data_source.redis import RedisDataSource
from rqalpha_mod_fxdayu_source.event_source import IntervalEventSource, RealTimeEventSource
from rqalpha_mod_fxdayu_source.price_board import StockLimitUpDownPriceBoard


class FxdayuSourceMod(AbstractMod):
    def __init__(self):
        self._old_cache_length = CacheMixin.CACHE_LENGTH
        self._old_max_cache_space = CacheMixin.MAX_CACHE_SPACE

    def start_up(self, env, mod_config):
        env.set_price_board(StockLimitUpDownPriceBoard())
        type_ = DataSourceType(mod_config.source)
        if type_ in [DataSourceType.MONGO, DataSourceType.REAL_TIME]:
            from rqalpha_mod_fxdayu_source.data_source.mongo import MongoDataSource, MongoCacheDataSource
            args = (env.config.base.data_bundle_path, mod_config.mongo_url)
            data_source_cls = MongoCacheDataSource if mod_config.enable_cache else MongoDataSource
        elif type_ == DataSourceType.BUNDLE:
            from rqalpha_mod_fxdayu_source.data_source.bundle import BundleCacheDataSource, BundleDataSource
            args = (env.config.base.data_bundle_path, mod_config.bundle_path)
            data_source_cls = BundleCacheDataSource if mod_config.enable_cache else BundleDataSource
        elif type_ == DataSourceType.QUANTOS:
            from rqalpha_mod_fxdayu_source.data_source.quantos import QuantOsSource, QuantOsCacheSource
            args = (env.config.base.data_bundle_path, mod_config.quantos_url,
                    mod_config.quantos_user, mod_config.quantos_token)
            data_source_cls = QuantOsCacheSource if mod_config.enable_cache else QuantOsSource
        else:
            raise RuntimeError("data source type [%s] is not supported" % mod_config.source)
        if mod_config.enable_cache:
            if mod_config.cache_length:
                CacheMixin.set_cache_length(int(mod_config.cache_length))
            if mod_config.max_cache_space:
                CacheMixin.set_max_cache_space(int(mod_config.max_cache_space))
        data_source = data_source_cls(*args)
        mod_config.redis_uri = mod_config.redis_url  # fit rqalpha
        if env.config.base.run_type is RUN_TYPE.BACKTEST and env.config.base.persist_mode == PERSIST_MODE.ON_NORMAL_EXIT:
            # generate user context using backtest
            persist_provider = DiskPersistProvider(mod_config.persist_path)
            env.set_persist_provider(persist_provider)

        is_real_time = env.config.base.run_type in (RUN_TYPE.PAPER_TRADING, RUN_TYPE.LIVE_TRADING)
        if is_real_time or type_ == DataSourceType.REAL_TIME:
            user_system_log.warn(_("[Warning] When you use this version of RealtimeTradeMod, history_bars can only "
                                   "get data from yesterday."))
            if mod_config.redis_url:
                data_source = RedisDataSource(env.config.base.data_bundle_path, mod_config.redis_url,
                                              datasource=data_source)
                system_log.info(_("RealtimeTradeMod using market from redis"))
            else:
                data_source = DirectDataSource(env.config.base.data_bundle_path)
                system_log.info(_("RealtimeTradeMod using market from network"))
        if is_real_time:
            event_source = RealTimeEventSource(mod_config.fps, mod_config)
            # add persist
            persist_provider = DiskPersistProvider(mod_config.persist_path)
            env.set_persist_provider(persist_provider)

            env.config.base.persist = True
            env.config.base.persist_mode = PERSIST_MODE.REAL_TIME
        else:
            event_source = IntervalEventSource(env)
        env.set_data_source(data_source)
        # a patch to start_date since it's real time mod
        if env.config.base.start_date == datetime.now().date():
            trading_dates = data_source.get_trading_calendar()
            pos = trading_dates.searchsorted(env.config.base.start_date)
            if trading_dates[pos].to_pydatetime().date() != env.config.base.start_date:
                env.config.base.start_date = trading_dates[max(0, pos - 1)].to_pydatetime().date()
        env.set_event_source(event_source)

    def tear_down(self, code, exception=None):
        CacheMixin.set_cache_length(self._old_cache_length)
        CacheMixin.set_max_cache_space(self._old_max_cache_space)
