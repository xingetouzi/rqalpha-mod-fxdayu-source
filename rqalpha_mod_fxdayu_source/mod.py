# encoding: utf-8
from rqalpha.const import RUN_TYPE, PERSIST_MODE
from rqalpha.interface import AbstractMod
from rqalpha.mod.rqalpha_mod_sys_stock_realtime.direct_data_source import DirectDataSource
from rqalpha.utils.disk_persist_provider import DiskPersistProvider
from rqalpha.utils.logger import user_system_log, system_log
from rqalpha.utils.i18n import gettext as _

from rqalpha_mod_fxdayu_source.const import DataSourceType
from rqalpha_mod_fxdayu_source.data.bundle import BundleCacheDataSource, BundleDataSource
from rqalpha_mod_fxdayu_source.data.mongo import MongoDataSource, MongoCacheDataSource
from rqalpha_mod_fxdayu_source.event_source import IntervalEventSource, RealTimeEventSource
from rqalpha_mod_fxdayu_source.module.cache import CacheMixin
from rqalpha_mod_fxdayu_source.data.redis import RedisDataSource


class FxdayuSourceMod(AbstractMod):
    def __init__(self):
        self._old_cache_length = CacheMixin.CACHE_LENGTH
        self._old_max_cache_space = CacheMixin.MAX_CACHE_SPACE

    def start_up(self, env, mod_config):
        type_ = DataSourceType(mod_config.source)
        if type_ == DataSourceType.MONGO:
            args = (env.config.base.data_bundle_path, mod_config.mongo_url)
            data_source_cls = MongoCacheDataSource if mod_config.enable_cache else MongoDataSource
        elif type_ == DataSourceType.BUNDLE:
            args = (env.config.base.data_bundle_path, mod_config.bundle_path)
            data_source_cls = BundleCacheDataSource if mod_config.enable_cache else BundleDataSource
        else:
            raise RuntimeError("data source type [%s] is not supported" % mod_config.source)
        if mod_config.enable_cache:
            if mod_config.cache_length:
                CacheMixin.set_cache_length(int(mod_config.cache_length))
            if mod_config.max_cache_space:
                CacheMixin.set_cache_length(int(mod_config.cache_length))
        data_source = data_source_cls(*args)
        mod_config.redis_uri = mod_config.redis_url  # fit rqalpha
        if env.config.base.run_type in (RUN_TYPE.PAPER_TRADING, RUN_TYPE.LIVE_TRADING):
            user_system_log.warn(_("[Warning] When you use this version of RealtimeTradeMod, history_bars can only "
                                   "get data from yesterday."))

            if mod_config.redis_url:
                data_source = RedisDataSource(env.config.base.data_bundle_path, mod_config.redis_url,
                                              datasource=data_source)
                system_log.info(_("RealtimeTradeMod using market from redis"))
            else:
                data_source = DirectDataSource(env.config.base.data_bundle_path)
                system_log.info(_("RealtimeTradeMod using market from network"))
            event_source = RealTimeEventSource(mod_config.fps, mod_config)
            # add persist
            persist_provider = DiskPersistProvider(mod_config.persist_path)
            env.set_persist_provider(persist_provider)

            env.config.base.persist = True
            env.config.base.persist_mode = PERSIST_MODE.REAL_TIME
        else:
            event_source = IntervalEventSource(env)
        env.set_data_source(data_source)
        env.set_event_source(event_source)

    def tear_down(self, code, exception=None):
        CacheMixin.set_cache_length(self._old_cache_length)
        CacheMixin.set_max_cache_space(self._old_max_cache_space)
