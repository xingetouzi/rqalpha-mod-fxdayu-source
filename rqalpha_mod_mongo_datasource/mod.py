# encoding: utf-8

from rqalpha.interface import AbstractMod

from rqalpha_mod_mongo_datasource.const import DataSourceType
from rqalpha_mod_mongo_datasource.data.bundle import BundleCacheDataSource, BundleDataSource
from rqalpha_mod_mongo_datasource.data.mongo import MongoDataSource, MongoCacheDataSource
from rqalpha_mod_mongo_datasource.event_source import IntervalEventSource
from rqalpha_mod_mongo_datasource.module.cache import CacheMixin


class MongoDataMod(AbstractMod):
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
        event_source = IntervalEventSource(env)
        env.set_data_source(data_source)
        env.set_event_source(event_source)

    def tear_down(self, code, exception=None):
        CacheMixin.set_cache_length(self._old_cache_length)
        CacheMixin.set_max_cache_space(self._old_max_cache_space)
