# encoding: utf-8

from rqalpha.interface import AbstractMod

from rqalpha_mod_mongo_datasource.data_source import MongoDataSource, MongoCacheDataSource


class MongoDataMod(AbstractMod):
    def __init__(self):
        pass

    def start_up(self, env, mod_config):
        mongo_url = mod_config.mongo_url
        if mod_config.enable_cache:
            if mod_config.cache_length:
                source = MongoCacheDataSource(env.config.base.data_bundle_path, mongo_url, int(mod_config.cache_length))
            else:
                source = MongoCacheDataSource(env.config.base.data_bundle_path, mongo_url)
            source.init_cache()  # 为了支持单例模式下多次运行
        else:
            source = MongoDataSource(env.config.base.data_bundle_path, mongo_url)
        env.set_data_source(source)

    def tear_down(self, code, exception=None):
        pass
