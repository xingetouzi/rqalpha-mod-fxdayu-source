import os

__config__ = {
    "source": "mongo",
    "mongo_url": os.environ.get("MONGO_URL", "mongodb://127.0.0.1:27017"),
    "redis_url": os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"),
    "bundle_path": None,
    "enable_cache": True,
    "cache_length": None,
    "max_cache_space": None,
    "fps": 60,
    "persist_path": ".persist",
    "priority": 200,
}


def load_mod():
    from .mod import FxdayuSourceMod
    return FxdayuSourceMod()
