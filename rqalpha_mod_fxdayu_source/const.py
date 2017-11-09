from enum import Enum


class DataSourceType(Enum):
    MONGO = "mongo"
    BUNDLE = "bundle"
    REAL_TIME = "real_time"
