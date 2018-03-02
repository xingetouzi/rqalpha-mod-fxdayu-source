from enum import Enum


class DataSourceType(Enum):
    MONGO = "mongo"
    BUNDLE = "bundle"
    QUANTOS = "quantos"
    REAL_TIME = "real_time"
