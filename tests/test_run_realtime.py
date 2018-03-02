# -*- coding: utf-8 -*-

from rqalpha.api import *
from rqalpha import run_func
import pandas as pd


def init(context):
    logger.info("init")
    context.s1 = "000001.XSHE"
    update_universe(context.s1)
    context.fired = False


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    logger.info(bar_dict[context.s1])
    print(bar_dict[context.s1])
    # print(pd.DataFrame(history_bars(context.s1, 5, "1d", include_now=True)))
    print(pd.DataFrame(history_bars(context.s1, 5, "5m")))
    print(pd.DataFrame(history_bars(context.s1, 5, "5m", include_now=True)))
    if not context.fired:
        # order_percent并且传入1代表买入该股票并且使其占有投资组合的100%
        order_percent(context.s1, 1)
        context.fired = True
    else:
        order_percent(context.s1, 0)
        context.fired = False


config = {
    "base": {
        "start_date": "2016-06-01",
        "end_date": "2016-06-05",
        "accounts": {"stock": 100000},
        "frequency": "1m",
        "benchmark": None,
        "data_bundle_path": r"E:\Users\BurdenBear\.rqalpha\bundle",
        "strategy_file": __file__,
        "run_type": "p"
    },
    "extra": {
        "log_level": "verbose",
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            # "report_save_path": ".",
            "plot": True
        },
        "fxdayu_source": {
            "enabled": True,
            "mongo_url": "mongodb://192.168.0.101:27017",
            "redis_url": "redis://192.168.0.102:6379"
        }
    }
}

# 您可以指定您要传递的参数
run_func(init=init, before_trading=before_trading, handle_bar=handle_bar, config=config)

# 如果你的函数命名是按照 API 规范来，则可以直接按照以下方式来运行
# run_func(**globals())
