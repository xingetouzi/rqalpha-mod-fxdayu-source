# encoding:utf-8

import talib as ta
from rqalpha import run_file
from rqalpha.api import *

frequency = "1m"
report_path = ".report"


def calculate(close, period):
    mas = ta.MA(close, period)  # 第三发
    mal = ta.MA(close, 5 * period)
    if mas[-1] > mal[-1]:
        return 1
    else:
        return 0


def init(context):
    context.s1 = "000001.XSHE"
    context.PERIOD = 80
    context.stoplossmultipler = 0.97
    context.takepofitmultipler = 4
    scheduler.run_daily(run_daily)


def statistic(close):
    count = 0
    for i in range(1, 21):
        count += calculate(close[-i * 5:], i)
    return count * 5


def run_daily(context, bar_dict):
    print("run daily :{}".format(context.now))


def handle_bar(context, bar_dict):
    print("heatbeat: {}".format(context.now))
    stop_loss(context, bar_dict)
    entry_exit(context, bar_dict)


def entry_exit(context, bar_dict):
    close = history_bars(context.s1, context.PERIOD + 1, frequency, 'close')
    if len(close) == context.PERIOD + 1:
        ma_statistic1 = statistic(close[:-1])
        ma_statistic0 = statistic(close[1:])
        cur_position = context.portfolio.positions[context.s1].quantity
        shares = context.portfolio.cash / bar_dict[context.s1].close
        if ma_statistic1 > 50 > ma_statistic0 and cur_position > 0:
            order_target_value(context.s1, 0)
        if ma_statistic1 < 65 < ma_statistic0 and cur_position == 0:
            order_shares(context.s1, shares)


def stop_loss(context, bar_dict):
    for stock in context.portfolio.positions:
        avg_price = context.portfolio.positions[stock].avg_price
        if bar_dict[stock].last < avg_price * context.stoplossmultipler:
            order_target_percent(stock, 0)
        elif bar_dict[stock].last > avg_price * context.takepofitmultipler:
            order_target_percent(stock, 0)


config = {
    "base": {
        "start_date": "2012-01-01",
        "end_date": "2016-12-01",
        "accounts": {'stock': 100000},
        "benchmark": None,
        "frequency": frequency,
        #     "strategy_file_path": os.path.abspath(__file__)
    },
    "extra": {
        "log_level": "verbose",
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            "report_save_path": ".report",
            "plot": True
        },
        "fxdayu_source": {
            "enabled": True,
            "mongo_url": "mongodb://192.168.0.101:27017,192.168.0.102:27017",
            "enable_cache": True,
            "cache_length": 10000
        }
    }
}

# config["mod"]["fxdayu_source"] = {
#     "enabled": True,
#     "source": "bundle",
#     "enable_cache": True,
#     "cache_length": 10000
# }

if __name__ == "__main__":
    import time
    import os

    st = time.time()
    os.makedirs(report_path, exist_ok=True)
    run_file(__file__, config)
    print("Time Cost: %s seconds" % (time.time() - st))
