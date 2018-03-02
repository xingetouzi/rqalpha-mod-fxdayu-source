# -*- coding: utf-8 -*-
import itertools

from rqalpha.api import *
import pandas as pd
from rqalpha.utils.datetime_func import convert_dt_to_int


def init(context):
    logger.info("init")
    context.s1 = "000001.XSHE"
    update_universe(context.s1)
    context.fired = False


def before_trading(context):
    pass


def handle_bar(context, bar_dict):
    bar = bar_dict[context.s1]
    print(bar)
    assert bar.datetime == context.now
    lengths = [5]
    frequencies = ["1m"]
    for l, f in itertools.product(lengths, frequencies):
        # print(pd.DataFrame(history_bars(context.s1, 5, "1d", include_now=True)))
        df = pd.DataFrame(history_bars(context.s1, l, f))
        print(df)
        assert len(df) == l
        assert convert_dt_to_int(context.now) == df["datetime"].iloc[-1]
    if not context.fired:
        # order_percent并且传入1代表买入该股票并且使其占有投资组合的100%
        order_percent(context.s1, 1)
        context.fired = True
