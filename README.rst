************************************************
Expanded Rqalpha Data and Event Source by Fxdayu
************************************************

介绍
========
rqalpha是一款开源的基于事件驱动的交易执行引擎，其架构高度解耦，支持各种mod拓展。

本mod主要拓展了rqalpha数据源和事件源，数据源共有3种方案，前2种为公司内部使用，
分别基于mongodb和bcolz的压缩格式文件，外部很难接入。最新加入第3种对接tushare-pro开源财经数据源（感谢米哥），
使用起来非常方便。

特点
=======
+ 依托于tushare-pro的数据服务，只要有网络通畅就可以进行回测和实时交易，无需额外数据文件下载或数据库搭建
+ 使用简单，只需安装、激活此mod，加入相应配置，并在quantos得到使用tushare-pro数据服务的权限即可
+ 支持按多种时间频率获取数据

======= ==================
\*d      任意天,1d,5d等
\*h      quantos数据暂未实现
\*m      任意分钟,1d,5d,10d等
======= ==================

+ 内置简单的数据缓存(Beta)

安装
======
.. code-block:: bash

    $ pip install git+https://github.com/xingetouzi/rqalpha-mod-fxdayu-source.git
    $ rqalpha mod install fxdayu_source

用例
======
strategy.py

.. code-block:: python

    # -*- coding: utf-8 -*-
    import itertools
    import os
    from rqalpha.api import *
    import pandas as pd
    from rqalpha.utils.datetime_func import convert_dt_to_int
    from rqalpha import run

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

    config = {
        "base": {
            "start_date": "2016-06-01",
            "end_date": "2016-06-05",
            "accounts": {"stock": 100000},
            "frequency": "1m",
            "benchmark": None,
            "strategy_file": __file__
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
            "sys_simulation": {
                "enabled": True,
                # "matching_type": "last"
            },
            "fxdayu_source": {
                "enabled": True,
                "source": "quantos",
                "quantos_user": "139xxxxxxxx", # 填入您的quantos用户名
                "quantos_token": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" # 填入您的quantos Token
                # 其他配置参数
            }
        }
    }

    if __name__ == "__main__":
        # 您可以指定您要传递的参数
        run(config=config)

运行strategy.py可以看到结果，没有发生AssertionError表示能够正常读取数据。

.. code-block:: bash

    $ python strategy.py


配置选项
========
============================= ==============================  ================= =======================================
选项                           默认值                           适用数据源类型       含义
============================= ==============================  ================= =======================================
fxdayu_source.enabled         "mongo"                         通用               行情源类型,可选值为"mongo","bundle","quantos"
fxdayu_source.bundle_path     None                            bundle            bundle数据文件位置，默认取"~/.fxdayu/bundle", 可以用环境变量覆盖，取值为"$FXDAYU_ROOT/bundle"
fxdayu_source.mongo_url       "mongodb://localhost:27017"     mongo             mongodb数据库地址
fxdayu_source.enable_cache    True                            通用               bool型，是否开启分页读取缓存优化功能(缓存优化适用于回测)。
fxdayu_source.cache_length    1000                            通用               当开启缓存优化时，指定单页缓存的条目数
fxdayu_source.quantos_url     "tcp://data.quantos.org:8910"   quantos           可选，tushare服务器地址，默认不需要配置
fxdayu_source.quantos_user    None                            quantos           必填，quantos用户名，可以从环境变量QUANTOS_USER传入
fxdayu_source.quantos_token   None                            quantos           必填，quantos Token，可以从环境变量QUANTOS_TOKEN传入
============================= ==============================  ================= =======================================

说明
=========
由于此mod使用了一些原来内部方案中的代码，故没有单独作为独立的模块。暂时不打算走正常的发布流程(旧的代码短期内可能有很大改动)，也不会发布到pypi，只分为master和dev分支，master为稳定分支，dev为开发分支，
功能变更将按日期写入changelog中。

加入开发
=========
github地址_

.. _github地址: https://github.com/xingetouzi/rqalpha-mod-fxdayu-source

欢迎提交各种Issue和Pull Request。