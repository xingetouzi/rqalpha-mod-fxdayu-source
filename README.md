# rqalpha mongodb datasource插件，支持分钟级别数据,以及任意分辨率的时间框架

## 安装步骤
在Terminal中运行
```
$ pip install git+https://github.com/xingetouzi/rqalpha-mod-fxdayu-source.git
$ rqalpha mod install fxdayu_source
```

## 配置选项：
| 选项 | 默认值 | 说明 |
| --- | :--: | --- |
| fxdayu\_source.enabled | True | 插件开关 |
| fxdayu\_source.source | "mongo" | 行情源类型，可选值为"mongo", "bundle" |
| fxdayu\_source.bundle\_path| None | bundle数据文件位置，默认取"~\\.fxdayu\\bundle", 可以用环境变量覆盖，取值为"$FXDAYU\_ROOT\\bundle" |
| fxdayu\_source.mongo\_url | "mongodb://localhost:27017" | mongodb数据库地址 |
| fxdayu\_source.enable\_cache | True | bool型，是否开启分页读取缓存优化功能(缓存优化仅适用于回测)。|
| fxdayu\_source.cache\_length | 1000 | 当开启缓存优化时，指定单页缓存的条目数 |

## 示例配置

1. mongo行情源
```
config = {
    "base": {
        "start_date": "2016-01-01",
        "end_date": "2016-06-05",
        "securities": ['stock'],
        "stock_starting_cash": 100000,
        "frequency": "1m",
        "benchmark": "000001.XSHG",
        "data_bundle_path": r"E:\Users\BurdenBear\.rqalpha\bundle",
        "strategy_file": __file__
    },
    "extra": {
        "log_level": "verbose",
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            # "report_save_path": ".",
            "plot": False
        },
        "sys_simulation": {
            "enabled": True,
            # "matching_type": "last"
        },
        "fxdayu_source": {
            "enabled": True,
            "mongo_url": "mongodb://192.168.0.103:30000",
        }
    }
}
```

2.bundle行情源
```
config = {
    "base": {
        "start_date": "2016-01-01",
        "end_date": "2016-06-05",
        "securities": ['stock'],
        "stock_starting_cash": 100000,
        "frequency": "1m",
        "benchmark": "000001.XSHG",
        "data_bundle_path": r"E:\Users\BurdenBear\.rqalpha\bundle",
        "strategy_file": __file__
    },
    "extra": {
        "log_level": "verbose",
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            # "report_save_path": ".",
            "plot": False
        },
        "sys_simulation": {
            "enabled": True,
            # "matching_type": "last"
        },
        "fxdayu_source": {
            "enabled": True,
            "source": "bundle",
            "bundle_path": os.path.expanduser("~\.fxdayu\bundle"),
        }
    }
}
```

# 支持的frequency
+ ```*d```: 任意天
+ ```*h```: 任意小时
+ ```*m```: 任意分钟

# MORE
更多example见

