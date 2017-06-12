# rqalpha mongodb datasource插件，支持分钟级别数据

## 安装步骤
1.进入项目文件保存目录,用git拉取项目文件,

```git clone https://github.com/xingetouzi/rqalpha-mod-mongo-datasource.git```

2.切换到对应安装有rqalpha的python虚拟环境

3.运行```rqalpha mod install -e .```

## 配置选项：
| 选项 | 默认值 | 说明 |
| --- | :--: | --- |
| mongo\_datasource.enabled | True | 插件开关 |
| mongo\_datasource.mongo\_url | "mongodb://localhost:27017" | mongodb数据库地址 |
| mongo\_datasource.enable\_cache | False | 是否开启读取缓存优化功能(缓存优化仅适用于回测) |
| mongo\_datasource.cache_length | 1000 | 当开启缓存优化时，指定缓存的大小 |

## 示例配置

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
        "mongo_datasource": {
            "enabled": True,
            "mongo_url": "mongodb://192.168.0.103:30000",
            "enable_cache": True,
            "cache_length": 10000
        }
    }
}
```



