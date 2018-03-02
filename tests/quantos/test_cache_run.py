# -*- coding: utf-8 -*-
import os
import time
from pathlib import Path

from rqalpha import run

path = Path(os.path.abspath(__file__)).parent.parent / "strategies" / "simple.py"
frequency = "1m"

config = {
    "base": {
        "start_date": "2015-12-17",
        "end_date": "2015-12-31",
        "accounts": {"stock": 100000},
        "frequency": frequency,
        "benchmark": None,
        "strategy_file": str(path)
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
        }
    }
}


if __name__ == "__main__":
    start = time.time()
    run(config=config)
    print("Time Cost: %s seconds" % (time.time() - start))
