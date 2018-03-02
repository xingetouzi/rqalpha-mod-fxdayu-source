import os
from pathlib import Path

from rqalpha import run

path = Path(os.path.abspath(__file__)).parent.parent / "strategies" / "simple.py"

config = {
    "base": {
        "start_date": "2016-06-01",
        "end_date": "2016-06-05",
        "accounts": {"stock": 100000},
        "frequency": "1m",
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
    # 您可以指定您要传递的参数
    run(config=config)
