import click
import os
from rqalpha import cli

__config__ = {
    "source": "quantos",
    "mongo_url": os.environ.get("MONGO_URL", "mongodb://127.0.0.1:27017"),
    "redis_url": os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"),
    "bundle_path": None,
    # quantos
    "quantos_url": None,
    "quantos_user": None,
    "quantos_token": None,
    # cache
    "enable_cache": True,
    "cache_length": None,
    "max_cache_space": None,
    # other
    "fps": 60,
    "persist_path": ".persist",
    "priority": 200,
}


def load_mod():
    from .mod import FxdayuSourceMod
    return FxdayuSourceMod()


"""
--force-init
"""

cli.commands['run'].params.append(
    click.Option(
        ('--force-init/--no-force-init', 'extra__force_run_init_when_pt_resume'),
        is_flag=True, default=False, show_default=True,
        help="[fxdayu_source]force run init when paper trading resume or not"
    )
)
