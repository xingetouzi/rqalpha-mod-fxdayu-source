import os

from zipline.data.minute_bars import BcolzMinuteBarReader

from .utils import sid_subdir_path as _sid_subdir_path
from .trading_calendar import ASTOCK_TRADING_CALENDAR as _

_  # register the ASTOCK_TRADING_CALENDAR


class AStockBcolzMinuteBarWriter(BcolzMinuteBarReader):
    def _get_carray_path(self, sid, field):
        sid, pa_dir = sid.split(".")
        return os.path.join(self._rootdir, pa_dir, _sid_subdir_path(int(sid)), field)
