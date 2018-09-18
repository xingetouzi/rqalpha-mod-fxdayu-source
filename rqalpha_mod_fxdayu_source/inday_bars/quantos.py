import time

import pandas as pd
from rqalpha.utils.logger import user_system_log

from rqalpha_mod_fxdayu_source.inday_bars.base import AbstractIndayBars
from rqalpha_mod_fxdayu_source.utils.converter import QuantOsConverter
from rqalpha_mod_fxdayu_source.utils.instrument import instrument_to_tushare
from rqalpha_mod_fxdayu_source.utils.quantos import QuantOsDataApiMixin, QuantOsQueryError, ensure_api_login


class QuantOsIndayBars(AbstractIndayBars, QuantOsDataApiMixin):
    MAX_RETRY = 3

    def __init__(self, api_url, user, token):
        super(QuantOsIndayBars, self).__init__()
        QuantOsDataApiMixin.__init__(self, api_url, user, token)

    @ensure_api_login
    def get_bars(self, instrument, frequency, trade_date=None, start_time=None, end_time=None):
        symbol = instrument_to_tushare(instrument)
        kwargs = {}
        if start_time is not None:
            kwargs["start_time"] = start_time
        elif end_time is not None:
            kwargs["end_time"] = end_time
        retry = 0
        while retry < self.MAX_RETRY:
            retry += 1
            try:
                freq = frequency[:-1] + frequency[-1].upper()
                params = dict(symbol=symbol, freq=freq, trade_date=0, **kwargs)
                bars, msg = self._api.bar(**params)
                code = msg.split(",")[0]
                if not isinstance(bars, pd.DataFrame) or code != "0":
                    raise QuantOsQueryError(msg)
                else:
                    break
            except QuantOsQueryError as e:
                if retry <= self.MAX_RETRY:
                    user_system_log.warning("[japs] Exception occurs when call api.bar with param [%s]: %s" % (params, e))
                    time.sleep(retry)
                else:
                    raise e
        return QuantOsConverter.df2np(bars)
