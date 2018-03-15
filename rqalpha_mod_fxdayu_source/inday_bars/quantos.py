import pandas as pd

from rqalpha_mod_fxdayu_source.inday_bars.base import AbstractIndayBars
from rqalpha_mod_fxdayu_source.utils.converter import QuantOsConverter
from rqalpha_mod_fxdayu_source.utils.instrument import instrument_to_tushare
from rqalpha_mod_fxdayu_source.utils.quantos import QuantOsDataApiMixin


class QuantOsIndayBars(AbstractIndayBars, QuantOsDataApiMixin):
    def __init__(self, api_url, user, token):
        super(QuantOsIndayBars, self).__init__()
        QuantOsDataApiMixin.__init__(self, api_url, user, token)

    def get_bars(self, instrument, frequency, trade_date=None, start_time=None, end_time=None):
        symbol = instrument_to_tushare(instrument)
        kwargs = {}
        if start_time is not None:
            kwargs["start_time"] = start_time
        elif end_time is not None:
            kwargs["end_time"] = end_time
        bars, msg = self._api.bar(symbol=symbol, freq=frequency[:-1] + frequency[-1].upper(), trade_date=0,
                                  **kwargs)
        if not isinstance(bars, pd.DataFrame):
            raise RuntimeError(msg)
        return QuantOsConverter.df2np(bars)
