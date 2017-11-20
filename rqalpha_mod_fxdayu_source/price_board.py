import numpy as np
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.core.bar_dict_price_board import BarDictPriceBoard


class StockLimitUpDownPriceBoard(BarDictPriceBoard):
    def __init__(self):
        super(StockLimitUpDownPriceBoard, self).__init__()
        self._previous_close = {}

    def _get_prev_close(self, instrument):
        order_book_id = instrument.order_book_id
        date = self._env.data_proxy.get_previous_trading_date(self._env.calendar_dt)
        if order_book_id not in self._previous_close or date > self._previous_close[order_book_id][0]:
            bar = self._env.data_source.history_bars(
                instrument, 1, "1d", "close", date, adjust_type="none"
            )
            if bar is not None:
                prev_close = np.squeeze(bar)
            else:
                prev_close = np.nan
            self._previous_close[order_book_id] = (date, prev_close)
        return self._previous_close[order_book_id][1]

    def _get_limit_up(self, instrument):
        return round(self._get_prev_close(instrument) * 1.1, 2)

    def _get_limit_down(self, instrument):
        return round(self._get_prev_close(instrument) * 0.9, 2)

    def get_limit_up(self, order_book_id):
        instrument = self._env.get_instrument(order_book_id)
        if instrument.enum_type in [INSTRUMENT_TYPE.CS, INSTRUMENT_TYPE.INDX]:
            return self._get_limit_up(instrument)
        else:
            return super(StockLimitUpDownPriceBoard, self).get_limit_up(order_book_id)

    def get_limit_down(self, order_book_id):
        instrument = self._env.get_instrument(order_book_id)
        if instrument.enum_type in [INSTRUMENT_TYPE.CS, INSTRUMENT_TYPE.INDX]:
            return self._get_limit_down(instrument)
        else:
            return super(StockLimitUpDownPriceBoard, self).get_limit_down(order_book_id)
