class AbstractIndayBars(object):
    def get_bars(self, instrument, frequency, trade_date=None, start_time=None, end_time=None):
        raise NotImplementedError
