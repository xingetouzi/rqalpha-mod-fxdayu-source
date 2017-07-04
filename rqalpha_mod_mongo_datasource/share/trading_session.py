from abc import ABCMeta, abstractproperty
from datetime import date, datetime, time

from six import with_metaclass


class TradingSession(with_metaclass(ABCMeta)):
    def __init__(self):
        self._minuter_per_day = None

    @abstractproperty
    def sessions(self):
        raise NotImplementedError

    @property
    def minute_per_day(self):
        total = 0
        for offset, number in self.sessions:
            total += number
        return total


class AStockTradingSession(TradingSession):
    @property
    def sessions(self):
        return [
            (0, self.cal_delta_minute(time(9, 31), time(11, 30))),
            (self.cal_delta_minute(time(9, 31), time(13, 00)), self.cal_delta_minute(time(13, 1), time(15, 00)))
        ]

    def cal_delta_minute(self, start, end):
        """

        Args:
            start(datetime.time): period start
            end(datetime.time): period end
        Returns:
            int: how many minutes between this period
        """
        dt = datetime.combine(date.today(), end) - datetime.combine(date.today(), start)
        result = (dt.days * 24 * 60 + dt.seconds // 60) + 1
        if result < 0:
            raise RuntimeError("period end should be after period start")
        else:
            return result


ATOCK_TRADING_SESSION = AStockTradingSession()
