import os
import unittest
from datetime import datetime, timedelta, time

from rqalpha.utils.datetime_func import convert_dt_to_int

from rqalpha_mod_fxdayu_source.data_source.common.realtime import RedisDataSource
from rqalpha_mod_fxdayu_source.data_source.mongo import MongoDataSource

RQALPHA_ROOT = os.environ.get("RQALPHA_ROOT")
REDIS_URL = os.environ.get("REDIS_URL")


class TestRawHistoryBars(unittest.TestCase):
    # 初始化工作
    def setUp(self):
        path = os.path.join(RQALPHA_ROOT, "bundle")
        history_source = self.source = MongoDataSource(str(path), mongo_url="mongodb://192.168.0.101:27017")
        self.source = RedisDataSource(path, REDIS_URL, history_source)
        self.instrument = self.source.get_all_instruments()[0]

    def test_end_length_with_history(self):
        try:
            start = None
            end = datetime.now()
            ei = convert_dt_to_int(end)
            today_i = ei // 1000000 * 1000000
            yesterday_i = convert_dt_to_int((end - timedelta(days=1)).replace(hour=0, minute=0, second=0))
            frequency = "1m"
            length = 300
            data = self.source.raw_history_bars(self.instrument, frequency, end_dt=end,
                                                length=length)
            dts = data["datetime"]
            assert dts[dts > today_i][0] == today_i + 93100
            assert dts[dts < today_i][-1] == yesterday_i + 150000
            assert len(data) == length
            assert 0 < dts[-1] - ei < 100
        except Exception as e:
            print("start: {}".format(start))
            print("end: {}".format(end))
            print("length: {}".format(length))
            print("data:\n{}".format(data))
            raise e

    def test_end_length_without_history(self):
        end = datetime.now()
        ei = convert_dt_to_int(end)
        frequency = "1m"
        data = self.source.raw_history_bars(self.instrument, frequency, end_dt=end,
                                            length=2)
        assert len(data) == 2
        assert data["datetime"][-1] - ei < 100

    def test_start_end_with_history(self):
        end = datetime.now()
        start = datetime.combine(end.date() - timedelta(days=1), time(hour=9, minute=33))
        frequency = "1m"
        ei = convert_dt_to_int(end)
        si = convert_dt_to_int(start)
        today_i = ei // 1000000 * 1000000
        yesterday_i = si // 1000000 * 1000000
        data = self.source.raw_history_bars(self.instrument, frequency, start_dt=start, end_dt=end)
        dts = data["datetime"]
        assert dts[dts > today_i][0] == today_i + 93100
        assert dts[dts < today_i][-1] == yesterday_i + 150000
        assert dts[0] == si
        assert 0 < dts[-1] - ei < 100

    def test_start_end_without_history(self):
        end = datetime.now()
        start = datetime.combine(end.date(), time=time(hour=9, minute=33))
        ei = convert_dt_to_int(end)
        si = convert_dt_to_int(start)
        frequency = "1m"
        data = self.source.raw_history_bars(self.instrument, frequency, start_dt=start, end_dt=end)
        dts = data["datetime"]
        if start > end:
            assert len(data) == 0
        else:
            assert dts[0] == si
            assert 0 < dts[-1] - ei < 100

    def test_start_length_with_history(self):
        end = datetime.now()
        start = datetime.combine(end.date() - timedelta(days=1), time=time(hour=9, minute=31))
        ei = convert_dt_to_int(end)
        si = convert_dt_to_int(start)
        today_i = ei // 1000000 * 1000000
        yesterday_i = si // 1000000 * 1000000
        length = 300
        frequency = "1m"
        data = self.source.raw_history_bars(self.instrument, frequency, start_dt=start, length=length)
        dts = data["datetime"]
        assert dts[dts > today_i][0] == today_i + 93100
        assert dts[dts < today_i][-1] == yesterday_i + 150000
        assert dts[0] == si
        assert len(dts) == length

    def test_start_length_without_history(self):
        end = datetime.now()
        start = datetime.combine(end.date(), time=time(hour=9, minute=31))
        ei = convert_dt_to_int(end)
        si = convert_dt_to_int(start)
        frequency = "1m"
        length = 2
        data = self.source.raw_history_bars(self.instrument, frequency, start_dt=start, length=length)
        dts = data["datetime"]
        if len(dts) and start <= end:
            assert dts[0] == si
            assert len(dts) == length


if __name__ == '__main__':
    unittest.main()
