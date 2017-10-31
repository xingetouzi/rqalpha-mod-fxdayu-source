# encoding: utf-8

import pandas as pd
from rqalpha.data.adjust import FIELDS_REQUIRE_ADJUSTMENT, adjust_bars
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.utils.datetime_func import convert_dt_to_int

from rqalpha_mod_fxdayu_source.utils import DataFrameConverter

RESAMPLE_TAG_MAP = {
    "m": "T",
    "h": "h",
    "d": "d",
}

TIME_TOLERANCE = {
    "m": 100,
    "h": 10000,
    "d": 1000000,
}


class OddFrequencyDataSource(BaseDataSource):
    def __init__(self, *args, **kwargs):
        super(OddFrequencyDataSource, self).__init__(*args, **kwargs)

    def _resample_bars(self, bars, frequency):
        num = int(frequency[:-1])
        freq = frequency[-1]
        bar_data = DataFrameConverter.np2df(bars)
        bar_data = bar_data.set_index(bar_data["datetime"].values)
        resample_freq = str(num) + RESAMPLE_TAG_MAP[freq]
        resample_group = bar_data.resample(resample_freq, closed="right", label="right")
        resample_data = pd.DataFrame()
        resample_data["high"] = resample_group["high"].max().dropna()
        resample_data["low"] = resample_group["low"].min().dropna()
        resample_data["close"] = resample_group["close"].last().dropna()
        resample_data["open"] = resample_group["open"].first().dropna()
        resample_data["volume"] = resample_group["volume"].sum().dropna()
        resample_data["datetime"] = resample_group["datetime"].last().dropna()
        bar_data = resample_data.reset_index(list(range(len(resample_data))), drop=True)
        bar_data = DataFrameConverter.df2np(bar_data)
        return bar_data

    def get_bar(self, instrument, dt, frequency):
        num = int(frequency[:-1])
        freq = frequency[-1]
        if self.is_base_frequency(instrument, frequency):
            bars = self.raw_history_bars(instrument, frequency, end_dt=dt, length=1)
        else:
            if freq == "m":
                bars = self.raw_history_bars(instrument, "1" + freq, end_dt=dt, length=num)
                bars = self._resample_bars(bars, frequency)
            else:
                return super(OddFrequencyDataSource, self).get_bar(instrument, dt, frequency)
        if bars is None or not bars.size:
            return super(OddFrequencyDataSource, self).get_bar(
                instrument, dt, frequency
            )
        else:
            dti = convert_dt_to_int(dt)
            # TODO num * TIME_TOLERANCE[freq] maybe some problem in "d" frequency
            if abs(bars[-1]["datetime"] - dti) < num * TIME_TOLERANCE[freq]:
                return bars[-1]
            else:
                data = bars[-1].copy()
                data["datetime"] = dti
                data["open"] = data["close"]
                data["high"] = data["close"]
                data["low"] = data["close"]
                data["volume"] = 0
                return data

    def history_bars(self, instrument, bar_count, frequency, fields, dt,
                     skip_suspended=True, include_now=False,
                     adjust_type='pre', adjust_orig=None):
        if self.is_base_frequency(instrument, frequency):
            bars = self.raw_history_bars(instrument, frequency, end_dt=dt, length=bar_count)
        else:
            num = int(frequency[:-1])
            freq = frequency[-1]
            if freq == "m":
                lower_bar_count = (bar_count + 1) * num
                bars = self.raw_history_bars(instrument, "1" + freq, end_dt=dt, length=lower_bar_count)
                if bars is None:
                    return super(OddFrequencyDataSource, self).history_bars(
                        instrument, bar_count, frequency, fields, dt,
                        skip_suspended=skip_suspended, include_now=include_now,
                        adjust_type=adjust_type, adjust_orig=adjust_orig
                    )
                else:
                    if bars.size:
                        bars = self._resample_bars(bars, frequency)
                        dti = convert_dt_to_int(dt)
                        if bars["datetime"][-1] != dti and not include_now:
                            bars = bars[:-1]
                            bars = bars[-bar_count:]
                        else:
                            bars = bars[-bar_count:]
                            # TODO 跳过停牌
            else:
                return super(OddFrequencyDataSource, self).history_bars(
                    instrument, bar_count, frequency, fields, dt,
                    skip_suspended=skip_suspended, include_now=include_now,
                    adjust_type=adjust_type, adjust_orig=adjust_orig
                )
                # if fields is not None:
                #     if not isinstance(fields, six.string_types):
                #         fields = [field for field in fields if field in bar_data]
        if adjust_type == "none" or instrument.type in {"Future", "INDX"}:
            return bars if fields is None else bars[fields]
        if isinstance(fields, str) and fields not in FIELDS_REQUIRE_ADJUSTMENT:
            return bars if fields is None else bars[fields]
        return adjust_bars(bars, self.get_ex_cum_factor(instrument.order_book_id),
                           fields, adjust_type, adjust_orig)

    def raw_history_bars(self, *args, **kwargs):
        raise NotImplementedError

    def is_base_frequency(self, instrument, freq):
        num = int(freq[:-1])
        return num == 1
