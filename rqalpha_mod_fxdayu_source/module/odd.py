# encoding: utf-8

import pandas as pd
import six
from rqalpha.interface import AbstractDataSource
from rqalpha.utils.datetime_func import convert_dt_to_int

from rqalpha_mod_fxdayu_source.utils import DataFrameConverter

RESAMPLE_TAG_MAP = {
    "m": "T",
    "h": "h",
    "d": "d",
}


class OddFrequencyDataSource(AbstractDataSource):
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
        if self.is_base_frequency(instrument, frequency):
            bar_data = self.raw_history_bars(instrument, frequency, end_dt=dt, length=1)
        else:
            num = int(frequency[:-1])
            freq = frequency[-1]
            if freq == "m":
                bar_data = self.raw_history_bars(instrument, "1" + freq, end_dt=dt, length=num)
                bar_data = self._resample_bars(bar_data, frequency)
            else:
                return super(OddFrequencyDataSource, self).get_bar(instrument, dt, frequency)
        if bar_data is None or not bar_data.size:
            return super(OddFrequencyDataSource, self).get_bar(
                instrument, dt, frequency
            )
        else:
            dti = convert_dt_to_int(dt)
            return bar_data[-1] if bar_data[-1]["datetime"] == dti else None

    def history_bars(self, instrument, bar_count, frequency, fields, dt,
                     skip_suspended=True, include_now=False,
                     adjust_type='pre', adjust_orig=None):
        if self.is_base_frequency(instrument, frequency):
            bar_data = self.raw_history_bars(instrument, frequency, end_dt=dt, length=bar_count)
        else:
            num = int(frequency[:-1])
            freq = frequency[-1]
            if freq == "m":
                lower_bar_count = (bar_count + 1) * num
                bar_data = self.raw_history_bars(instrument, "1" + freq, end_dt=dt, length=lower_bar_count)
                if bar_data is None:
                    return super(OddFrequencyDataSource, self).history_bars(
                        instrument, bar_count, frequency, fields, dt,
                        skip_suspended=skip_suspended, include_now=include_now,
                        adjust_type=adjust_type, adjust_orig=adjust_orig
                    )
                else:
                    if bar_data.size:
                        bar_data = self._resample_bars(bar_data, frequency)
                        dti = convert_dt_to_int(dt)
                        if bar_data["datetime"][-1] != dti and not include_now:
                            bar_data = bar_data[:-1]
                            bar_data = bar_data[-bar_count:]
                        else:
                            bar_data = bar_data[-bar_count:]
                            # TODO 复权以及跳过停牌
            else:
                return super(OddFrequencyDataSource, self).history_bars(
                        instrument, bar_count, frequency, fields, dt,
                        skip_suspended=skip_suspended, include_now=include_now,
                        adjust_type=adjust_type, adjust_orig=adjust_orig
                )
                # if fields is not None:
                #     if not isinstance(fields, six.string_types):
                #         fields = [field for field in fields if field in bar_data]
        return bar_data if fields is None else bar_data[fields]

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        raise NotImplementedError

    def is_base_frequency(self, instrument, freq):
        num = int(freq[:-1])
        return num == 1
