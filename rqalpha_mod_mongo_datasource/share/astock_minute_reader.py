# encoding: utf-8

import os
from lru import LRU

import bcolz
import numba as nb
import numpy as np
import pandas as pd
import six
from rqalpha.data.converter import StockBarConverter
from rqalpha.utils.datetime_func import convert_dt_to_int
from zipline.data._minute_bar_internal import find_position_of_minute
from zipline.data.minute_bars import BcolzMinuteBarReader
from zipline.gens.sim_engine import NANOS_IN_MINUTE

from .utils import FXDAYU_BUNDLE_PATH
from .trading_calendar import ASTOCK_TRADING_CALENDAR as _
from .trading_session import ATOCK_TRADING_SESSION
from .utils import sid_subdir_path as _sid_subdir_path, calc_minute_index as _calc_minute_index

_  # register the ASTOCK_TRADING_CALENDAR


class AStockBcolzMinuteBarReader(BcolzMinuteBarReader):
    FIELDS = ["open", "high", "low", "close", "volume"]

    def __init__(self, rootdir=FXDAYU_BUNDLE_PATH, sid_cache_size=1000, converter=StockBarConverter,
                 trading_session=ATOCK_TRADING_SESSION):
        minutes_dir = os.path.join(rootdir if rootdir is not None else FXDAYU_BUNDLE_PATH, "minutes")
        super(AStockBcolzMinuteBarReader, self).__init__(minutes_dir, sid_cache_size=sid_cache_size)
        self._index_skip_suspending = LRU(sid_cache_size)
        self._converter = converter
        self._minute_index = _calc_minute_index(self._market_opens, trading_session)

    def _get_carray_path(self, sid, field):
        sid, pa_dir = sid.split(".")
        return os.path.join(self._rootdir, pa_dir, _sid_subdir_path(int(sid)), field)

    def _open_minute_file(self, field, sid):
        try:
            carray = self._carrays[field][sid]
        except KeyError:
            carray = self._carrays[field][sid] = \
                bcolz.carray(rootdir=self._get_carray_path(sid, field),
                             mode='r')

        return carray

    def _find_position_of_minute(self, minute_dt):
        """
        Internal method that returns the position of the given minute in the
        list of every trading minute since market open of the first trading
        day. Adjusts non market minutes to the last close.

        ex. this method would return 1 for 2002-01-02 9:32 AM Eastern, if
        2002-01-02 is the first trading day of the dataset.

        Parameters
        ----------
        minute_dt: pd.Timestamp
            The minute whose position should be calculated.

        Returns
        -------
        int: The position of the given minute in the list of all trading
        minutes since market open on the first trading day.
        """
        return find_position_of_minute(
            self._market_open_values,
            self._market_close_values,
            minute_dt.value / NANOS_IN_MINUTE,
            self._minutes_per_day,
            True,
        )

    def _filtered_index(self, instrument):
        # TODO 确认是否跳过日内涨跌停
        if instrument not in self._index_skip_suspending:
            carray = self._open_minute_file("close", instrument)
            sub_index = bcolz.eval("carray != 0", vm="numexpr")
            index = self._minute_index[:len(sub_index)][sub_index]
            self._index_skip_suspending[instrument] = index
        return self._index_skip_suspending[instrument]

    def get_dt_slice(self, instrument, start_dt=None, end_dt=None, length=None, skip_suspended=True):
        """

        Parameters
        ----------
        instrument
        start_dt
        end_dt
        length
        skip_suspended

        Returns
        -------

        """

        if not (start_dt and end_dt):
            if skip_suspended:
                index = self._filtered_index(instrument)
            else:
                index = self._minute_index
            if end_dt and length:
                start_dt = index[np.searchsorted(index, end_dt, side="right") - length]
            elif start_dt and length:
                end_dt = index[np.searchsorted(index, start_dt) + length]
        if not (start_dt and end_dt):
            raise RuntimeError("At least two of start_dt, end_dt and length must be given")
        slicer = self._minute_index.slice_indexer(start_dt, end_dt)
        return slicer.start, slicer.stop

    @staticmethod
    @nb.jit
    def numba_loops_ffill(arr):
        """Numba decorator solution provided by shx2.

        Parameters
        ----------
        arr
        """
        out = arr.copy()
        for row_idx in range(out.shape[0]):
            for col_idx in range(1, out.shape[1]):
                if np.isnan(out[row_idx, col_idx]):
                    out[row_idx, col_idx] = out[row_idx, col_idx - 1]
        return out

    @staticmethod
    # @nb.jit
    def numba_loops_dropna(arr):
        mask = np.full((arr.shape[0], len(arr.dtype)), True)
        for n, name in enumerate(arr.dtype.names):
            mask[:, n] = ~np.isnan(arr[name])
        mask = mask.min(axis=1)
        return arr[mask]

    def raw_history_bars(self, instrument, start_dt=None, end_dt=None, length=None, fields=None, skip_suspended=True):
        """

        Parameters
        ----------
        instrument
        start_dt
        end_dt
        length
        fields
        skip_suspended

        Returns
        -------

        """
        start_idx, end_idx = self.get_dt_slice(instrument, start_dt, end_dt, length, skip_suspended)
        if fields is None:
            fields_ = self.FIELDS
        elif isinstance(fields, six.string_types):
            fields_ = [fields]
        else:
            fields_ = [field for field in fields if field != "datetime"]
        num_minutes = end_idx - start_idx
        types = {f: self._converter.field_type(f, np.float64) for f in fields_}
        dtype = np.dtype([("datetime", np.uint64)] +
                         [(f, self._converter.field_type(f, np.float64)) for f in fields_])
        shape = (num_minutes,)
        result = np.empty((num_minutes,), dtype=dtype)
        for field in fields_:
            if field != 'volume':
                out = np.full(shape, np.nan, dtype=types[field])
            else:
                out = np.zeros(shape, dtype=types[field])
            carray = self._open_minute_file(field, instrument)
            values = carray[start_idx: end_idx]
            where = values != 0
            if field != 'volume':
                out[:len(where)][where] = values[where] * self._ohlc_ratio_inverse_for_sid(instrument)
            else:
                out[:len(where)][where] = values[where]
            result[field] = out
        result["datetime"] = list(map(convert_dt_to_int, self._minute_index[start_idx: end_idx].to_pydatetime()))
        result = result if fields is None else result[fields]
        return self.numba_loops_dropna(result) if skip_suspended else self.numba_loops_ffill(result)

    def load_raw_arrays(self, instruments, start_dt=None, end_dt=None, fields=None, length=None):
        """
        Load raw arrays from bundles
        Mainly used for Data API

        Parameters
        ----------
        instruments:
            list of instrument, The asset identifiers in the window.
        start_dt: Timestamp
            Beginning of the window range.
        end_dt: Timestamp
            End of the window range.
        length:
            Length of the window range.
        fields : list of str
            'open', 'high', 'low', 'close', or 'volume'

        Returns
        -------
        list of np.ndarray
            A list with an entry per field of ndarrays with shape
            (minutes in range, sids) with a dtype of float64, containing the
            values for the respective field over start and end dt range.
        """
        # 修改部分
        ###
        if not (start_dt or end_dt or length):
            raise RuntimeError("At least two of start_dt, end_dt and length must be given")
        if end_dt is not None:
            end_idx = self._find_position_of_minute(end_dt)
            if length is not None:
                start_idx = end_idx - length + 1
            else:
                start_idx = self._find_position_of_minute(start_dt)
        else:
            start_idx = self._find_position_of_minute(start_dt)
            end_idx = start_idx + length - 1
        ###

        num_minutes = (end_idx - start_idx + 1)

        results = []

        # 修改部分
        ###
        # indices_to_exclude = self._exclusion_indices_for_range(
        #     start_idx, end_idx)
        indices_to_exclude = None  # 暂时不处理交易时间奇异的情况
        if indices_to_exclude is not None:
            for excl_start, excl_stop in indices_to_exclude:
                length = excl_stop - excl_start + 1
                num_minutes -= length
        ###

        shape = num_minutes, len(instruments)
        if fields is None:
            fields = ["open", "high", "low", "close", "volume"]
        for field in fields:
            if field != 'volume':
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)

            for i, sid in enumerate(instruments):
                carray = self._open_minute_file(field, sid)
                values = carray[start_idx:end_idx + 1]
                if indices_to_exclude is not None:
                    for excl_start, excl_stop in indices_to_exclude[::-1]:
                        excl_slice = np.s_[
                                     excl_start - start_idx:excl_stop - start_idx + 1]
                        values = np.delete(values, excl_slice)

                where = values != 0
                # first slice down to len(where) because we might not have
                # written data for all the minutes requested
                if field != 'volume':
                    out[:len(where), i][where] = (
                        values[where] * self._ohlc_ratio_inverse_for_sid(sid))
                else:
                    out[:len(where), i][where] = values[where]
            results.append(out)
        return results

    def available_data_range(self):
        return self.calendar.first_session.to_pydatetime().date(), self.calendar.last_session.to_pydatetime().date()
