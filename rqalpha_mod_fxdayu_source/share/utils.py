import os

import numpy as np
import pandas as pd


def sid_subdir_path(sid):
    """
    Format subdir path to limit the number directories in any given
    subdirectory to 100.

    The number in each directory is designed to support at least 100000
    equities.

    Parameters
    ----------
    sid : int
        Asset identifier.

    Returns
    -------
    out : string
        A path for the bcolz rootdir, including subdirectory prefixes based on
        the padded string representation of the given sid.

        e.g. 1 is formatted as 00/00/000001.bcolz

    """
    padded_sid = format(sid, '06')
    return os.path.join(
        # subdir 2 00/XX
        padded_sid[0:2],
        # subdir 2 XXX/0
        padded_sid[2:4],
        "{0}.bcolz".format(str(padded_sid))
    )


def calc_minute_index(market_opens, trading_session):
    """
    Cal all trading minutes according to input daily market open and trading session information.

    Parameters
    ----------
    market_opens: datetime64 array
        array of every day market open.
    trading_session: set -> list
        list of time offset in minutes for every trading session in a day.
    Returns
    -------
    out : datetime64 array
        all trading minutes.
    """
    minutes_per_day = trading_session.minute_per_day
    minutes = np.zeros(len(market_opens) * minutes_per_day, dtype="datetime64[ns]")
    deltas_lst = []
    session_offsets = []
    for offset, duration in trading_session.sessions:
        deltas_lst.append(np.arange(0, duration, dtype="timedelta64[m]"))
        session_offsets.append(pd.Timedelta(minutes=offset))
    for i, marker_open in enumerate(market_opens):
        start = marker_open.asm8
        sessions = []
        for deltas, session_offset in zip(deltas_lst, session_offsets):
            sessions.append(deltas + start + session_offset)
        minute_values = np.concatenate(sessions)
        start_ix = minutes_per_day * i
        end_ix = start_ix + minutes_per_day
        minutes[start_ix:end_ix] = minute_values
    return pd.to_datetime(minutes, utc=True, box=True)


FXDAYU_ROOT = os.environ.get("FXDAYU_ROOT", "~/.fxdayu")
FXDAYU_BUNDLE_PATH = os.path.join(FXDAYU_ROOT, "bundle")
