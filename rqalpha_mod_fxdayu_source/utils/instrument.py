import os
from pathlib import Path

import pandas as pd
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.utils.logger import system_log

path = Path(os.path.abspath(__file__)).parent.parent / "data" / "index_symbol_map.csv"
_map_instrument_to_tushare = pd.read_csv(path).drop_duplicates("symbol_ricequant").set_index("symbol_ricequant")
_suffix_map = {
    "XSHE": "SZ",
    "XSHG": "SH"
}


def instrument_to_tushare(instrument):
    """

    Parameters
    ----------
    instrument: rqalpha.model.instrument.Instrument

    Returns
    -------
    string: tushare code of the instrument
    """
    if instrument.enum_type in [INSTRUMENT_TYPE.INDX, INSTRUMENT_TYPE.CS]:
        if instrument.enum_type == INSTRUMENT_TYPE.INDX:
            try:
                return _map_instrument_to_tushare["symbol_tushare"].loc[instrument.order_book_id]
            except KeyError:
                # raise system_log.warning("Index %s may be not supported!" % instrument.order_book_id)
                pass
        code, suffix = instrument.order_book_id.split(".")
        return ".".join([code, _suffix_map[suffix]])
    else:
        # TODO 期货等
        raise RuntimeError("Unsupported instrument type.")
