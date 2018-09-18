import os
import time
import functools

from rqalpha.utils.logger import user_system_log

_api = None
_user = None
_token = None
_max_retry = 3

def ensure_api_login(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except QuantOsQueryError:
            api_login()
            return func(*args, **kwargs)
    return wrapper

def api_login():
    global _api

    retry = 0
    while retry < _max_retry:
        retry += 1
        try:
            _, msg = _api.login(_user, _token)
            code = msg.split(",")[0]
            if code != "0":
                raise QuantOsQueryError(msg)
            else:
                break
        except QuantOsQueryError as e:
            user_system_log.warn("[japs] Exception occurs when call api.login: %s" % e)
            if retry > _max_retry:
                raise e
            else:
                time.sleep(retry)

class QuantOsQueryError(Exception):
    """Error occurrs when make query from quantos."""

class QuantOsDataApiMixin(object):
    def __init__(self, api_url=None, user=None, token=None):
        global _api, _user, _token
        from jaqs.data import DataApi
        if _api is None:
            url = api_url or os.environ.get("QUANTOS_URL", "tcp://data.quantos.org:8910")
            _user = user or os.environ.get("QUANTOS_USER")
            _token = token or os.environ.get("QUANTOS_TOKEN")
            _api = DataApi(addr=url)
            api_login()
        self._api = _api