import os

_api = None


class QuantOsDataApiMixin(object):
    def __init__(self, api_url=None, user=None, token=None):
        global _api
        from jaqs.data import DataApi
        if _api is None:
            url = api_url or os.environ.get("QUANTOS_URL", "tcp://data.quantos.org:8910")
            user = user or os.environ.get("QUANTOS_USER")
            token = token or os.environ.get("QUANTOS_TOKEN")
            _api = DataApi(addr=url)
            _api.login(user, token)
        self._api = _api
