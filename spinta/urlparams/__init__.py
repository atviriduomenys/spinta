from spinta.commands import prepare
from spinta.components import Context
from spinta.utils.url import parse_url_path


class UrlParams:
    def __init__(self):
        self.params = {}


class Version:
    def __init__(self):
        self.version = None


@prepare.register()
def prepare(context: Context, url_params: UrlParams, version: Version, *,
            path="", method="", headers={}) -> UrlParams:
    url_params.params = parse_url_path(path)
    return url_params
