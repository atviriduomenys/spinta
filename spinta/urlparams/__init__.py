from spinta.commands import prepare
from spinta.components import Context
from spinta.utils.url import parse_url_path


class UrlParams:
    def __init__(self):
        self.params = {}
        self.path = None


class Version:
    def __init__(self):
        self.version = None


@prepare.register()
def prepare(context: Context, url_params: UrlParams, version: Version, *, path="") -> UrlParams:
    url_params.params = parse_url_path(url_params.path)
    return url_params
