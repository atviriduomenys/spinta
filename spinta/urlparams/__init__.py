from spinta.commands import prepare
from spinta.components import Context
from spinta.utils.url import parse_url_path


class UrlParams:
    def __init__(self, path="", method="", headers={}):
        self.params = {}
        self.path = path
        self.method = method
        self.headers = headers

    def parse_path(self):
        # sets self.params with parsed request parameters
        self.params = parse_url_path(self.path)


class Version:
    def __init__(self):
        self.version = None


@prepare.register()
def prepare(context: Context, url_params: UrlParams, version: Version) -> UrlParams:
    url_params.parse_path()
    return url_params
