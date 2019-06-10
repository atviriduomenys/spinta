from starlette.requests import Request

from spinta.commands import prepare
from spinta.components import Context
from spinta.utils.url import parse_url_path
from spinta.utils.response import get_response_type


class UrlParams:

    def __init__(self):
        self.model = None
        self.id = None
        self.properties = None
        self.dataset = None
        self.changes = None
        self.sort = None
        self.limit = None
        self.offset = None
        self.format = None
        self.count = None

        # Deprecated, added for backwards compatibility.
        self.params = {}


class Version:

    def __init__(self):
        self.version = None


@prepare.register()
def prepare(context: Context, params: UrlParams, version: Version, request: Request) -> UrlParams:
    path = request.path_params['path'].strip('/')
    p = parse_url_path(context, path)
    params.model = p['path']
    params.id = p.get('id')
    params.properties = p.get('properties')
    params.dataset = p.get('source')
    params.sort = p.get('sort')
    params.limit = p.get('limit')

    if 'changes' in p:
        params.changes = True
        params.offset = p['changes'] or -10
    else:
        params.changes = False
        params.offset = p.get('offset')

    params.format = get_response_type(context, request, p)
    params.count = p.get('count')
    params.params = p  # XXX: for backwards compatibility
    return params
