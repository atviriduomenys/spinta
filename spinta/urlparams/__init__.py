import cgi

from starlette.requests import Request

from spinta.commands import prepare
from spinta.components import Context
from spinta.utils.url import parse_url_path


class UrlParams:

    def __init__(self):
        self.model = None
        self.id = None
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
    config = context.get('config')
    path = request.path_params['path'].strip('/')
    p = parse_url_path(path)
    params.model = p['path']
    params.id = p.get('id')
    params.dataset = p.get('source')
    params.sort = p.get('sort')
    params.limit = p.get('limit')

    if 'changes' in p:
        params.changes = True
        params.offset = p['changes'] or -10
    else:
        params.changes = False
        params.offset = p.get('offset')

    if 'format' in p:
        params.format = p['format']
    else:
        if 'accept' in request.headers and request.headers['accept']:
            formats = {
                'text/html': 'html',
                'application/xhtml+xml': 'html',
            }
            for name, exporter in config.exporters.items():
                for media_type in exporter.accept_types:
                    formats[media_type] = name

            media_types, _ = cgi.parse_header(request.headers['accept'])
            for media_type in media_types.lower().split(','):
                if media_type in formats:
                    params.format = formats[media_type]
                    break

        if params.format is None:
            params.format = 'json'

    params.count = p.get('count')
    params.params = p  # XXX: for backwards compatibility
    return params
