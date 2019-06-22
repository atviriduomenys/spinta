import cgi

from starlette.requests import Request

from spinta.commands import prepare
from spinta.components import Context, Manifest
from spinta.utils.url import parse_url_path
from spinta.components import UrlParams, Version
from spinta.exceptions import NotFound, FoundMultiple


@prepare.register()
def prepare(context: Context, params: UrlParams, version: Version, request: Request) -> UrlParams:
    path = request.path_params['path'].strip('/')
    p = params.params = parse_url_path(context, path)
    _prepare_model_params(params)
    params.search = any([
        'show' in p,
        'sort' in p,
        'limit' in p,
        'offset' in p,
        'count' in p,
        'query_params' in p,
    ])
    params.sort = p.get('sort')
    params.limit = p.get('limit')
    params.show = p.get('show')
    params.query = p.get('query_params')

    if 'changes' in p:
        params.changes = True
        params.offset = p['changes'] or -10
    else:
        params.changes = False
        params.offset = p.get('offset')

    params.format = get_response_type(context, request, p)
    params.count = 'count' in p
    params.contents = 'contents' in p
    return params


def _prepare_model_params(params):
    p = params.params
    params.path = p['path']
    params.id = p.get('id')
    params.properties = p.get('properties')
    params.dataset = p.get('dataset')
    params.resource = p.get('resource')


def get_model_by_name(context: Context, manifest: Manifest, name: str):
    config = context.get('config')
    UrlParams = config.components['urlparams']['component']
    params = UrlParams()
    params.params = parse_url_path(context, name)
    _prepare_model_params(params)
    model = get_model_from_params(manifest, params)
    if model is None:
        raise NotFound(f"Model {name!r} not found.")
    return model


def get_model_from_params(manifest, params):
    name = params.path

    if name in manifest.endpoints:
        # Allow users to specify a different URL endpoint to make URL look
        # nicer, but that is optional, they can still use model.name.
        name = manifest.endpoints[name]

    if name not in manifest.tree:
        raise NotFound(f"Model or collection {name!r} not found.")

    if params.dataset:
        dataset = manifest.objects['dataset'].get(params.dataset)
        if dataset is None:
            raise NotFound(f"Dataset ':dataset/{params.dataset}' not found.")
        if params.resource:
            resource = dataset.resources.get(params.resource)
            if resource is None:
                raise NotFound(
                    f"Resource ':dataset/{params.dataset}/:resource/{params.resource}'"
                    " not found."
                )
            return resource.objects.get(name)
        else:
            models = [
                resource.objects[name]
                for resource in dataset.resources.values()
                if name in resource.objects
            ]
            if len(models) == 0:
                return None
            elif len(models) == 1:
                return models[0]
            else:
                raise FoundMultiple(
                    f"Found multiple {name!r} models  in {params.dataset!r} "
                    "dataset. By more specific by providing resource name."
                )
    else:
        return manifest.objects['model'].get(name)


def get_response_type(context: Context, request: Request, params: dict = None):
    if params is None and 'path' in request.path_params:
        path = request.path_params['path'].strip('/')
        params = parse_url_path(context, path)
    elif params is None:
        params = {}

    if 'format' in params:
        return params['format']

    if 'accept' in request.headers and request.headers['accept']:
        formats = {
            'text/html': 'html',
            'application/xhtml+xml': 'html',
        }
        config = context.get('config')
        for name, exporter in config.exporters.items():
            for media_type in exporter.accept_types:
                formats[media_type] = name

        media_types, _ = cgi.parse_header(request.headers['accept'])
        for media_type in media_types.lower().split(','):
            if media_type in formats:
                return formats[media_type]

    return 'json'
