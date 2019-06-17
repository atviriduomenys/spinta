import cgi
import itertools
import json

from starlette.exceptions import HTTPException
from starlette.requests import Request

from spinta import commands
from spinta.types.store import get_model_from_params
from spinta.utils.url import parse_url_path
from spinta.components import Context, Action


METHOD_TO_ACTION = {
    'POST': Action.INSERT,
    'PUT': Action.UPDATE,
    'PATCH': Action.PATCH,
    'DELETE': Action.DELETE,
}


async def create_http_response(context, params, request):
    store = context.get('store')
    manifest = store.manifests['default']

    model = get_model_from_params(manifest, params.path, params.params)
    prop, ref = get_prop_from_params(model, params)

    if model is None or params.contents:
        return await commands.contents(context, request, params=params)

    if request.method == 'GET':
        context.set('transaction', manifest.backend.transaction())
        if params.changes:
            return await commands.changes(context, request, model, model.backend, action=Action.CHANGES, params=params)
        elif params.id:
            if prop and ref:
                return await commands.getone(context, request, prop, model.backend, action=Action.GETONE, params=params)
            elif prop:
                return await commands.getone(context, request, prop, prop.backend, action=Action.GETONE, params=params)
            else:
                return await commands.getone(context, request, model, model.backend, action=Action.GETONE, params=params)
        else:
            action = Action.SEARCH if params.search else Action.GETALL
            return await commands.getall(context, request, model, model.backend, action=action, params=params)
    else:
        context.bind('transaction', manifest.backend.transaction, write=True)
        action = METHOD_TO_ACTION[request.method]
        if prop and ref:
            return await commands.push(context, request, prop, model.backend, action=action, params=params)
        elif prop:
            return await commands.push(context, request, prop, prop.backend, action=action, params=params)
        else:
            return await commands.push(context, request, model, model.backend, action=action, params=params)


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


def peek_and_stream(stream):
    peek = list(itertools.islice(stream, 2))

    def _iter():
        for data in itertools.chain(peek, stream):
            yield data

    return _iter()


async def aiter(stream):
    for data in stream:
        yield data


async def get_request_data(request):
    ct = request.headers.get('content-type')
    if ct != 'application/json':
        raise HTTPException(
            status_code=415,
            detail=f"Only 'application/json' content-type is supported, got {ct!r}.",
        )

    try:
        data = await request.json()
    except json.decoder.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="not a valid json",
        )

    return data


def get_prop_from_params(model, params):
    if not params.properties:
        return None, False
    if len(params.properties) != 1:
        raise HTTPException(status_code=400, detail=(
            "only one property can be given, now "
            "these properties were given: "
        ) % ', '.join(map(repr, params.properties)))
    prop = params.properties[0]
    ref = False
    if prop.endswith(':ref'):
        ref = True
        prop = prop[:-len(':ref')]

    if prop not in model.properties:
        raise HTTPException(status_code=404, detail=f"Unknown property {prop}.")
    return model.properties[prop], ref
