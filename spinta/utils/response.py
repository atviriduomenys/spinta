import itertools
import json

from starlette.exceptions import HTTPException
from starlette.requests import Request

from spinta import commands
from spinta.urlparams import get_model_from_params
from spinta.components import Context, Action, UrlParams
from spinta.exceptions import JSONError


METHOD_TO_ACTION = {
    'POST': Action.INSERT,
    'PUT': Action.UPDATE,
    'PATCH': Action.PATCH,
    'DELETE': Action.DELETE,
}


async def create_http_response(context: Context, params: UrlParams, request: Request):
    store = context.get('store')
    manifest = store.manifests['default']

    model = get_model_from_params(manifest, params)
    prop, ref = get_prop_from_params(model, params)

    if model is None or params.contents:
        return await commands.contents(context, request, params=params)

    if request.method == 'GET':
        context.attach('transaction', manifest.backend.transaction)
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
        context.attach('transaction', manifest.backend.transaction, write=True)
        action = METHOD_TO_ACTION[request.method]
        if prop and ref:
            return await commands.push(context, request, prop, model.backend, action=action, params=params)
        elif prop:
            return await commands.push(context, request, prop, prop.backend, action=action, params=params)
        else:
            return await commands.push(context, request, model, model.backend, action=action, params=params)


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
        raise JSONError()

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
        raise HTTPException(
            status_code=404,
            detail=f"Resource {model.name!r} does not have property {prop!r}."
        )
    return model.properties[prop], ref
