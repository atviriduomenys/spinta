import itertools
import json

from starlette.requests import Request

from spinta import commands
from spinta.commands.formats import Format
from spinta.components import Context, Action, UrlParams, Node
from spinta import exceptions
from spinta.exceptions import NoBackendConfigured

METHOD_TO_ACTION = {
    'POST': Action.INSERT,
    'PUT': Action.UPDATE,
    'PATCH': Action.PATCH,
    'DELETE': Action.DELETE,
}


async def create_http_response(context: Context, params: UrlParams, request: Request):
    store = context.get('store')
    manifest = store.manifest

    if manifest.backend is None:
        raise NoBackendConfigured(manifest)

    if request.method == 'GET':
        context.attach('transaction', manifest.backend.transaction)

        if params.changes:
            _enforce_limit(context, params)
            return await commands.changes(context, request, params.model, params.model.backend, action=Action.CHANGES, params=params)

        elif params.pk:
            model = params.model
            action = Action.GETONE

            if params.prop and params.propref:
                prop = params.prop
                dtype = prop.dtype
                backend = model.backend
                return await commands.getone(context, request, prop, dtype, backend, action=action, params=params)
            elif params.prop:
                prop = params.prop
                dtype = prop.dtype
                backend = dtype.backend or model.backend
                return await commands.getone(context, request, prop, dtype, backend, action=action, params=params)
            else:
                backend = model.backend
                return await commands.getone(context, request, model, backend, action=action, params=params)

        else:
            search = any((
                params.query,
                params.select,
                params.limit is not None,
                params.offset is not None,
                params.sort,
                params.count,
            ))
            action = Action.SEARCH if search else Action.GETALL
            _enforce_limit(context, params)

            model = params.model
            backend = model.backend

            if backend is not None:
                # Namespace nodes do not have backend.
                context.attach(f'transaction.{backend.name}', backend.begin)

            if model.keymap:
                context.attach(f'keymap.{model.keymap.name}', lambda: model.keymap)

            return await commands.getall(context, request, model, backend, action=action, params=params)

    elif request.method == 'DELETE' and params.wipe:
        context.attach('transaction', manifest.backend.transaction, write=True)
        return await commands.wipe(context, request, params.model, params.model.backend, action=Action.WIPE, params=params)

    else:
        context.attach('transaction', manifest.backend.transaction, write=True)
        action = METHOD_TO_ACTION[request.method]
        if params.prop and params.propref:
            return await commands.push(context, request, params.prop.dtype, params.model.backend, action=action, params=params)
        elif params.prop:
            return await commands.push(context, request, params.prop.dtype, params.prop.dtype.backend, action=action, params=params)
        else:
            return await commands.push(context, request, params.model, params.model.backend, action=action, params=params)


def _enforce_limit(context: Context, params: UrlParams):
    config = context.get('config')
    fmt: Format = config.exporters[params.format]
    # XXX: I think this is not the best way to enforce limit, maybe simply
    #      an error should be raised?
    # XXX: Max resource count should be configurable.
    if not fmt.streamable and (params.limit is None or params.limit > 100):
        params.limit = 100
        params.limit_enforced = True


def peek_and_stream(stream):
    peek = list(itertools.islice(stream, 2))

    def _iter():
        for data in itertools.chain(peek, stream):
            yield data

    return _iter()


async def aiter(stream):
    for data in stream:
        yield data


async def get_request_data(node: Node, request: Request):
    ct = request.headers.get('content-type')
    if ct != 'application/json':
        raise exceptions.UnknownContentType(
            node,
            content_type=ct,
            supported_content_types=['application/json'],
        )

    try:
        data = await request.json()
    except json.decoder.JSONDecodeError as e:
        raise exceptions.JSONError(node, error=str(e))

    return data
