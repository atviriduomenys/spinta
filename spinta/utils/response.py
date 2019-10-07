import itertools
import json

from starlette.requests import Request

from spinta import commands
from spinta.components import Context, Action, UrlParams, Node
from spinta import exceptions


METHOD_TO_ACTION = {
    'POST': Action.INSERT,
    'PUT': Action.UPDATE,
    'PATCH': Action.PATCH,
    'DELETE': Action.DELETE,
}


async def create_http_response(context: Context, params: UrlParams, request: Request):
    store = context.get('store')
    manifest = store.manifests['default']

    if request.method == 'GET':
        context.attach('transaction', manifest.backend.transaction)
        if params.changes:
            return await commands.changes(context, request, params.model, params.model.backend, action=Action.CHANGES, params=params)
        elif params.pk:
            if params.prop and params.propref:
                return await commands.getone(context, request, params.prop, params.model.backend, action=Action.GETONE, params=params)
            elif params.prop:
                return await commands.getone(context, request, params.prop, params.prop.backend, action=Action.GETONE, params=params)
            else:
                return await commands.getone(context, request, params.model, params.model.backend, action=Action.GETONE, params=params)
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
            return await commands.getall(context, request, params.model, params.model.backend, action=action, params=params)
    else:
        context.attach('transaction', manifest.backend.transaction, write=True)
        action = METHOD_TO_ACTION[request.method]
        if params.prop and params.propref:
            return await commands.push(context, request, params.prop, params.model.backend, action=action, params=params)
        elif params.prop:
            return await commands.push(context, request, params.prop, params.prop.backend, action=action, params=params)
        else:
            return await commands.push(context, request, params.model, params.model.backend, action=action, params=params)


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
