from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional

from starlette.requests import Request

from spinta import commands
from spinta.auth import AdminToken
from spinta.commands.read import prepare_data_for_response
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.components import UrlParams
from spinta.components import Version
from spinta.formats.html.commands import build_template_context
from spinta.formats.html.components import Cell
from spinta.manifests.components import Manifest
from spinta.renderer import render
from spinta.typing import ObjectData


def make_get_request(
    path: str,
    query: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    method: str = 'GET',
) -> Request:
    if not path.startswith('/'):
        path = '/' + path
    req = {
        'type': 'http',
        'method': method,
        'path': path,
        'path_params': {'path': path},
        'headers': [
            (k.lower().encode(), v.encode())
            for k, v in (headers or {}).items()
        ],
    }
    if query:
        req['query_string'] = query.encode()
    return Request(req)


def render_data(
    context: Context,
    manifest: Manifest,
    path: str,
    query: Optional[str],
    data: ObjectData,
    *,
    method: str = 'GET',
    accept: str = 'application/json',
    headers: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    context.set('auth.token', AdminToken())

    if headers is None:
        headers = {}
    headers['Accept'] = accept
    request = make_get_request(path, query, headers, method)
    params: UrlParams = commands.prepare(
        context,
        UrlParams(),
        Version(),
        request,
    )
    action = params.action
    model = params.model

    data = next(prepare_data_for_response(context, model, action, params, data, reserved=[
        '_type',
        '_id',
        '_revision'
    ]))

    if params.action in (Action.GETALL, Action.SEARCH):
        data = [data]

    if params.format == 'html':
        resp = _get_html_template_context(context, model, action, params, data)
    else:
        resp = render(context, request, model, params, data, action=action)

    return resp


def _get_html_template_context(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    rows: Iterable[Dict[str, Cell]],
):
    ctx = build_template_context(context, model, action, params, rows)
    resp = next(ctx['data'], None)
    if resp is not None:
        resp = dict(zip(ctx['header'], resp))
    return resp
