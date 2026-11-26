from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from spinta import commands
from spinta.components import Context, Node, UrlParams
from spinta.core.enums import Action
from spinta.formats.json.components import Json
from spinta.formats.jsonld.helpers import (
    enrich_object,
    enrich_objects,
    jsonld_objects_enabled,
    protocol_context_value,
)
from spinta.types.text.components import Text
from spinta.utils.response import async_response_iterator, peek_and_stream


@commands.render.register(Context, Request, Node, Json)
def render(
    context: Context,
    request: Request,
    node: Node,
    fmt: Json,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: dict | None = None,
):
    return _render(
        context,
        request,
        fmt,
        action,
        params,
        data,
        status_code,
        headers,
        node,
    )


def _render(
    context: Context,
    request: Request,
    fmt: Json,
    action: Action,
    params: UrlParams,
    data,
    status_code: int,
    headers: dict,
    node: Node = None,
):
    if action in (Action.GETALL, Action.SEARCH, Action.CHANGES):
        # In python dict is also an iterable, but here we want a true iterable,
        # a list or a generator of dicts.
        assert not isinstance(data, dict), data
        context_value = None
        # `CHANGES` (LDES) is not covered by JSON-LD (spec section 7), so it keeps its body.
        if action != Action.CHANGES and jsonld_objects_enabled(context, node, params):
            # Each item is enriched (4.2), and the top-level `@context` makes `_data` a graph (10.4).
            data = enrich_objects(context, node, data)
            context_value = protocol_context_value(context)
        return StreamingResponse(
            async_response_iterator(peek_and_stream(fmt(data, context_value=context_value))),
            status_code=status_code,
            media_type=fmt.content_type,
            headers=headers,
        )
    elif action == Action.DELETE:
        return Response(None, status_code=status_code, headers=headers)
    else:
        if action == Action.GETONE and jsonld_objects_enabled(context, node, params):
            # A single object of a model (rule 3). Sub-resources have no model as `node`, so they are not enriched.
            data = enrich_object(context, node, data)
        return JSONResponse(fmt.data(data), status_code=status_code, headers=headers, media_type=fmt.content_type)


@commands.prepare_dtype_for_response.register(Context, Json, Text, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Json,
    dtype: Text,
    value: dict,
    *,
    data: dict[str, Any],
    action: Action,
    select: dict = None,
):
    if "C" in value:
        value[""] = value.pop("C")

    if len(value) == 1 and select is not None:
        for key, data in value.items():
            key = "C" if key == "" else key
            if key not in select.keys():
                return data

    return value
