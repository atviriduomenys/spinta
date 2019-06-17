from starlette.requests import Request

from spinta.components import Context, Action, UrlParams, Node
from spinta import commands


def render(
    context: Context,
    request: Request,
    node: Node,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
):
    config = context.get('config')
    fmt = config.exporters[params.format]

    # TODO:
    #media_type = exporter.content_type
    #_params = {
    #    k: v for k, v in params.params.items()
    #    if k in exporter.params
    #}
    #stream = aiter(exporter(rows, action, **_params))
    #return StreamingResponse(stream, media_type=media_type, status_code=status_code)

    if node:
        return commands.render(context, request, node, fmt, action=action, params=params, data=data, status_code=status_code)
    else:
        return commands.render(context, request, fmt, action=action, params=params, data=data, status_code=status_code)
