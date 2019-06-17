from starlette.exceptions import HTTPException
from starlette.requests import Request

from spinta import commands
from spinta.components import Context, Action, UrlParams
from spinta.renderer import render


@commands.contents.register()
async def contents(context: Context, request: Request, *, params: UrlParams):
    if request.method != 'GET':
        raise HTTPException(status_code=400, detail="Unsupported http method {requet.method!r}.")

    store = context.get('store')
    manifest = store.manifests['default']

    if params.path in manifest.endpoints:
        name = manifest.endpoints[params.path]
    else:
        name = params.path

    node = None
    return render(context, request, node, Action.CONTENTS, params, {
        'contents': manifest.tree[name],
        'datasets': [],
    })
