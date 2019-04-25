import pkg_resources as pres
import uvicorn

from starlette.applications import Starlette
from starlette.templating import Jinja2Templates

from spinta.commands import prepare
from spinta.urlparams import Version
from spinta.utils.response import create_http_response


templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))

app = Starlette()

context = None


@app.route('/{path:path}', methods=['GET', 'POST'])
async def homepage(request):
    global context

    with context.enter():
        config = context.get('config')
        UrlParams = config.components['urlparams']['component']
        params = prepare(context, UrlParams(), Version(), request)
        return await create_http_response(context, params, request)


def run(context):
    set_context(context)
    app.debug = context.get('config').debug
    uvicorn.run(app, host='0.0.0.0', port=8000)


def set_context(context_):
    global context
    if context is not None:
        raise Exception("Context was already set.")
    context = context_
