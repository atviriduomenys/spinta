import pkg_resources as pres
import uvicorn

from starlette.applications import Starlette
from starlette.templating import Jinja2Templates

from spinta.commands import prepare
from spinta.urlparams import Version


templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))

app = Starlette()

context = None

COLORS = {
    'change': '#B2E2AD',
    'null': '#C1C1C1',
}


@app.route('/{path:path}')
async def homepage(request):
    global context

    url_path = request.path_params['path'].strip('/')

    with context.enter():
        config = context.get('config')
        store = context.get('store')
        manifest = store.manifests['default']
        context.bind('transaction', manifest.backend.transaction)

        # get UrlParams parser class from configs
        UrlParams = config.components['urlparams']['component']

        url_params = UrlParams(
            path=url_path,
            method=request.method,
            headers=request.headers,
        )

        # parse url params using prepare command
        url_params = prepare(context, url_params, Version())

        # get UrlResponse class from config
        UrlResponse = config.components['urlresponse']['component']
        url_response = UrlResponse(url_params, request)

        # get response using prepare command
        url_response = prepare(context, url_response)

        return url_response.response


def run(context):
    set_context(context)
    app.debug = context.get('config').debug
    uvicorn.run(app, host='0.0.0.0', port=8000)


def set_context(context_):
    global context
    if context is not None:
        raise Exception("Context was already set.")
    context = context_
