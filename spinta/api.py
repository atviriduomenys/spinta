import pkg_resources as pres
import uvicorn
import logging

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.templating import Jinja2Templates

from spinta.commands import prepare
from spinta.urlparams import Version
from spinta.utils.response import create_http_response
from spinta.utils.response import get_response_type

log = logging.getLogger(__name__)

templates = Jinja2Templates(
    directory=pres.resource_filename('spinta', 'templates'),
)

app = Starlette()

context = None


@app.route('/auth/token', methods=['POST'])
async def auth_token(request):
    global context

    auth = context.get('auth')
    return auth.create_token_response({
        'method': request.method,
        'url': str(request.url),
        'body': dict(await request.form()),
        'headers': request.headers,
    })


@app.route('/{path:path}', methods=['GET', 'POST'])
async def homepage(request):
    global context

    with context.enter():
        config = context.get('config')
        UrlParams = config.components['urlparams']['component']
        params = prepare(context, UrlParams(), Version(), request)
        return await create_http_response(context, params, request)


@app.exception_handler(Exception)
@app.exception_handler(HTTPException)
async def http_exception(request, exc):
    global context

    if isinstance(exc, HTTPException):
        status_code = exc.status_code
        error = exc.detail
    else:
        status_code = 500
        error = str(exc)

    response = {
        "error": error,
    }

    fmt = get_response_type(context, request, request)
    if fmt == 'json':
        return JSONResponse(response, status_code=status_code)
    else:
        templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
        response = {
            **response,
            'request': request,
        }
        return templates.TemplateResponse('error.html', response, status_code=status_code)


def run(context):
    set_context(context)
    app.debug = context.get('config').debug
    uvicorn.run(app, host='0.0.0.0', port=8000)


def set_context(context_):
    global context
    if context is not None:
        raise Exception("Context was already set.")
    context = context_

    store = context.get('store')
    manifest = store.manifests['default']
    log.info("Spinta has started!")
    log.info('manifest: %s', manifest.path.resolve())
