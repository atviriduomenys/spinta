import pkg_resources as pres
import logging

from authlib.common.errors import AuthlibHTTPError

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles

from spinta.auth import get_auth_request
from spinta.auth import get_auth_token
from spinta.commands import prepare, get_version
from spinta.exceptions import BaseError
from spinta.urlparams import Version
from spinta.utils.response import create_http_response
from spinta.urlparams import get_response_type
from spinta import commands, components
from spinta.auth import AuthorizationServer, ResourceProtector, BearerTokenValidator
from spinta.config import create_context
from spinta.middlewares import ContextMiddleware

log = logging.getLogger(__name__)

templates = Jinja2Templates(
    directory=pres.resource_filename('spinta', 'templates'),
)

app = Starlette()
app.add_middleware(ContextMiddleware)


def _load_context(context: components.Context):
    rc = context.get('config.raw')

    config = context.set('config', components.Config())
    store = context.set('store', components.Store())

    commands.load(context, config, rc)
    commands.check(context, config)
    commands.load(context, store, rc)
    commands.check(context, store)

    commands.wait(context, store, rc)

    prepare(context, store.internal)
    prepare(context, store)

    context.set('auth.server', AuthorizationServer(context))
    context.set('auth.resource_protector', ResourceProtector(context, BearerTokenValidator))


@app.on_event('startup')
async def create_app_context():
    context = create_context()
    _load_context(context)

    config = context.get('config')
    app.debug = config.debug
    app.state.context = context

    if config.docs_path:
        app.mount('/docs', StaticFiles(directory=config.docs_path, html=True))

    # This reoute matches everything, so it must be added last.
    app.add_route('/{path:path}', homepage, methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])


@app.route('/version', methods=['GET'])
async def version(request: Request):
    version = get_version(request.state.context)
    return JSONResponse(version)


@app.route('/auth/token', methods=['POST'])
async def auth_token(request: Request):
    auth = request.state.context.get('auth.server')
    return auth.create_token_response({
        'method': request.method,
        'url': str(request.url),
        'body': dict(await request.form()),
        'headers': request.headers,
    })


async def homepage(request: Request):
    context = request.state.context

    context.set('auth.request', get_auth_request({
        'method': request.method,
        'url': str(request.url),
        'body': None,
        'headers': request.headers,
    }))
    context.set('auth.token', get_auth_token(context))

    config = context.get('config')

    UrlParams = config.components['urlparams']['component']
    params = prepare(context, UrlParams(), Version(), request)

    return await create_http_response(context, params, request)


@app.exception_handler(Exception)
@app.exception_handler(BaseError)
@app.exception_handler(AuthlibHTTPError)
@app.exception_handler(HTTPException)
async def http_exception(request, exc):
    log.exception('Error: %s', exc)

    error = None

    if isinstance(exc, HTTPException):
        status_code = exc.status_code
        err_message = exc.detail
    elif isinstance(exc, AuthlibHTTPError):
        status_code = exc.status_code
        if exc.description:
            # show overriden description
            err_message = str(f'{exc.error}: {exc.description}')
        elif exc.get_error_description():
            # show dynamic description
            err_message = str(f'{exc.error}: {exc.get_error_description()}')
        else:
            # if no description, show plain error string
            err_message = exc.error
    elif isinstance(exc, BaseError):
        status_code = exc.status_code
        error = exc.error()
    else:
        status_code = 500
        err_message = str(exc)

    if error is None:
        error = {
            "code": type(exc).__name__,
            "message": err_message,
        }

    response = {"errors": [error]}

    fmt = get_response_type(request.state.context, request, request)
    if fmt == 'json':
        return JSONResponse(response, status_code=status_code)
    else:
        templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
        response = {
            **response,
            'request': request,
        }
        return templates.TemplateResponse('error.html', response, status_code=status_code)
