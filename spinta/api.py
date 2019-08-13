import pkg_resources as pres
import uvicorn
import logging

from authlib.common.errors import AuthlibHTTPError

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.templating import Jinja2Templates
from starlette.types import Receive, Send, Scope
from starlette.staticfiles import StaticFiles

from spinta.auth import get_auth_request
from spinta.auth import get_auth_token
from spinta.commands import prepare
from spinta.config import RawConfig
from spinta.exceptions import ConflictError, DataError, NotFound
from spinta.urlparams import Version
from spinta.utils.response import create_http_response
from spinta.urlparams import get_response_type

log = logging.getLogger(__name__)

templates = Jinja2Templates(
    directory=pres.resource_filename('spinta', 'templates'),
)

app = Starlette()

context = None


class ContextMiddleware(BaseHTTPMiddleware):

    async def asgi(self, receive: Receive, send: Send, scope: Scope) -> None:
        global context

        # TODO: Temporary fix for https://github.com/encode/starlette/issues/472
        # --------------------------8<--------------------------
        self._orig_send = send
        # -------------------------->8--------------------------

        with context.enter():
            request = Request(scope, receive=receive)
            response = await self.call_next(request)
            await response(receive, send)

    # TODO: Temporary fix for https://github.com/encode/starlette/issues/472
    # --------------------------8<--------------------------
    async def call_next(self, request: Request):
        import asyncio
        from starlette.responses import StreamingResponse

        inner = self.app(dict(request))
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue()  # type: asyncio.Queue

        async def coro() -> None:
            try:
                await inner(request.receive, queue.put)
            finally:
                await queue.put(None)

        task = loop.create_task(coro())
        message = await queue.get()
        if message is None:
            task.result()
            raise RuntimeError("No response returned.")

        scope = dict(request)
        if 'http.response.template' in scope.get("extensions", {}):
            if message["type"] == "http.response.template":
                await self._orig_send(message)
                message = await queue.get()

        assert message["type"] == "http.response.start", message['type']

        async def body_stream():
            while True:
                message = await queue.get()
                if message is None:
                    break
                assert message["type"] == "http.response.body"
                yield message["body"]
            task.result()

        response = StreamingResponse(
            status_code=message["status"], content=body_stream()
        )
        response.raw_headers = message["headers"]
        return response
    # -------------------------->8--------------------------


app.add_middleware(ContextMiddleware)

# FIXME: configuration should be loaded at one of the entry points: ASGI, CLI, etc.
raw_config = RawConfig()
raw_config.read()
docs_path = raw_config.get('docs_path')

if docs_path:
    # FIXME: after upgrading to starlette >= 0.12.0, index.html can be served
    # automatically with html=True flag. Otherwise we need to explicitly
    # call for `index.html` in the URL, e.g. `/docs/index.html`. Without
    # doing this HTTP/404 will be raised.
    app.mount('/docs', StaticFiles(directory=docs_path))
    # app.mount('/docs', StaticFiles(directory="uml", html=True))


@app.route('/version', methods=['GET'])
async def version(request: Request):
    return JSONResponse({
        'api': {
            'version': '0.0.1'
        },
        'implementation': {
            'name': 'Spinta',
            'version': '0.0.1'
        },
        'build': None,
    })


@app.route('/auth/token', methods=['POST'])
async def auth_token(request: Request):
    global context

    auth = context.get('auth.server')
    return auth.create_token_response({
        'method': request.method,
        'url': str(request.url),
        'body': dict(await request.form()),
        'headers': request.headers,
    })


@app.route('/{path:path}', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
async def homepage(request: Request):
    global context

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
@app.exception_handler(NotFound)
@app.exception_handler(DataError)
@app.exception_handler(AuthlibHTTPError)
@app.exception_handler(HTTPException)
@app.exception_handler(ConflictError)
async def http_exception(request, exc):
    global context

    log.exception('Error: %s', exc)

    if isinstance(exc, HTTPException):
        status_code = exc.status_code
        error = exc.detail
    elif isinstance(exc, AuthlibHTTPError):
        status_code = exc.status_code
        if exc.description:
            # show overriden description
            error = str(f'{exc.error}: {exc.description}')
        elif exc.get_error_description():
            # show dynamic description
            error = str(f'{exc.error}: {exc.get_error_description()}')
        else:
            # if no description, show plain error string
            error = exc.error
    elif isinstance(exc, DataError):
        status_code = 400
        error = str(exc) or str(exc.message)
    elif isinstance(exc, NotFound):
        status_code = 404
        error = str(exc)
    elif isinstance(exc, ConflictError):
        status_code = 409
        error = str(exc)
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
