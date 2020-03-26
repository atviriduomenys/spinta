from typing import Type

import pkg_resources as pres
import logging

from authlib.common.errors import AuthlibHTTPError

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles
from starlette.routing import Route, Mount
from starlette.middleware import Middleware

from spinta import components
from spinta.auth import AuthorizationServer, ResourceProtector, BearerTokenValidator
from spinta.auth import get_auth_request
from spinta.auth import get_auth_token
from spinta.commands import init, get_version
from spinta.components import Context
from spinta.exceptions import BaseError, MultipleErrors, error_response
from spinta.middlewares import ContextMiddleware
from spinta.urlparams import Version
from spinta.urlparams import get_response_type
from spinta.utils.response import create_http_response
from spinta.accesslog import create_accesslog

log = logging.getLogger(__name__)

templates = Jinja2Templates(
    directory=pres.resource_filename('spinta', 'templates'),
)


async def favicon(request: Request):
    return Response('', media_type='text/plain', status_code=404)


async def version(request: Request):
    version = get_version(request.state.context)
    return JSONResponse(version)


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
    context.attach('accesslog', create_accesslog, context, loaders=(
        context.get('store'),
        context.get("auth.token"),
        request,
    ))

    config = context.get('config')
    UrlParams: Type[components.UrlParams] = config.components['urlparams']['component']
    params = init(context, UrlParams(), Version(), request)

    return await create_http_response(context, params, request)


async def error(request, exc):
    log.exception('Error: %s', exc)

    headers = {}

    if isinstance(exc, MultipleErrors):
        # TODO: probably there should be a more sophisticated way to get status
        #       code from error list.
        # XXX:  On the other hand, mxing status code is probably not a good
        #       idea, so instead, MultipleErrors must have one status code for
        #       all errros. And that should be inforced by MultipleErrors. If an
        #       exception comes in with a different status code, then this
        #       dhould be an error.
        status_code = exc.errors[0].status_code
        errors = [
            error_response(error)
            for error in exc.errors
        ]

    elif isinstance(exc, BaseError):
        status_code = exc.status_code
        errors = [error_response(exc)]
        headers = exc.headers
    else:
        if isinstance(exc, HTTPException):
            status_code = exc.status_code
            message = exc.detail
        elif isinstance(exc, AuthlibHTTPError):
            status_code = exc.status_code
            if exc.description:
                # show overriden description
                message = str(f'{exc.error}: {exc.description}')
            elif exc.get_error_description():
                # show dynamic description
                message = str(f'{exc.error}: {exc.get_error_description()}')
            else:
                # if no description, show plain error string
                message = exc.error
            headers['www-authenticate'] = f'Bearer error="{exc.error}"'
        else:
            status_code = 500
            message = str(exc)

        errors = [
            {
                'code': type(exc).__name__,
                'message': message,
            }
        ]

    response = {'errors': errors}

    fmt = get_response_type(request.state.context, request)
    if fmt == 'json':
        return JSONResponse(
            response,
            status_code=status_code,
            headers=headers,
        )
    else:
        response = {
            **response,
            'request': request,
        }
        return templates.TemplateResponse(
            'error.html',
            response,
            status_code=status_code,
            headers=headers,
        )


def init(context: Context):
    config = context.get('config')

    routes = [
        Route('/favicon.ico', favicon, methods=['GET']),
        Route('/version', version, methods=['GET']),
        Route('/auth/token', auth_token, methods=['POST'])
    ]

    if config.docs_path:
        routes += [
            Mount('/docs', StaticFiles(directory=config.docs_path, html=True)),
        ]

    # This reoute matches everything, so it must be added last.
    routes += [
        Route('/{path:path}', homepage, methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE']),
    ]

    middleware = [
        Middleware(ContextMiddleware)
    ]

    exception_handlers = {
        Exception: error,
        BaseError: error,
        MultipleErrors: error,
        AuthlibHTTPError: error,
        HTTPException: error,
    }

    app = Starlette(
        debug=config.debug,
        routes=routes,
        middleware=middleware,
        exception_handlers=exception_handlers,
    )

    # Add context to state in order to pass it to request handlers
    app.state.context = context

    context.bind('auth.server', AuthorizationServer, context)
    context.bind('auth.resource_protector', ResourceProtector, context, BearerTokenValidator)

    config.rc.dump(
        'env',
        'backends',
        'manifests',
        'manifest',
        'accesslog',
        'default_auth_client',
        to=log.info,
    )

    return app
