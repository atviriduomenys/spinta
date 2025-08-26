import json
import uuid
from typing import Type

import logging

from authlib.common.errors import AuthlibHTTPError
from authlib.oauth2.rfc6750.errors import InsufficientScopeError
from pydantic import ValidationError

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles
from starlette.routing import Route, Mount
from starlette.middleware import Middleware

from spinta import components, commands
from spinta.api.validators import ClientAddData, ClientPatchData, ClientSecretPatchData
from spinta.auth import (
    AuthorizationServer,
    check_scope,
    query_client,
    get_clients_list,
    client_exists,
    create_client_file,
    delete_client_file,
    update_client_file,
    get_clients_path,
    Scopes,
    authenticate_token,
)
from spinta.auth import ResourceProtector
from spinta.auth import BearerTokenValidator
from spinta.auth import get_auth_request
from spinta.auth import get_auth_token
from spinta.commands import prepare, get_version
from spinta.components import Context, UrlParams
from spinta.exceptions import BaseError, MultipleErrors, error_response, InsufficientPermission, ClientValidationError
from spinta.middlewares import ContextMiddleware
from spinta.urlparams import Version
from spinta.urlparams import get_response_type
from spinta.accesslog import create_accesslog
from spinta.exceptions import NoAuthServer
from spinta.utils.path import resource_filename

log = logging.getLogger(__name__)

templates = Jinja2Templates(
    directory=str(resource_filename("spinta", "templates")),
)


async def favicon(request: Request):
    return Response("", media_type="text/plain", status_code=404)


async def robots(request: Request):
    content = "\n".join(
        [
            "User-Agent: *",
            "Allow: /",
            "",
        ]
    )
    return Response(content, media_type="text/plain", status_code=200)


async def version(request: Request):
    version = get_version(request.state.context)
    return JSONResponse(version)


async def auth_token(request: Request):
    context = request.state.context
    auth_server = context.get("auth.server")
    if auth_server.enabled():
        resp: JSONResponse = auth_server.create_token_response(
            {
                "method": request.method,
                "url": str(request.url.replace(query="")),
                "body": dict(await request.form()),
                "headers": request.headers,
            }
        )

        payload = json.loads(resp.body.decode("utf-8"))
        _auth_accesslog(context, request, payload, "json")

        return resp
    else:
        raise NoAuthServer()


def _auth_accesslog(context: Context, request: Request, payload: dict, output_format: str):
    access_token = payload.get("access_token")
    if not access_token:
        return

    token = payload["access_token"]
    type_ = payload["token_type"]

    resource_protector = context.get("auth.resource_protector")
    token = authenticate_token(resource_protector, token=token, type_=type_)
    params = UrlParams()
    params.format = output_format
    context.attach("accesslog", create_accesslog, context, loaders=(context.get("store"), request, token, params))
    accesslog = context.get("accesslog")
    accesslog.auth()


def _auth_client_context(request: Request) -> Context:
    context: Context = request.state.context
    context.set(
        "auth.request",
        get_auth_request(
            {
                "method": request.method,
                "url": str(request.url.replace(query="")),
                "body": None,
                "headers": request.headers,
            }
        ),
    )
    context.set("auth.token", get_auth_token(context))
    context.attach(
        "accesslog",
        create_accesslog,
        context,
        loaders=(
            context.get("store"),
            context.get("auth.token"),
        ),
    )
    return context


async def auth_clients_add(request: Request) -> JSONResponse:
    context = _auth_client_context(request)

    try:
        check_scope(context, Scopes.AUTH_CLIENTS)
    except InsufficientScopeError:
        raise InsufficientPermission(scope=Scopes.AUTH_CLIENTS)

    config = context.get("config")
    commands.load(context, config)

    data = await request.json()
    try:
        client_data = ClientAddData.model_validate(data).model_dump(exclude_unset=True)
    except ValidationError as e:
        if "secret" in data.keys():
            data["secret"] = "..."
        error_dict = {",".join(err["loc"]): err["msg"] for err in e.errors()}
        raise ClientValidationError(
            request_data=data, errors=error_dict, inputs=list(ClientAddData.model_fields.keys())
        )

    client_id = str(uuid.uuid4())
    name = client_data.get("client_name") or client_id

    path = get_clients_path(config)
    while client_exists(path, client_id):
        client_id = str(uuid.uuid4())

    client_file, client_ = create_client_file(
        path,
        name,
        client_id,
        client_data["secret"],
        client_data.get("scopes"),
        client_data.get("backends"),
    )

    return JSONResponse(
        {
            "client_id": client_["client_id"],
            "client_name": name,
            "scopes": client_["scopes"],
            "backends": client_["backends"],
        }
    )


async def auth_clients_get_all(request: Request) -> JSONResponse:
    context = _auth_client_context(request)
    try:
        check_scope(context, Scopes.AUTH_CLIENTS)
    except InsufficientScopeError:
        raise InsufficientPermission(scope=Scopes.AUTH_CLIENTS)

    config = context.get("config")
    commands.load(context, config)

    path = get_clients_path(config)
    ids = get_clients_list(path)
    return_values = []
    for client_path in ids:
        client = query_client(path, client_path)
        return_values.append({"client_id": client.id, "client_name": client.name})
    return JSONResponse(return_values)


async def auth_clients_get_specific(request: Request) -> JSONResponse:
    context = _auth_client_context(request)
    token = context.get("auth.token")
    config = context.get("config")
    commands.load(context, config)

    try:
        client_id = request.path_params["client"]
        if client_id != token.get_client_id():
            check_scope(context, Scopes.AUTH_CLIENTS)
    except InsufficientScopeError:
        raise InsufficientPermission(scope=Scopes.AUTH_CLIENTS)

    path = get_clients_path(config)
    client = query_client(path, client_id)
    return_value = {"client_id": client.id, "client_name": client.name, "scopes": list(client.scopes)}
    return JSONResponse(return_value)


async def auth_clients_delete_specific(request: Request) -> Response:
    try:
        context = _auth_client_context(request)
        check_scope(context, Scopes.AUTH_CLIENTS)
    except InsufficientScopeError:
        raise InsufficientPermission(scope=Scopes.AUTH_CLIENTS)

    config = context.get("config")
    commands.load(context, config)

    path = get_clients_path(config)
    client_id = request.path_params["client"]
    delete_client_file(path, client_id)
    return Response(status_code=204)


async def auth_clients_patch_specific(request: Request) -> JSONResponse:
    context = _auth_client_context(request)
    token = context.get("auth.token")
    client_id = request.path_params["client"]

    validator_class = ClientPatchData
    try:
        check_scope(context, Scopes.AUTH_CLIENTS)
    except InsufficientScopeError:
        validator_class = ClientSecretPatchData
        # Requests without AUTH_CLIENTS scope can only change its own secret
        if client_id != token.get_client_id():
            raise InsufficientPermission(scope=Scopes.AUTH_CLIENTS)

    config = context.get("config")
    commands.load(context, config)
    path = get_clients_path(config)

    data = await request.json()
    try:
        client_data = validator_class.model_validate(data).model_dump(exclude_unset=True)
    except ValidationError as e:
        if "secret" in data.keys():
            data["secret"] = "..."
        error_dict = {",".join(err["loc"]): err["msg"] for err in e.errors()}
        raise ClientValidationError(
            request_data=data, errors=error_dict, inputs=list(validator_class.model_fields.keys())
        )

    new_data = update_client_file(
        context,
        path,
        client_id,
        name=client_data.get("client_name", None),
        secret=client_data.get("secret", None),
        scopes=client_data.get("scopes", None),
        backends=client_data.get("backends", None),
    )
    return_value = {
        "client_id": new_data["client_id"],
        "client_name": new_data["client_name"],
        "scopes": list(new_data["scopes"]),
        "backends": new_data["backends"],
    }
    return JSONResponse(return_value)


async def homepage(request: Request):
    from spinta.utils.response import create_http_response

    context: Context = request.state.context

    context.set(
        "auth.request",
        get_auth_request(
            {
                "method": request.method,
                "url": str(request.url.replace(query="")),
                "body": None,
                "headers": request.headers,
            }
        ),
    )
    context.set("auth.token", get_auth_token(context))

    config = context.get("config")
    UrlParams: Type[components.UrlParams]
    UrlParams = config.components["urlparams"]["component"]
    store = context.get("store")

    # Currently need to initialize the manifest and then add missing models
    # otherwise, manifest never gets created and becomes infinite loop
    manifest = commands.create_request_manifest(context, store.manifest)
    context.set("request.manifest", manifest)
    commands.load_for_request(context, manifest)
    commands.reload_backend_metadata(context, manifest, manifest.backend)
    params: UrlParams = prepare(context, UrlParams(), Version(), request)
    context.attach(
        "accesslog",
        create_accesslog,
        context,
        loaders=(
            store,
            context.get("auth.token"),
            request,
            params,
        ),
    )
    return await create_http_response(context, params, request)


async def error(request, exc):
    log.exception("Error: %s", exc)

    headers = {}

    if isinstance(exc, MultipleErrors):
        # TODO: probably there should be a more sophisticated way to get status
        #       code from error list.
        # XXX:  On the other hand, mixing status code is probably not a good
        #       idea, so instead, MultipleErrors must have one status code for
        #       all errors. And that should be enforced by MultipleErrors. If
        #       an exception comes in with a different status code, then this
        #       should be an error.
        status_code = exc.errors[0].status_code
        errors = [error_response(e) for e in exc.errors]

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
                # show overridden description
                message = str(f"{exc.error}: {exc.description}")
            elif exc.get_error_description():
                # show dynamic description
                message = str(f"{exc.error}: {exc.get_error_description()}")
            else:
                # if no description, show plain error string
                message = exc.error
            headers["www-authenticate"] = f'Bearer error="{exc.error}"'
        else:
            status_code = 500
            message = str(exc)

        errors = [
            {
                "code": type(exc).__name__,
                "message": message,
            }
        ]

    response = {"errors": errors}

    fmt = get_response_type(request.state.context, request)
    if fmt == "json" or fmt is None:
        return JSONResponse(
            response,
            status_code=status_code,
            headers=headers,
        )
    else:
        response = {
            **response,
            "request": request,
        }

        return templates.TemplateResponse(
            "error.html",
            response,
            status_code=status_code,
            headers=headers,
        )


async def srid_check(request: Request):
    from shapely.geometry import Point
    from spinta.backends.postgresql.types.geometry.helpers import get_osm_link

    srid = request.path_params["srid"]
    x = request.path_params["x"]
    y = request.path_params["y"]

    point = Point(x, y)
    osm_link = get_osm_link(point, srid)
    return RedirectResponse(url=osm_link)


def init(context: Context):
    config = context.get("config")

    routes = [
        Route("/robots.txt", robots, methods=["GET"]),
        Route("/favicon.ico", favicon, methods=["GET"]),
        Route("/version", version, methods=["GET"]),
        Route("/auth/token", auth_token, methods=["POST"]),
        Route("/_srid/{srid:int}/{x:spinta_float}/{y:spinta_float}", srid_check, methods=["GET"]),
        Route("/auth/clients", auth_clients_get_all, methods=["GET"]),
        Route("/auth/clients", auth_clients_add, methods=["POST"]),
        Route("/auth/clients/{client}", auth_clients_get_specific, methods=["GET"]),
        Route("/auth/clients/{client}", auth_clients_delete_specific, methods=["DELETE"]),
        Route("/auth/clients/{client}", auth_clients_patch_specific, methods=["PATCH"]),
    ]

    if config.docs_path:
        routes += [
            Mount("/docs", StaticFiles(directory=config.docs_path, html=True)),
        ]

    # This route matches everything, so it must be added last.
    routes += [
        Route("/{path:path}", homepage, methods=["HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"]),
    ]

    middleware = [Middleware(ContextMiddleware, context=context)]

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

    context.bind("auth.server", AuthorizationServer, context)
    context.bind(
        "auth.resource_protector",
        ResourceProtector,
        context,
        BearerTokenValidator,
    )

    return app
