from __future__ import annotations

from typing import Any

from spinta.auth import authorized, query_client
from spinta.components import Property
from spinta.core.enums import Action
from spinta.core.ufuncs import ufunc, Expr, Bind, ShortExpr
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder
from spinta.datasets.components import Param
from spinta.exceptions import (
    InvalidClientBackendCredentials,
    InvalidClientBackend,
    UnknownMethod,
    MissingRequiredProperty,
    PropertyNotFound,
)
from spinta.utils.config import get_clients_path
from spinta.utils.data import take
from spinta.utils.schema import NA


@ufunc.resolver(SoapQueryBuilder, Expr)
def select(env: SoapQueryBuilder, expr: Expr) -> Expr:
    return expr


@ufunc.resolver(SoapQueryBuilder, Bind, object, name="eq")
def eq_(env: SoapQueryBuilder, field: Bind, value: object) -> Expr | None:
    try:
        prop = env.resolve_property(field)
    except PropertyNotFound:
        # leave query parameter for other query builders to resolve
        return Expr("eq", field, value)

    if not isinstance(prop.external.prepare, Expr):
        return Expr("eq", field, value)

    env.query_params.url_params[prop.place] = str(value)


@ufunc.resolver(SoapQueryBuilder, Expr, name="and")
def and_(env: SoapQueryBuilder, expr: Expr) -> list[Any]:
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call("and", args)


@ufunc.resolver(SoapQueryBuilder, list, name="and")
def and_(env: SoapQueryBuilder, args: list) -> Any:
    if len(args) > 1:
        return ShortExpr("and", *args)
    elif args:
        return args[0]


def _finalize_soap_request_body_resolve(env: SoapQueryBuilder) -> None:
    for param_body_key, param_body_value in env.soap_request_body.items():
        if not isinstance(param_body_value, Expr):
            continue
        env.soap_request_body[param_body_key] = env.resolve(param_body_value)


def _populate_soap_request_body_with_url_values(env: SoapQueryBuilder) -> None:
    for prop in take(env.model.properties).values():
        if not authorized(env.context, prop, Action.GETALL):
            continue
        env.call("soap_request_body", prop)


@ufunc.resolver(SoapQueryBuilder)
def soap_request_body(env: SoapQueryBuilder) -> None:
    """
    Populates env.soap_request_body looping through properties and through resource params
    """
    for param in env.params.values():
        if not hasattr(param, "soap_body"):
            continue
        env.soap_request_body.update(param.soap_body)

    _finalize_soap_request_body_resolve(env)
    _populate_soap_request_body_with_url_values(env)


@ufunc.resolver(SoapQueryBuilder, Property)
def soap_request_body(env: SoapQueryBuilder, prop: Property) -> None:
    """
    Only care about properties that describe URL query parameters:
        properties without `source` and with `prepare`.
    We ignore the rest.
    """
    if prop.external.name:
        # Ignore properties with `source`
        return None

    if not isinstance(prop.external.prepare, Expr):
        # Ignore properties without `prepare` expression
        return None

    resource_param = env(this=prop).resolve(prop.external.prepare)

    env.call("soap_request_body", prop, resource_param)


@ufunc.resolver(SoapQueryBuilder, Property, Param)
def soap_request_body(env: SoapQueryBuilder, prop: Property, param: Param) -> None:
    """Replace default param.soap_body values with url_param values"""
    url_param_value = env.query_params.url_params.get(prop.place, NA)
    param_body_key = next(iter(param.soap_body))
    default_value = env.soap_request_body.get(param_body_key, NA)
    final_value = url_param_value if url_param_value is not NA else default_value

    if final_value is NA:
        env.soap_request_body.pop(param_body_key, None)
        final_value = None
    else:
        env.soap_request_body[param_body_key] = final_value

    if prop.dtype.required and param_body_key not in env.soap_request_body:
        raise MissingRequiredProperty(prop, prop=prop.name)

    env.property_values.update({param.name: final_value})


@ufunc.resolver(SoapQueryBuilder, str)
def creds(env: SoapQueryBuilder, credential_key: str) -> Any:
    client_config_file = query_client(
        get_clients_path(env.context.get("config")), env.context.get("auth.token").get_aud()
    )
    backend_name = env.model.external.resource.name

    client_backend = client_config_file.backends.get(backend_name)
    if not client_backend:
        raise InvalidClientBackend(backend_name=backend_name)

    try:
        credential = client_backend[credential_key]
    except KeyError:
        raise InvalidClientBackendCredentials(key=credential_key, backend_name=backend_name)

    return credential


@ufunc.resolver(SoapQueryBuilder)
def creds(env: SoapQueryBuilder) -> Any:
    raise UnknownMethod(expr="creds()", name="creds")
