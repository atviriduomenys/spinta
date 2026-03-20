from __future__ import annotations

from typing import Any

from lxml import etree

from spinta.adapters.soap_plugins import get_deferred_prepare_names
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


class MakeCDATA:
    """
    Small helper class for making CDATA objects. Instances of this class are passed to dask. Passing etree.CDATA
    objects directly doesn't work because etree.CDATA objects are not hashable and that breaks dask
    """

    data: str

    def __init__(self, data: str) -> None:
        self.data = data

    def __call__(self) -> etree.CDATA:
        return etree.CDATA(self.data)


@ufunc.resolver(SoapQueryBuilder, Expr)
def select(env: SoapQueryBuilder, expr: Expr) -> Expr:
    return expr


@ufunc.resolver(SoapQueryBuilder, Bind, object, name="eq")
def eq_(env: SoapQueryBuilder, field: Bind, value: object) -> Expr | None:
    try:
        prop = env.resolve_property(field)
    except PropertyNotFound:
        # leave query parameter for other resolvers
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
    deferred_names = get_deferred_prepare_names()
    for param_body_key, param_body_value in env.soap_request_body.items():
        if not isinstance(param_body_value, Expr):
            continue
        if getattr(param_body_value, "name", None) in deferred_names:
            continue
        env.soap_request_body[param_body_key] = env.resolve(param_body_value)


def _param_for_soap_body_key(env: SoapQueryBuilder, param_source: str) -> Param | None:
    for param in env.params.values():
        soap_body = getattr(param, "soap_body", None)
        if soap_body and param_source in soap_body:
            return param
    return None


def _resolve_deferred_soap_request_body_exprs(env: SoapQueryBuilder) -> None:
    """Resolve adapter-deferred prepares after the full SOAP body is assembled."""
    deferred_names = get_deferred_prepare_names()
    if not deferred_names:
        return
    for param_source, value in list(env.soap_request_body.items()):
        if not isinstance(value, Expr):
            continue
        if getattr(value, "name", None) not in deferred_names:
            continue
        resolved = env.resolve(value)
        param = _param_for_soap_body_key(env, param_source)
        if param and getattr(param, "soap_body_value_type", None) == "cdata" and resolved:
            env.soap_request_body[param_source] = MakeCDATA(resolved)
        else:
            env.soap_request_body[param_source] = resolved
        if param is not None:
            env.property_values[param.name] = resolved


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
    _resolve_deferred_soap_request_body_exprs(env)


@ufunc.resolver(SoapQueryBuilder, Property)
def soap_request_body(env: SoapQueryBuilder, prop: Property) -> None:
    """
    Only care for properties that describe URL query parameters:
        properties without `source` and with `prepare`.
    """
    if prop.external.name:
        return None

    if not isinstance(prop.external.prepare, Expr):
        return None

    resource_param = env(this=prop).resolve(prop.external.prepare)

    env.call("soap_request_body", prop, resource_param)


def _get_final_soap_request_body_value(env: SoapQueryBuilder, property_name: str, param_source: str) -> Any:
    """If value in URL - use it (even if it's None). If not - use whatever default is given in DSA"""
    url_value = env.query_params.url_params.get(property_name, NA)
    param_default_value = env.soap_request_body.get(param_source, NA)

    return url_value if url_value is not NA else param_default_value


@ufunc.resolver(SoapQueryBuilder, Property, Param)
def soap_request_body(env: SoapQueryBuilder, prop: Property, param: Param) -> None:
    """Merge URL query params into the SOAP body; resolve non-deferred Expr; keep deferred Expr for a later pass."""
    soap_body = getattr(param, "soap_body", None)
    if not soap_body:
        return
    param_source = next(iter(soap_body))
    deferred_names = get_deferred_prepare_names()

    # Single source of truth (same as pre-adapter behaviour): URL wins over manifest default.
    final_value = _get_final_soap_request_body_value(env, prop.place, param_source)

    if isinstance(final_value, Expr):
        if getattr(final_value, "name", None) in deferred_names:
            pass
        else:
            final_value = env.resolve(final_value)

    is_deferred_expr = isinstance(final_value, Expr) and getattr(final_value, "name", None) in deferred_names

    if final_value is NA:
        env.soap_request_body.pop(param_source, None)
        final_value = None
    elif is_deferred_expr:
        env.soap_request_body[param_source] = final_value
    else:
        if param.soap_body_value_type == "cdata" and final_value:
            soap_final_value = MakeCDATA(final_value)
        else:
            soap_final_value = final_value
        env.soap_request_body[param_source] = soap_final_value

    if prop.dtype.required and param_source not in env.soap_request_body:
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
