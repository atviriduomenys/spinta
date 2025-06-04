from __future__ import annotations

from typing import Any

from spinta.auth import authorized
from spinta.components import Property
from spinta.core.enums import Action
from spinta.core.ufuncs import ufunc, Expr, Bind, ShortExpr
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder
from spinta.dimensions.param.components import ResolvedResourceParam
from spinta.utils.data import take


@ufunc.resolver(SoapQueryBuilder, Expr)
def select(env: SoapQueryBuilder, expr: Expr) -> Expr:
    return expr


@ufunc.resolver(SoapQueryBuilder, Bind, str, name="eq")
def eq_(env: SoapQueryBuilder, field: Bind, value: str) -> None:
    prop = env.resolve_property(field)

    if not isinstance(prop.external.prepare, Expr):
        return

    env.query_params.url_params[prop.place] = value


@ufunc.resolver(SoapQueryBuilder, Expr, name="and")
def and_(env: SoapQueryBuilder, expr: Expr) -> list[Any]:
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call('and', args)


@ufunc.resolver(SoapQueryBuilder, list, name='and')
def and_(env: SoapQueryBuilder, args: list) -> Any:
    if len(args) > 1:
        return ShortExpr("and", *args)
    elif args:
        return args[0]


@ufunc.resolver(SoapQueryBuilder)
def soap_request_body(env: SoapQueryBuilder) -> None:
    """
    Loops through all model properties and populates env.soap_request_body
    using variables from resource.param and URL parameters
    """
    for prop in take(env.model.properties).values():
        if not authorized(env.context, prop, Action.GETALL):
            continue
        env.call("soap_request_body", prop)


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

    resource_property_value = env(this=prop).resolve(prop.external.prepare)

    env.call("soap_request_body", prop, resource_property_value)


@ufunc.resolver(SoapQueryBuilder, Property, ResolvedResourceParam)
def soap_request_body(env: SoapQueryBuilder, prop: Property, resolved_resource_param: ResolvedResourceParam) -> None:
    value = env.query_params.url_params.get(prop.place, resolved_resource_param.value)

    env.soap_request_body.update({resolved_resource_param.source: value})
    env.property_values.update({resolved_resource_param.target: value})
