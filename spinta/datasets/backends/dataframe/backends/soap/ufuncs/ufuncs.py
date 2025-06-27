from __future__ import annotations

from typing import Any

from spinta.auth import authorized
from spinta.components import Property
from spinta.core.enums import Action
from spinta.core.ufuncs import ufunc, Expr, Bind, ShortExpr
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder
from spinta.datasets.components import Param
from spinta.utils.data import take
from spinta.utils.schema import NA


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
        return

    if not isinstance(prop.external.prepare, Expr):
        # Ignore properties without `prepare` expression
        return

    resource_param = env(this=prop).resolve(prop.external.prepare)
    env.call("soap_request_body", prop, resource_param)


@ufunc.resolver(SoapQueryBuilder, Property, Param)
def soap_request_body(env: SoapQueryBuilder, prop: Property, param: Param) -> None:
    # Replace default param.soap_body values with url_param values
    url_param_value = env.query_params.url_params.get(prop.place)
    param_soap_body = {
        key: url_param_value or (value if value is not NA else None)
        for key, value in param.soap_body.items()
    }

    # param row can only have one value, so we take this value
    param_value = list(param_soap_body.values())[0] if param_soap_body else None

    env.soap_request_body.update(param_soap_body)
    env.property_values.update({param.name: param_value})
