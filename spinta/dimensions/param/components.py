from __future__ import annotations

from typing import Any

from spinta.components import Model
from spinta.core.ufuncs import Env, Expr, Unresolved
from spinta.datasets.components import Dataset
from spinta.exceptions import ParamPropertyNotAllowed
from spinta.manifests.components import Manifest
from spinta.ufuncs.propertyresolver.components import PropertyResolver


class ParamBuilder(Env):
    model: Model
    manifest: Manifest
    params: dict
    target_param: str
    query: Expr | None


class ParamLoader(Env):
    manifest: Manifest
    dataset: Dataset


class ParamProperty(Unresolved):
    """Request-time condition `eq(prop, value)` on a source-less property linked to a resource param (`param()`).

    The value is a constant at request time: it is sent to the data source as a query parameter, not as a row filter.
    """

    def __init__(self, prop: Any, value: Any):
        self.prop = prop
        self.value = value

    def __repr__(self):
        return f"ParamProperty({self.prop.name!r}, {self.value!r})"


class ParamPropertyBuilder(Env):
    """Resolves query conditions on source-less properties linked to resource params (`param()`).

    A linked `eq()` resolves to `ParamProperty`, other conditions resolve to `None`. A linked property used more than
    once, or nested inside another function, is an error.
    """

    model: Any
    property_resolver: Any
    # Linked conditions recorded so far, in request order.
    props: list

    def init(self, model: Any) -> ParamPropertyBuilder:
        return self(
            model=model,
            props=[],
            property_resolver=PropertyResolver(context=self.context).init(model=model),
        )

    def resolve_property(self, *args, **kwargs):
        return self.property_resolver.resolve_property(*args, **kwargs)

    def resolve(self, expr: Any):
        try:
            return super().resolve(expr)
        except NotImplementedError:
            # A name with an `Expr` resolver in another builder (e.g. `any`) disables `autoargs` for that name
            # globally, so this builder may get no matching signature. Resolve arguments and use the default.
            if not isinstance(expr, Expr):
                return expr
            args, kwargs = expr.resolve(self)
            return self.default_resolver(expr, *args, **kwargs)

    def default_resolver(self, expr: Expr, *args, **kwargs):
        # A function without its own resolver gets already resolved args. A linked property is a constant only as a
        # direct top-level condition, so it must not be an argument of any function.
        reject_param_properties(expr.name, list(args) + list(kwargs.values()))
        return None


def reject_param_properties(name: str, args: list) -> None:
    """Raise if a linked property (`ParamProperty`) is an argument of function `name`."""
    for arg in args:
        if isinstance(arg, ParamProperty):
            raise ParamPropertyNotAllowed(
                arg.prop.model,
                property=arg.prop.name,
                reason=f"inside {name}()",
            )


class ParamValues(Env):
    """Collects request-time values of linked properties, see `ParamProperty`."""

    # Values keyed by linked property name.
    values: dict


# Created by spinta.datasets.utils:iterparams() function.
ResolvedParams = dict[str, Any]
