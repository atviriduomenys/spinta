from typing import Any
from typing import Union

from spinta.components import Context
from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Expr
from spinta.exceptions import PropertyNotFound
from spinta.manifests.components import Manifest
from spinta.naming.components import NameFormatter
from spinta.types.datatype import Ref
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def _re_format_expr(env: NameFormatter, expr: Any) -> Any:
    if isinstance(expr, Expr):
        args = [_re_format_expr(env, a) for a in expr.args]
        kwargs = {k: _re_format_expr(env, v) for k, v in expr.kwargs.items()}
        return type(expr)(expr.name, *args, **kwargs)

    if isinstance(expr, tuple):
        return tuple(_re_format_expr(env, v) for v in expr)

    if isinstance(expr, list):
        return [_re_format_expr(env, v) for v in expr]

    if isinstance(expr, dict):
        return {k: _re_format_expr(env, v) for k, v in expr}

    if isinstance(expr, Bind):
        if expr.name not in env.model.properties:
            raise PropertyNotFound(env.model, property=expr.name)
        prop = env.model.properties[expr.name]
        return Expr('bind', _format_property_place(prop))

    if isinstance(expr, ForeignProperty):
        fpr: ForeignProperty = expr
        expr = Expr(
            'getattr',
            Expr('bind', _format_property_place(fpr.left.prop)),
            Expr('bind', _format_property_place(fpr.right.prop)),
        )
        for fpr in reversed(fpr.chain[:-1]):
            expr = Expr(
                'getattr',
                expr,
                Expr('bind', _format_property_place(fpr.left.prop)),
            )
        return expr

    return expr


def _format_expr(
    context: Context,
    node: Union[Model, Property],
    expr: Expr,
) -> Expr:
    env = NameFormatter(context)
    if isinstance(node, Model):
        env.update(model=node, prop=None)
    elif isinstance(node, Property):
        env.update(model=node.model, prop=node)
    else:
        raise NotImplementedError
    expr = env.resolve(expr)
    return _re_format_expr(env, expr)


def _format_property_expr(context: Context, prop: Property) -> None:
    if prop.external and prop.external.prepare:
        prop.external.prepare = _format_expr(context, prop, prop.external.prepare)


def _format_model_expr(context: Context, model: Model) -> None:
    if model.external and model.external.prepare:
        model.external.prepare = _format_expr(context, model, model.external.prepare)
    for prop in model.properties.values():
        _format_property_expr(context, prop)


def _format_model_name(name: str) -> str:
    if '/' in name:
        ns, name = name.rsplit('/', 1)
        return '/'.join([ns.lower(), to_model_name(name)])
    else:
        return to_model_name(name)


def _format_property_name(prop: Property) -> str:
    is_ref = isinstance(prop.dtype, Ref)
    return to_property_name(prop.name, is_ref)


def _format_property_place(prop: Property) -> str:
    is_ref = isinstance(prop.dtype, Ref)
    place = prop.place.split('.')
    return '.'.join(
        [to_property_name(n) for n in place[:-1]] +
        [to_property_name(n, is_ref) for n in place[-1:]]
    )


def _format_property(prop: Property) -> Property:
    if prop.name.startswith('_'):
        return prop
    else:
        prop.name = _format_property_name(prop)
        prop.place = _format_property_place(prop)
        return prop


def _format_model(model: Model) -> Model:
    model.name = _format_model_name(model.name)
    model.properties = {
        prop.name: prop
        for prop in map(_format_property, model.properties.values())
    }
    return model


def reformat_names(context: Context, manifest: Manifest):
    for model in manifest.models.values():
        _format_model_expr(context, model)

    manifest.objects['model'] = {
        model.name: model
        for model in map(_format_model, manifest.models.values())
    }
