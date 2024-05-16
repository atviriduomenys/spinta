from typing import List, Any, Tuple, Dict

from spinta.auth import authorized
from spinta.components import Page, Property, Action
from spinta.core.ufuncs import ufunc, Expr, Negative, Bind, GetAttr
from spinta.datasets.backends.sql.ufuncs.components import Selected
from spinta.exceptions import InvalidArgumentInExpression, CannotSelectTextAndSpecifiedLang, \
    LangNotDeclared, FieldNotInResource, PropertyNotFound
from spinta.types.datatype import DataType, String, Ref, Object, Array, File, BackRef, PrimaryKey, ExternalRef
from spinta.types.text.components import Text
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder, Star, ReservedProperty
from spinta.ufuncs.basequerybuilder.helpers import get_pagination_compare_query
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.schema import NA


@ufunc.resolver(BaseQueryBuilder, Bind, Bind, name='getattr')
def getattr_(
    env: BaseQueryBuilder,
    field: Bind,
    attr: Bind
):
    return GetAttr(field.name, attr)


@ufunc.resolver(BaseQueryBuilder, Bind, GetAttr, name='getattr')
def getattr_(
    env: BaseQueryBuilder,
    obj: Bind,
    attr: GetAttr
):
    return GetAttr(obj.name, attr)


@ufunc.resolver(BaseQueryBuilder, GetAttr)
def _resolve_getattr(
    env: BaseQueryBuilder,
    attr: GetAttr,
) -> ForeignProperty:
    prop = env.model.properties[attr.obj]
    return env.call('_resolve_getattr', prop.dtype, attr.name)


@ufunc.resolver(BaseQueryBuilder, Ref, GetAttr)
def _resolve_getattr(
    env: BaseQueryBuilder,
    dtype: Ref,
    attr: GetAttr,
):
    if attr.obj in dtype.properties:
        return dtype.properties[attr.obj].dtype
    prop = dtype.model.properties[attr.obj]
    fpr = ForeignProperty(None, dtype, prop.dtype)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(BaseQueryBuilder, Ref, Bind)
def _resolve_getattr(
    env: BaseQueryBuilder,
    dtype: Ref,
    attr: Bind,
):
    if attr.name in dtype.properties:
        return dtype.properties[attr.name].dtype
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, Ref, GetAttr)
def _resolve_getattr(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = fpr.push(prop)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(BaseQueryBuilder, Ref, Bind)
def _resolve_getattr(
    env: BaseQueryBuilder,
    dtype: Ref,
    attr: Bind,
):
    if attr.name in dtype.properties:
        return dtype.properties[attr.name].dtype

    # Check for self reference, no need to do joins if table already contains the value
    if attr.name == '_id':
        return ReservedProperty(dtype, attr.name)

    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(BaseQueryBuilder, ExternalRef, Bind)
def _resolve_getattr(
    env: BaseQueryBuilder,
    dtype: ExternalRef,
    attr: Bind,
):
    if attr.name in dtype.properties:
        return dtype.properties[attr.name].dtype

    # Check for self reference, no need to do joins if table already contains the value
    for refprop in dtype.refprops:
        if refprop.name == attr.name:
            return ReservedProperty(dtype, attr.name)

    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(BaseQueryBuilder, BackRef, GetAttr)
def _resolve_getattr(
    env: BaseQueryBuilder,
    dtype: BackRef,
    attr: GetAttr,
):
    prop = dtype.model.properties[attr.obj]
    fpr = ForeignProperty(None, dtype, prop.dtype)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(BaseQueryBuilder, BackRef, Bind)
def _resolve_getattr(env, dtype, attr):
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, BackRef, GetAttr)
def _resolve_getattr(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    dtype: BackRef,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = fpr.push(prop)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, BackRef, Bind)
def _resolve_getattr(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    dtype: BackRef,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return fpr.push(prop)


@ufunc.resolver(BaseQueryBuilder, Text, Bind)
def _resolve_getattr(
    env: BaseQueryBuilder,
    dtype: Text,
    bind: Bind
):
    if dtype.prop.name in env.model.properties:
        prop = env.model.properties[dtype.prop.name]
        if bind.name in prop.dtype.langs:
            return prop.dtype.langs[bind.name].dtype
        raise LangNotDeclared(dtype, lang=bind.name)


@ufunc.resolver(BaseQueryBuilder, Object, GetAttr)
def _resolve_getattr(
    env: BaseQueryBuilder,
    dtype: Object,
    attr: GetAttr,
):
    if attr.obj in dtype.properties:
        prop = dtype.properties[attr.obj]
        return env.call('_resolve_getattr', prop.dtype, attr.name)
    raise FieldNotInResource(dtype, property=attr.obj)


@ufunc.resolver(BaseQueryBuilder, Object, Bind)
def _resolve_getattr(env, dtype, attr):
    if attr.name in dtype.properties:
        return dtype.properties[attr.name].dtype
    else:
        raise FieldNotInResource(dtype, property=attr.name)


@ufunc.resolver(BaseQueryBuilder, Array, (Bind, GetAttr))
def _resolve_getattr(env, dtype, attr):
    return env.call('_resolve_getattr', dtype.items.dtype, attr)


@ufunc.resolver(BaseQueryBuilder, File, Bind)
def _resolve_getattr(env, dtype, attr):
    return ReservedProperty(dtype, attr.name)


@ufunc.resolver(BaseQueryBuilder, GetAttr)
def select(env: BaseQueryBuilder, attr: GetAttr) -> Selected:
    resolved = env.call('_resolve_getattr', attr)
    return env.call('select', resolved, attr)


@ufunc.resolver(BaseQueryBuilder, DataType, GetAttr)
def select(env: BaseQueryBuilder, dtype: DataType, attr: GetAttr) -> Selected:
    return env.call('select', dtype.prop)


@ufunc.resolver(BaseQueryBuilder, ReservedProperty)
def select(env, prop):
    return env.call('select', prop.dtype, prop.param)


@ufunc.resolver(BaseQueryBuilder, ReservedProperty, GetAttr)
def select(env: BaseQueryBuilder, reserved: ReservedProperty, attr: GetAttr) -> Selected:
    return env.call('select', reserved)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty)
def select(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
) -> Selected:
    return env.call('select', fpr, fpr.right.prop)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, GetAttr)
def select(env: BaseQueryBuilder, fpr: ForeignProperty, attr: GetAttr) -> Selected:
    """For things like select(foo.bar.baz)."""
    return Selected(
        prop=fpr.right.prop,
        prep=env.call('select', fpr, fpr.right.prop),
    )


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, Property)
def select(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    prop: Property,
) -> Selected:
    resolved_key = fpr / prop
    if resolved_key not in env.resolved:
        if isinstance(prop.external, list):
            raise ValueError("Source can't be a list, use prepare instead.")
        if prop.external.prepare is not NA:
            if isinstance(prop.external.prepare, Expr):
                result = env(this=prop).resolve(prop.external.prepare)
            else:
                result = prop.external.prepare
            result = env.call('select', fpr, prop.dtype, result)
        else:
            result = env.call('select', fpr, prop.dtype)
        assert isinstance(result, Selected), prop
        env.resolved[resolved_key] = result
    return env.resolved[resolved_key]


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, DataType, object)
def select(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    prep: Any,
) -> Selected:
    result = env.call('select', fpr, prep)
    return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, tuple)
def select(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    prep: Tuple[Any],
) -> Tuple[Any]:
    return tuple(env.call('select', fpr, v) for v in prep)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, Bind)
def select(env: BaseQueryBuilder, fpr: ForeignProperty, item: Bind):
    model = fpr.right.prop.model
    prop = model.flatprops.get(item.name)
    if prop and authorized(env.context, prop, Action.SEARCH):
        return env.call('select', fpr, prop)
    else:
        raise PropertyNotFound(model, property=item.name)


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, PrimaryKey)
def select(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    dtype: PrimaryKey,
) -> Selected:
    model = dtype.prop.model
    pkeys = model.external.pkeys

    if not pkeys:
        raise RuntimeError(
            f"Can't join {dtype.prop} on right table without primary key."
        )

    if len(pkeys) == 1:
        prop = pkeys[0]
        result = env.call('select', fpr, prop)
    else:
        result = [
            env.call('select', fpr, prop)
            for prop in pkeys
        ]
    return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(BaseQueryBuilder, list)
def select(
    env: BaseQueryBuilder,
    prep: List[Any],
) -> List[Any]:
    return [env.call('select', v) for v in prep]


@ufunc.resolver(BaseQueryBuilder, tuple)
def select(
    env: BaseQueryBuilder,
    prep: Tuple[Any],
) -> Tuple[Any]:
    return tuple(env.call('select', v) for v in prep)


@ufunc.resolver(BaseQueryBuilder, dict)
def select(
    env: BaseQueryBuilder,
    prep: Dict[str, Any],
) -> Dict[str, Any]:
    # TODO: Add tests.
    return {k: env.call('select', v) for k, v in prep.items()}


@ufunc.resolver(BaseQueryBuilder, DataType, object)
def select(env: BaseQueryBuilder, dtype: DataType, prep: Any) -> Selected:
    if isinstance(prep, str):
        # XXX: Backwards compatibility thing.
        #      str values are interpreted as Bind values and Bind values are
        #      assumed to be properties. So here we skip
        #      `env.call('select', prep)` and return `prep` as is.
        #      This should be eventually removed, once backwards compatibility
        #      for resolving strings as properties is removed.
        return Selected(prop=dtype.prop, prep=prep)
    elif isinstance(prep, Expr):
        # If `prepare` expression returns another expression, then this means,
        # it must be processed on values returned by query.
        prop = dtype.prop
        if prop.external and prop.external.name:
            sel = env.call('select', prop)
            return Selected(item=sel.item, prop=sel.prop, prep=prep)
        else:
            return Selected(item=None, prop=prop, prep=prep)
    else:
        result = env.call('select', prep)
        return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(BaseQueryBuilder, Expr, name='paginate')
def paginate(env, expr):
    if len(expr.args) != 1:
        raise InvalidArgumentInExpression(arguments=expr.args, expr='paginate')
    page = expr.args[0]
    if isinstance(page, Page):
        if page.is_enabled:
            if not page.filter_only:
                env.page.select = env.call('select', page)
                for by, page_by in page.by.items():
                    sorted_ = env.call('sort', Negative(page_by.prop.name) if by.startswith("-") else Bind(page_by.prop.name))
                    if sorted_ is not None:
                        if isinstance(sorted_, (list, set, tuple)):
                            env.page.sort += sorted_
                        else:
                            env.page.sort.append(sorted_)
            env.page.page_ = page
            env.page.size = page.size
            return env.resolve(get_pagination_compare_query(page))
    else:
        raise InvalidArgumentInExpression(arguments=expr.args, expr='paginate')


@ufunc.resolver(BaseQueryBuilder, Expr, name='expand')
def expand(env, expr):
    result = []
    if expr.args:
        for arg in expr.args:
            resolved = env.resolve(arg)
            result.append(resolved)
        return result
    return None


@ufunc.resolver(BaseQueryBuilder, str, name='op')
def op_(env, arg: str):
    if arg == '*':
        return Star()
    else:
        raise NotImplementedError


@ufunc.resolver(BaseQueryBuilder, Expr, name='page')
def page_(env, expr):
    pass


@ufunc.resolver(BaseQueryBuilder, DataType, list)
def validate_dtype_for_select(env, dtype: DataType, selected_props: List[Property]):
    raise NotImplemented(f"validate_dtype_for_select with {dtype.name} is not implemented")


@ufunc.resolver(BaseQueryBuilder, Text, list)
def validate_dtype_for_select(env, dtype: Text, selected_props: List[Property]):
    for prop in selected_props:
        if dtype.prop.name == prop.name or prop.parent == dtype.prop:
            raise CannotSelectTextAndSpecifiedLang(dtype)


@ufunc.resolver(BaseQueryBuilder, String, list)
def validate_dtype_for_select(env, dtype: String, selected_props: List[Property]):
    if dtype.prop.parent and isinstance(dtype.prop.parent.dtype, Text):
        parent = dtype.prop.parent
        for prop in selected_props:
            if parent.name == prop.name or prop == dtype.prop:
                raise CannotSelectTextAndSpecifiedLang(parent.dtype)


