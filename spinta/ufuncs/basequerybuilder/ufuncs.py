from typing import List, Any, Tuple, Dict

from spinta.components import Page, Property
from spinta.core.ufuncs import ufunc, Expr, Negative, Bind, GetAttr
from spinta.datasets.backends.sql.ufuncs.components import Selected
from spinta.datasets.components import ExternalBackend
from spinta.exceptions import InvalidArgumentInExpression, CannotSelectTextAndSpecifiedLang, \
    LangNotDeclared, FieldNotInResource
from spinta.types.datatype import DataType, String, Ref, Object, Array, File, BackRef, PrimaryKey, ExternalRef
from spinta.types.text.components import Text
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder, Star, ReservedProperty, NestedProperty, \
    ResultProperty, LiteralProperty, Flip
from spinta.ufuncs.basequerybuilder.helpers import get_pagination_compare_query, process_literal_value
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.schema import NA

# This file contains reusable resolvers, that should be backend independent
# in case there are cases where you need to have backend specific, just overload them
# keep in mind, that `select(env, expr)` is written backend specific, at least for now

# The main goal of this resolver logic, is that you have `_resolve_getattr`
# which is responsible for returning right types to process further
# Bind mean that it's a leaf node and can be accessed directly
# GetAttr mean that it can be nested further
# ForeignProperty is used, when you need to access properties from other models
# ReservedProperty is used, when datatypes do not have direct association to required field
#   example: File (when accessing child properties), Ref (when accessing it's own key, without needing to join models)


COMPARE = [
    'eq',
    'ne',
    'lt',
    'le',
    'gt',
    'ge',
    'startswith',
    'contains',
]


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


@ufunc.resolver(BaseQueryBuilder, GetAttr, Bind, name='getattr')
def getattr_(
    env: BaseQueryBuilder,
    obj: GetAttr,
    attr: Bind
):
    leaf = env.call('getattr', obj.name, attr)
    return GetAttr(obj.obj, leaf)


@ufunc.resolver(BaseQueryBuilder, GetAttr)
def _resolve_getattr(
    env: BaseQueryBuilder,
    attr: GetAttr,
) -> ForeignProperty:
    prop = env.model.properties[attr.obj]
    return env.call('_resolve_getattr', prop.dtype, attr.name)


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


@ufunc.resolver(BaseQueryBuilder, ForeignProperty, Ref, Bind)
def _resolve_getattr(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return fpr.push(prop)


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

        return NestedProperty(
            left=dtype,
            right=env.call('_resolve_getattr', prop.dtype, attr.name)
        )
    raise FieldNotInResource(dtype, property=attr.obj)


@ufunc.resolver(BaseQueryBuilder, Object, Bind)
def _resolve_getattr(env, dtype, attr):
    if attr.name in dtype.properties:
        return NestedProperty(
            left=dtype,
            right=dtype.properties[attr.name].dtype
        )
    else:
        raise FieldNotInResource(dtype, property=attr.name)


@ufunc.resolver(BaseQueryBuilder, Array, (Bind, GetAttr))
def _resolve_getattr(env, dtype, attr):
    return NestedProperty(
        left=dtype,
        right=env.call('_resolve_getattr', dtype.items.dtype, attr)
    )


@ufunc.resolver(BaseQueryBuilder, File, Bind)
def _resolve_getattr(env, dtype, attr):
    return ReservedProperty(dtype, attr.name)


@ufunc.resolver(BaseQueryBuilder, GetAttr)
def select(env: BaseQueryBuilder, attr: GetAttr) -> Selected:
    resolved = env.call('_resolve_getattr', attr)
    return env.call('select', resolved)


@ufunc.resolver(BaseQueryBuilder, NestedProperty)
def select(env: BaseQueryBuilder, nested: NestedProperty) -> Selected:
    return Selected(
        prop=nested.left.prop,
        prep=env.call('select', nested.right),
    )


@ufunc.resolver(BaseQueryBuilder, ReservedProperty)
def select(env, prop):
    return env.call('select', prop.dtype, prop.param)


@ufunc.resolver(BaseQueryBuilder, ResultProperty)
def select(env: BaseQueryBuilder, prop: ResultProperty):
    return Selected(
        prep=prop.expr
    )


@ufunc.resolver(BaseQueryBuilder, ForeignProperty)
def select(
    env: BaseQueryBuilder,
    fpr: ForeignProperty,
) -> Selected:
    return env.call('select', fpr, fpr.right.prop)


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
        if prop.external and prop.external.prepare is not NA:
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


@ufunc.resolver(BaseQueryBuilder, LiteralProperty)
def select(
    env: BaseQueryBuilder,
    prep: LiteralProperty,
) -> Selected:
    return Selected(prep=prep.value)


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
        if not isinstance(env.backend, ExternalBackend) or (prop.external and prop.external.name):
            sel = env.call('select', dtype)
            return Selected(item=sel.item, prop=sel.prop, prep=prep)
        else:
            return Selected(item=None, prop=prop, prep=prep)
    else:
        result = env.call('select', prep)
        return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(BaseQueryBuilder, Flip)
def select(env: BaseQueryBuilder, flip_: Flip):
    return env.call('select', flip_.dtype, flip_)


@ufunc.resolver(BaseQueryBuilder, GetAttr)
def sort(env, field):
    dtype = env.call('_resolve_getattr', field)
    return env.call('sort', dtype)


@ufunc.resolver(BaseQueryBuilder, Expr, name='paginate')
def paginate(env, expr):
    if len(expr.args) != 1:
        raise InvalidArgumentInExpression(arguments=expr.args, expr='paginate')
    page = expr.args[0]
    if isinstance(page, Page):
        if page.enabled:
            if not page.filter_only:
                env.page.select = env.call('select', page)
                for by, page_by in page.by.items():
                    sorted_ = env.call('sort',
                                       Negative(page_by.prop.name) if by.startswith("-") else Bind(page_by.prop.name))
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


@ufunc.resolver(BaseQueryBuilder, NestedProperty, object, names=COMPARE)
def compare(env: BaseQueryBuilder, op: str, nested: NestedProperty, value: object):
    return env.call(op, nested.right, value)


@ufunc.resolver(BaseQueryBuilder, Bind, object, names=COMPARE)
def compare(env, op, field, value):
    prop = env.model.get_from_flatprops(field.name)
    return env.call(op, prop.dtype, value)


@ufunc.resolver(BaseQueryBuilder, Expr)
def testlist(env: BaseQueryBuilder, expr: Expr) -> tuple:
    args, kwargs = expr.resolve(env)
    result = []
    for arg in args:
        result.append(process_literal_value(arg))
    return tuple(result)


@ufunc.resolver(BaseQueryBuilder)
def flip(env: BaseQueryBuilder):
    return env.call('flip', env.this)


@ufunc.resolver(BaseQueryBuilder, Property)
def flip(env: BaseQueryBuilder, prop: Property):
    return env.call('flip', prop.dtype)


@ufunc.resolver(BaseQueryBuilder, DataType)
def flip(env: BaseQueryBuilder, dtype: DataType):
    return Expr('flip')
