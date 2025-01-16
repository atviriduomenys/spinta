from functools import reduce
from typing import Dict, Any, Tuple, List

from spinta.auth import authorized
from spinta.components import Property, Action
from spinta.core.ufuncs import Expr, ufunc, Bind, Unresolved, GetAttr
from spinta.datasets.backends.dataframe.ufuncs.query.components import DaskDataFrameQueryBuilder, DaskSelected as Selected
from spinta.exceptions import PropertyNotFound, NotImplementedFeature, SourceCannotBeList
from spinta.types.datatype import DataType, PrimaryKey, Ref, Binary
from spinta.types.text.components import Text
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.data import take
from spinta.utils.schema import NA
import base64 as b64


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call('and', args)


@ufunc.resolver(DaskDataFrameQueryBuilder, list, name='and')
def and_(env, args):
    if len(args) > 1:
        return reduce(lambda x, y: x & y, args)
    elif args:
        return args[0]


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call('or', args)


@ufunc.resolver(DaskDataFrameQueryBuilder, list, name='or')
def or_(env, args):
    if len(args) > 1:
        return reduce(lambda x, y: x | y, args)
    elif args:
        return args[0]


@ufunc.resolver(DaskDataFrameQueryBuilder)
def distinct(env: DaskDataFrameQueryBuilder):
    if env.model and env.model.external and not env.model.external.unknown_primary_key:
        extracted_columns = [prop.external.name for prop in env.model.external.pkeys if prop.external]
        env.dataframe = env.dataframe.drop_duplicates(extracted_columns)
    else:
        env.dataframe = env.dataframe.drop_duplicates()


@ufunc.resolver(DaskDataFrameQueryBuilder, int)
def limit(env: DaskDataFrameQueryBuilder, n: int):
    env.limit = n


@ufunc.resolver(DaskDataFrameQueryBuilder, int)
def offset(env: DaskDataFrameQueryBuilder, n: int):
    env.offset = n


@ufunc.resolver(DaskDataFrameQueryBuilder, object)
def _resolve_unresolved(env: DaskDataFrameQueryBuilder, value: Any) -> Any:
    if isinstance(value, Unresolved):
        raise ValueError(f"Unresolved value {value!r}.")
    else:
        return value


@ufunc.resolver(DaskDataFrameQueryBuilder, Bind)
def _resolve_unresolved(env: DaskDataFrameQueryBuilder, field: Bind) -> str:
    prop = env.model.flatprops.get(field.name)
    if prop:
        return prop.external.name
    else:
        raise PropertyNotFound(env.model, property=field.name)


@ufunc.resolver(DaskDataFrameQueryBuilder)
def count(env: DaskDataFrameQueryBuilder):
    return len(env.dataframe.index)


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr)
def select(env: DaskDataFrameQueryBuilder, expr: Expr):
    keys = [str(k) for k in expr.args]
    args, kwargs = expr.resolve(env)
    args = list(zip(keys, args)) + list(kwargs.items())

    if env.selected is not None:
        raise RuntimeError("`select` was already called.")

    env.selected = {}
    if args:
        for key, arg in args:
            env.selected[key] = env.call('select', arg)
    else:
        for prop in take(['_id', all], env.model.properties).values():
            if authorized(env.context, prop, Action.GETALL):
                env.selected[prop.place] = env.call('select', prop)


@ufunc.resolver(DaskDataFrameQueryBuilder, Bind)
def select(env: DaskDataFrameQueryBuilder, item: Bind, *, nested: bool = False):
    prop = _get_property_for_select(env, item.name, nested=nested)
    return env.call('select', prop)


def _get_property_for_select(
    env: DaskDataFrameQueryBuilder,
    name: str,
    *,
    nested: bool = False,
) -> Property:
    # TODO: `name` can refer to (in specified order):
    #       - var - a defined variable
    #       - param - a parameter if parametrization is used
    #       - item - an item of a dict or list
    #       - prop - a property
    #       Currently only `prop` is resolved.
    prop = env.model.flatprops.get(name)
    if prop and (
        # Check authorization only for top level properties in select list.
        # XXX: Not sure if nested is the right property to user, probably better
        #      option is to check if this call comes from a prepare context. But
        #      then how prepare context should be defined? Probably resolvers
        #      should be called with a different env class?
        #      tag:resolving_private_properties_in_prepare_context
        nested or
        authorized(env.context, prop, Action.SEARCH)
    ):
        return prop
    else:
        raise PropertyNotFound(env.model, property=name)


@ufunc.resolver(DaskDataFrameQueryBuilder, Property)
def select(env: DaskDataFrameQueryBuilder, prop: Property) -> Selected:
    if prop.place not in env.resolved:
        if isinstance(prop.external, list):
            raise SourceCannotBeList(prop)
        if prop.external.prepare is not NA:
            # If `prepare` formula is given, evaluate formula.
            if isinstance(prop.external.prepare, Expr):
                result = env(this=prop).resolve(prop.external.prepare)
            else:
                result = prop.external.prepare
            # XXX: Maybe interpretation of prepare formula should be done under
            #      a different Env? This way, select resolvers would know when
            #      properties are resolved under a formula context and for
            #      example would be able to decide to not check permissions on
            #      properties.
            #      tag:resolving_private_properties_in_prepare_context
            result = env.call('select', prop.dtype, result)
        elif prop.external and prop.external.name:
            # If prepare is not given, then take value from `source`.
            result = env.call('select', prop.dtype)
        elif prop.is_reserved():
            # Reserved properties never have external source.
            result = env.call('select', prop.dtype)
        elif not prop.dtype.requires_source:
            # Some property types do not require source (object, text, etc)
            result = env.call('select', prop.dtype)
        else:
            # If `source` is not given, return None.
            result = Selected(prop=prop, prep=None)
        assert isinstance(result, Selected), prop
        env.resolved[prop.place] = result
    return env.resolved[prop.place]


@ufunc.resolver(DaskDataFrameQueryBuilder, DataType)
def select(env: DaskDataFrameQueryBuilder, dtype: DataType) -> Selected:
    return Selected(
        item=dtype.prop.external.name,
        prop=dtype.prop,
    )


@ufunc.resolver(DaskDataFrameQueryBuilder, Text)
def select(env: DaskDataFrameQueryBuilder, dtype: Text) -> Selected:
    prep = {}
    for lang, prop in dtype.langs.items():
        prep[lang] = env.call('select', prop)
    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(DaskDataFrameQueryBuilder, DataType, object)
def select(env: DaskDataFrameQueryBuilder, dtype: DataType, prep: Any) -> Selected:
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
            sel = env.call('select', dtype)
            return Selected(item=sel.item, prop=sel.prop, prep=prep)
        else:
            return Selected(item=None, prop=prop, prep=prep)
    else:
        if prep is not None:
            result = env.call('select', prep)
        else:
            result = None
        if isinstance(result, Selected):
            return result
        return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(DaskDataFrameQueryBuilder, PrimaryKey)
def select(
    env: DaskDataFrameQueryBuilder,
    dtype: PrimaryKey,
) -> Selected:
    model = dtype.prop.model
    pkeys = model.external.pkeys

    if not pkeys:
        # If primary key is not specified use all properties to uniquely
        # identify row.
        pkeys = take(model.properties).values()

    if len(pkeys) == 1:
        prop = pkeys[0]
        result = env.call('select', prop)
    else:
        result = [
            env.call('select', prop)
            for prop in pkeys
        ]
    return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(DaskDataFrameQueryBuilder, Selected)
def select(env: DaskDataFrameQueryBuilder, selected: Selected):
    return selected


@ufunc.resolver(DaskDataFrameQueryBuilder, Ref, object)
def select(env: DaskDataFrameQueryBuilder, dtype: Ref, prep: Any) -> Selected:
    fpr = ForeignProperty(None, dtype, dtype.model.properties['_id'].dtype)
    return Selected(
        prop=dtype.prop,
        prep=env.call('select', fpr, fpr.right.prop),
    )


@ufunc.resolver(DaskDataFrameQueryBuilder, GetAttr)
def select(env: DaskDataFrameQueryBuilder, attr: GetAttr) -> Selected:
    """For things like select(foo.bar.baz)."""

    fpr: ForeignProperty = env.call('_resolve_getattr', attr)
    raise NotImplementedFeature(fpr.left.prop.model, feature="Ability to use foreign properties")
    return Selected(
        prop=fpr.right.prop,
        prep=env.call('select', fpr, fpr.right.prop),
    )


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty)
def select(
    env: DaskDataFrameQueryBuilder,
    fpr: ForeignProperty,
) -> Selected:
    return env.call('select', fpr, fpr.right.prop)


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, Property)
def select(
    env: DaskDataFrameQueryBuilder,
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


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, DataType)
def select(
    env: DaskDataFrameQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
) -> Selected:
    # TODO need join for this to work
    return Selected(
        item=dtype.prop.name,
        prop=dtype.prop,
    )


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, DataType, object)
def select(
    env: DaskDataFrameQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    prep: Any,
) -> Selected:
    result = env.call('select', fpr, prep)
    return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, tuple)
def select(
    env: DaskDataFrameQueryBuilder,
    fpr: ForeignProperty,
    prep: Tuple[Any],
) -> Tuple[Any]:
    return tuple(env.call('select', fpr, v) for v in prep)


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, Bind)
def select(env: DaskDataFrameQueryBuilder, fpr: ForeignProperty, item: Bind):
    model = fpr.right.prop.model
    prop = model.flatprops.get(item.name)
    if prop and authorized(env.context, prop, Action.SEARCH):
        return env.call('select', fpr, prop)
    else:
        raise PropertyNotFound(model, property=item.name)


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, PrimaryKey)
def select(
    env: DaskDataFrameQueryBuilder,
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


@ufunc.resolver(DaskDataFrameQueryBuilder, list)
def select(
    env: DaskDataFrameQueryBuilder,
    prep: List[Any],
) -> List[Any]:
    return [env.call('select', v) for v in prep]


@ufunc.resolver(DaskDataFrameQueryBuilder, tuple)
def select(
    env: DaskDataFrameQueryBuilder,
    prep: Tuple[Any],
) -> Tuple[Any]:
    return tuple(env.call('select', v) for v in prep)


@ufunc.resolver(DaskDataFrameQueryBuilder, dict)
def select(
    env: DaskDataFrameQueryBuilder,
    prep: Dict[str, Any],
) -> Dict[str, Any]:
    return {k: env.call('select', v) for k, v in prep.items()}


@ufunc.resolver(DaskDataFrameQueryBuilder)
def base64(env: DaskDataFrameQueryBuilder) -> bytes:
    return env.call('base64', env.this)


@ufunc.resolver(DaskDataFrameQueryBuilder, Property)
def base64(env: DaskDataFrameQueryBuilder, prop: Property) -> bytes:
    return env.call('base64', prop.dtype)


@ufunc.resolver(DaskDataFrameQueryBuilder, Binary)
def base64(env: DaskDataFrameQueryBuilder, dtype: Binary) -> Selected:
    item = f'base64({dtype.prop.external.name})'
    env.dataframe[item] = env.dataframe[dtype.prop.external.name].str.encode('ascii').map(b64.decodebytes, meta=(dtype.prop.external.name, bytes))
    return Selected(
        item=item,
        prop=dtype.prop
    )


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


@ufunc.resolver(DaskDataFrameQueryBuilder, Bind, object, names=COMPARE)
def compare(env, op, field, value):
    prop = env.model.get_from_flatprops(field.name)
    return env.call(op, prop.dtype, value)


@ufunc.resolver(DaskDataFrameQueryBuilder, DataType, object, name="eq")
def eq_(env: DaskDataFrameQueryBuilder, dtype: DataType, obj: object):
    name = dtype.prop.external.name
    return env.dataframe[name] == str(obj)
