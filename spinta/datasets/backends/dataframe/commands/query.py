from typing import Dict, Any, Tuple, List

from dask import dataframe as Dataframe

from spinta.auth import authorized
from spinta.components import Model, Property, Action
from spinta.core.ufuncs import Env, Expr, ufunc, Bind, Unresolved
from spinta.datasets.backends.dataframe.components import DaskBackend
from spinta.datasets.backends.sql.commands.query import GetAttr
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import UnknownMethod, PropertyNotFound, NotImplementedFeature, SourceCannotBeList
from spinta.types.datatype import DataType, PrimaryKey, Ref
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.data import take
from spinta.utils.schema import NA


class Selected:
    # Item index in select list.
    item: str = None
    # Model property if a property is selected.
    prop: Property = None
    # A value or an Expr for further processing on selected value.
    prep: Any = NA

    def __init__(
        self,
        item: str = None,
        prop: Property = None,
        # `prop` can be Expr or any other value.
        prep: Any = NA,
    ):
        self.item = item
        self.prop = prop
        self.prep = prep

    def __repr__(self):
        return self.debug()

    def debug(self, indent: str = ''):
        prop = self.prop.place if self.prop else 'None'
        if isinstance(self.prep, Selected):
            return (
                f'{indent}Selected('
                f'item={self.item}, '
                f'prop={prop}, '
                f'prep=...)\n'
            ) + self.prep.debug(indent + '  ')
        elif isinstance(self.prep, (tuple, list)):
            return (
                f'{indent}Selected('
                f'item={self.item}, '
                f'prop={prop}, '
                f'prep={type(self.prep).__name__}...)\n'
            ) + ''.join([
                p.debug(indent + '- ')
                if isinstance(p, Selected)
                else str(p)
                for p in self.prep
            ])
        else:
            return (
                f'{indent}Selected('
                f'item={self.item}, '
                f'prop={prop}, '
                f'prep={self.prep})\n'
            )


class DaskDataFrameQueryBuilder(Env):
    backend: DaskBackend
    model: Model
    dataframe: Dataframe
    # `resolved` is used to map which prop.place properties are already
    # resolved, usually it maps to Selected, but different DataType's can return
    # different results.
    resolved: Dict[str, Selected]
    selected: Dict[str, Selected] = None
    params: ResolvedParams

    def init(self, backend: DaskBackend, dataframe: Dataframe):
        return self(
            backend=backend,
            dataframe=dataframe,
            resolved={},
            selected=None,
            sort={
                "desc": [],
                "asc": []
            },
            limit=None,
            offset=None,
        )

    def build(self, where):
        if self.selected is None:
            self.call('select', Expr('select'))
        df = self.dataframe

        if self.limit is not None:
            df = df.loc[:self.limit - 1]
        #
        if self.offset is not None:
            df = df.loc[self.offset:]
        return df

    def execute(self, expr: Any):
        expr = self.call('_resolve_unresolved', expr)
        return super().execute(expr)

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(name=expr.name, expr=str(expr(*args, **kwargs)))


@ufunc.resolver(DaskDataFrameQueryBuilder, int)
def limit(env: DaskDataFrameQueryBuilder, n: int):
    env.limit = n


@ufunc.resolver(DaskDataFrameQueryBuilder, int)
def offset(env: DaskDataFrameQueryBuilder, n: int):
    env.offset = n


@ufunc.resolver(DaskDataFrameQueryBuilder, Bind, Bind, name='getattr')
def getattr_(env: DaskDataFrameQueryBuilder, obj: Bind, attr: Bind):
    return GetAttr(obj.name, attr)


@ufunc.resolver(DaskDataFrameQueryBuilder, Bind, GetAttr, name='getattr')
def getattr_(env: DaskDataFrameQueryBuilder, obj: Bind, attr: GetAttr):
    return GetAttr(obj.name, attr)


@ufunc.resolver(DaskDataFrameQueryBuilder, GetAttr)
def _resolve_getattr(
    env: DaskDataFrameQueryBuilder,
    attr: GetAttr,
) -> ForeignProperty:
    prop = env.model.properties[attr.obj]
    return env.call('_resolve_getattr', prop.dtype, attr.name)


@ufunc.resolver(DaskDataFrameQueryBuilder, Ref, GetAttr)
def _resolve_getattr(
    env: DaskDataFrameQueryBuilder,
    dtype: Ref,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = ForeignProperty(None, dtype, prop.dtype)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(DaskDataFrameQueryBuilder, Ref, Bind)
def _resolve_getattr(
    env: DaskDataFrameQueryBuilder,
    dtype: Ref,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, Ref, GetAttr)
def _resolve_getattr(
    env: DaskDataFrameQueryBuilder,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = fpr.push(prop)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(DaskDataFrameQueryBuilder, ForeignProperty, Ref, Bind)
def _resolve_getattr(
    env: DaskDataFrameQueryBuilder,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return fpr.push(prop)


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
