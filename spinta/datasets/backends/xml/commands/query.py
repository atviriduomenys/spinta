import base64
import dataclasses
from typing import Dict, Any, Tuple, List, Optional, TypedDict, Union, TypeVar, overload

import dask as dask
from _decimal import Decimal

from spinta.auth import authorized
from spinta.components import Model, Property, Action
from spinta.core.ufuncs import Env, Expr, ufunc, Bind, Negative, Unresolved
from spinta.datasets.backends.sql.commands.query import GetAttr
from spinta.datasets.backends.xml.components import Xml
from spinta.dimensions.enum.helpers import prepare_enum_value
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import UnknownMethod, PropertyNotFound, UnableToCast, NotImplementedFeature
from spinta.types.datatype import DataType, PrimaryKey, Ref, Integer, String
from spinta.types.file.components import FileData
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.data import take
from spinta.utils.itertools import flatten
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
    backend: Xml
    model: Model
    dataframe: dask.dataframe
    # `resolved` is used to map which prop.place properties are already
    # resolved, usually it maps to Selected, but different DataType's can return
    # different results.
    resolved: Dict[str, Selected]
    selected: Dict[str, Selected] = None
    params: ResolvedParams

    def init(self, backend: Xml, dataframe: dask.dataframe):
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
        print(self.selected)
        df = self.dataframe
        # qry = sa.select(self.columns)
        # qry = qry.select_from(self.joins.from_)
        #
        # if where is not None:
        #     qry = qry.where(where)
        #
        if self.sort["desc"]:
            df = df.sort_values(by=self.sort["desc"], ascending=False)
        if self.sort["asc"]:
            df = df.sort_values(by=self.sort["asc"], ascending=False)
        #
        if self.limit is not None:
            df = df.loc[:self.limit]
        #
        if self.offset is not None:
            df = df.loc[self.offset:]
        return df

    def execute(self, expr: Any):
        expr = self.call('_resolve_unresolved', expr)
        return super().execute(expr)

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(name=expr.name, expr=str(expr(*args, **kwargs)))


@ufunc.resolver(DaskDataFrameQueryBuilder, DataType, list)
def eq(env: DaskDataFrameQueryBuilder, dtype: DataType, value: List[Any]):
    column = dtype.prop.external.name
    return env.dataframe[env.dataframe[column].isin(value)]


@ufunc.resolver(DaskDataFrameQueryBuilder, DataType, list)
def ne(env: DaskDataFrameQueryBuilder, dtype: DataType, value: List[Any]):
    column = dtype.prop.external.name
    return env.dataframe[~env.dataframe[column].isin(value)]


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


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr, name='list')
def list_(env: DaskDataFrameQueryBuilder, expr: Expr) -> List[Any]:
    args, kwargs = expr.resolve(env)
    return list(args)


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr)
def testlist(env: DaskDataFrameQueryBuilder, expr: Expr) -> Tuple[Any]:
    args, kwargs = expr.resolve(env)
    return tuple(args)


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
            print(arg)
            print(type(arg))
            env.selected[key] = env.call('select', arg)
    else:
        for prop in take(['_id', all], env.model.properties).values():
            if authorized(env.context, prop, Action.GETALL):
                env.selected[prop.place] = env.call('select', prop)


@ufunc.resolver(DaskDataFrameQueryBuilder, object)
def select(env: DaskDataFrameQueryBuilder, value: Any) -> Selected:
    """For things like select(1, count())."""
    raise NotImplementedFeature("Ability to write custom function for property.prepare")


@ufunc.resolver(DaskDataFrameQueryBuilder, Bind)
def select(env: DaskDataFrameQueryBuilder, item: Bind, *, nested: bool = False):
    prop = _get_property_for_select(env, item.name, nested=nested)
    return env.call('select', prop)


@ufunc.resolver(DaskDataFrameQueryBuilder, str)
def select(env: DaskDataFrameQueryBuilder, item: str, *, nested: bool = False):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = _get_property_for_select(env, item, nested=nested)
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
            # TODO: Should raise one of spinta.exceptions exception, with
            #       property as a context.
            raise ValueError("Source can't be a list, use prepare instead.")
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
    print(prep)
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
    # TODO: Add tests.
    return {k: env.call('select', v) for k, v in prep.items()}


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr)
def sort(env: DaskDataFrameQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    env.sort = {
        "desc": [],
        "asc": []
    }
    for key in args:
        prop = env.model.properties[key.name]
        if prop.external and prop.external.name:
            if isinstance(key, Negative):
                env.sort["desc"].append(prop.external.name)
            else:
                env.sort["asc"].append(prop.external.name)


@ufunc.resolver(DaskDataFrameQueryBuilder, int)
def limit(env: DaskDataFrameQueryBuilder, n: int):
    env.limit = n


@ufunc.resolver(DaskDataFrameQueryBuilder, int)
def offset(env: DaskDataFrameQueryBuilder, n: int):
    env.offset = n


@ufunc.resolver(DaskDataFrameQueryBuilder, Property, object, object)
def swap(env: DaskDataFrameQueryBuilder, prop: Property, old: Any, new: Any) -> Any:
    return Expr('swap', old, new)


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr)
def file(env: DaskDataFrameQueryBuilder, expr: Expr) -> Expr:
    args, kwargs = expr.resolve(env)
    return Expr(
        'file',
        name=env.call('select', kwargs['name'], nested=True),
        content=env.call('select', kwargs['content'], nested=True),
    )


class _FileSelected(TypedDict):
    name: Selected      # File name
    content: Selected   # File content


@ufunc.resolver(DaskDataFrameQueryBuilder, Expr)
def file(env: DaskDataFrameQueryBuilder, expr: Expr) -> FileData:
    """Post query file data processor

    Will be called with _FileSelected kwargs and no args.
    """
    kwargs: _FileSelected
    args, kwargs = expr.resolve(env)
    assert len(args) == 0, args
    name = env.data[kwargs['name'].item]
    content = env.data[kwargs['content'].item]
    if isinstance(content, str):
        content = content.encode('utf-8')
    if content is not None:
        content = base64.b64encode(content).decode()
    return {
        '_id': name,
        # TODO: Content probably should not be returned if not explicitly
        #       requested in select list.
        '_content': content,
    }


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder)
def cast(env: DaskDataFrameQueryBuilder) -> Expr:
    return Expr('cast')


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder)
def cast(env: DaskDataFrameQueryBuilder) -> Any:
    return env.call('cast', env.prop.dtype, env.this)


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder, String, int)
def cast(env: DaskDataFrameQueryBuilder, dtype: String, value: int) -> str:
    return str(value)


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder, String, type(None))
def cast(env: DaskDataFrameQueryBuilder, dtype: String, value: Optional[Any]) -> str:
    return ''


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder, Integer, Decimal)
def cast(env: DaskDataFrameQueryBuilder, dtype: Integer, value: Decimal) -> int:
    return env.call('cast', dtype, float(value))


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder, Integer, float)
def cast(env: DaskDataFrameQueryBuilder, dtype: Integer, value: float) -> int:
    if value % 1 > 0:
        raise UnableToCast(dtype, value=value, type=dtype.name)
    else:
        return int(value)


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder, Bind, Bind)
def point(env: DaskDataFrameQueryBuilder, x: Bind, y: Bind) -> Expr:
    return Expr(
        'point',
        env.call('select', x, nested=True),
        env.call('select', y, nested=True),
    )


@overload
@ufunc.resolver(DaskDataFrameQueryBuilder, Selected, Selected)
def point(env: DaskDataFrameQueryBuilder, x: Selected, y: Selected) -> Expr:
    x = env.data[x.item]
    y = env.data[y.item]
    return f'POINT ({x} {y})'
