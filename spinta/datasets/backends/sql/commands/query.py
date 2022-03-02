from __future__ import annotations

import base64
import dataclasses
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import TypedDict
from typing import Union

import sqlalchemy as sa
from sqlalchemy.sql.functions import Function

from spinta.auth import authorized
from spinta.components import Action
from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import Negative
from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.components import SqlResultBuilder
from spinta.dimensions.enum.helpers import prepare_enum_value
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import PropertyNotFound
from spinta.exceptions import UnknownMethod
from spinta.types.datatype import DataType
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.types.datatype import String
from spinta.types.file.components import FileData
from spinta.ufuncs.components import ForeignProperty
from spinta.core.ufuncs import Unresolved
from spinta.utils.data import take
from spinta.utils.itertools import flatten
from spinta.utils.schema import NA


def ensure_list(value: Any) -> List[Any]:
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, list):
        return value
    else:
        return [value]


class SqlFrom:
    backend: Sql
    joins: Dict[str, sa.Table]
    from_: sa.Table

    def __init__(self, backend: Sql, table: sa.Table):
        self.backend = backend
        self.joins = {}
        self.from_ = table

    def get_table(
        self,
        env: SqlQueryBuilder,
        prop: ForeignProperty,
    ) -> sa.Table:
        fpr: Optional[ForeignProperty] = None
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            # Left table foreign keys
            lmodel = fpr.left.prop.model
            if len(fpr.chain) > 1:
                # Use table alias of previous join.
                ltable = self.joins[fpr.chain[-2].name]
            else:
                # Use the main table, without alias.
                ltable = self.backend.get_table(lmodel)
            lenv = env(model=lmodel, table=ltable)
            lfkeys = lenv.call('join_table_on', fpr.left.prop)
            lfkeys = ensure_list(lfkeys)

            # Right table primary keys
            rpkeys = []
            rmodel = fpr.right.prop.model
            rtable = self.backend.get_table(rmodel).alias()
            renv = env(model=rmodel, table=rtable)
            for rpk in fpr.left.refprops:
                rpkeys += ensure_list(
                    renv.call('join_table_on', rpk)
                )

            # Number of keys on both left and right must be equal.
            assert len(lfkeys) == len(rpkeys), (lfkeys, rpkeys)
            condition = []
            for lfk, rpk in zip(lfkeys, rpkeys):
                condition += [lfk == rpk]

            # Build `JOIN rtable ON (condition)`.
            assert len(condition) > 0
            if len(condition) == 1:
                condition = condition[0]
            else:
                condition = sa.and_(*condition)

            self.joins[fpr.name] = rtable
            self.from_ = self.from_.outerjoin(rtable, condition)

        return self.joins[fpr.name]


class SqlQueryBuilder(Env):
    backend: Sql
    model: Model
    table: sa.Table
    joins: SqlFrom
    columns: List[sa.Column]
    # `resolved` is used to map which prop.place properties are already
    # resolved, usually it maps to Selected, but different DataType's can return
    # different results.
    resolved: Dict[str, Selected]
    selected: Dict[str, Selected] = None
    params: ResolvedParams

    def init(self, backend: Sql, table: sa.Table):
        return self(
            backend=backend,
            table=table,
            columns=[],
            resolved={},
            selected=None,
            joins=SqlFrom(backend, table),
            sort=[],
            limit=None,
            offset=None,
        )

    def build(self, where):
        if self.selected is None:
            # If select list was not explicitly given by client, then select all
            # properties.
            self.call('select', Expr('select'))

        qry = sa.select(self.columns)
        qry = qry.select_from(self.joins.from_)

        if where is not None:
            qry = qry.where(where)

        if self.sort:
            qry = qry.order_by(*self.sort)

        if self.limit is not None:
            qry = qry.limit(self.limit)

        if self.offset is not None:
            qry = qry.offset(self.offset)

        return qry

    def execute(self, expr: Any):
        expr = self.call('_resolve_unresolved', expr)
        return super().execute(expr)

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(name=expr.name, expr=str(expr(*args, **kwargs)))

    def add_column(self, column: Union[sa.Column, Function]) -> int:
        """Returns position in select column list, which is stored in
        Selected.item.
        """
        assert isinstance(column, (sa.Column, Function)), column
        if column not in self.columns:
            self.columns.append(column)
        return self.columns.index(column)


class Selected:
    # Item index in select list.
    item: int = None
    # Model property if a property is selected.
    prop: Property = None
    # A value or an Expr for further processing on selected value.
    prep: Any = NA

    def __init__(
        self,
        item: int = None,
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


@dataclasses.dataclass
class GetAttr(Unresolved):
    obj: str
    name: Union[GetAttr, Bind]


@ufunc.resolver(SqlQueryBuilder, Bind, Bind, name='getattr')
def getattr_(env: SqlQueryBuilder, obj: Bind, attr: Bind):
    return GetAttr(obj.name, attr)


@ufunc.resolver(SqlQueryBuilder, Bind, GetAttr, name='getattr')
def getattr_(env: SqlQueryBuilder, obj: Bind, attr: GetAttr):
    return GetAttr(obj.name, attr)


@ufunc.resolver(SqlQueryBuilder, GetAttr)
def _resolve_getattr(
    env: SqlQueryBuilder,
    attr: GetAttr,
) -> ForeignProperty:
    prop = env.model.properties[attr.obj]
    return env.call('_resolve_getattr', prop.dtype, attr.name)


@ufunc.resolver(SqlQueryBuilder, Ref, GetAttr)
def _resolve_getattr(
    env: SqlQueryBuilder,
    dtype: Ref,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = ForeignProperty(None, dtype, prop.dtype)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(SqlQueryBuilder, Ref, Bind)
def _resolve_getattr(
    env: SqlQueryBuilder,
    dtype: Ref,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype, prop.dtype)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, Ref, GetAttr)
def _resolve_getattr(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: GetAttr,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.obj]
    fpr = fpr.push(prop)
    return env.call('_resolve_getattr', fpr, prop.dtype, attr.name)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, Ref, Bind)
def _resolve_getattr(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: Ref,
    attr: Bind,
) -> ForeignProperty:
    prop = dtype.model.properties[attr.name]
    return fpr.push(prop)


@ufunc.resolver(SqlQueryBuilder, str, object, names=['eq', 'ne'])
def eq(env: SqlQueryBuilder, op: str, field: str, value: Any):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    return env.call(op, Bind(field), value)


def _sa_compare(op: str, column: sa.Column, value: Any):
    if op == 'eq':
        return column == value

    if op == 'ne':
        return column != value

    if op == 'lt':
        return column < value

    if op == 'le':
        return column <= value

    if op == 'gt':
        return column > value

    if op == 'ge':
        return column >= value

    if op == 'contains':
        return column.contains(value)

    if op == 'startswith':
        return column.startswith(value)

    raise NotImplementedError


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


T = TypeVar('T')


def _prepare_value(prop: Property, value: T) -> Union[T, List[T]]:
    return prepare_enum_value(prop, value)


@ufunc.resolver(SqlQueryBuilder, Bind, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, field: Bind, value: Any):
    prop = env.model.properties[field.name]
    value = _prepare_value(prop, value)
    return env.call(op, prop.dtype, value)


@ufunc.resolver(SqlQueryBuilder, GetAttr, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, attr: GetAttr, value: Any):
    fpr: ForeignProperty = env.call('_resolve_getattr', attr)
    value = _prepare_value(fpr.right.prop, value)
    return env.call(op, fpr, fpr.right, value)


@ufunc.resolver(SqlQueryBuilder, Bind, list, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, field: Bind, value: List[Any]):
    prop = env.model.properties[field.name]
    value = list(flatten(_prepare_value(prop, v) for v in value))
    return env.call(op, prop.dtype, value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, fpr: ForeignProperty, value: Any):
    value = _prepare_value(fpr.right.prop, value)
    return env.call(op, fpr, fpr.right, value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list, names=COMPARE)
def compare(
    env: SqlQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    value: List[Any],
):
    value = list(flatten(_prepare_value(fpr.right.prop, v) for v in value))
    return env.call(op, fpr, fpr.right, value)


@ufunc.resolver(SqlQueryBuilder, DataType, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, dtype: DataType, value: Any):
    column = env.backend.get_column(env.table, dtype.prop)
    return _sa_compare(op, column, value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType, object, names=COMPARE)
def compare(
    env: SqlQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: DataType,
    value: Any,
):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, dtype.prop)
    return _sa_compare(op, column, value)


@ufunc.resolver(SqlQueryBuilder, Function, object, names=COMPARE)
def compare(env: SqlQueryBuilder, op: str, func: Function, value: Any):
    return _sa_compare(op, func, value)


@ufunc.resolver(SqlQueryBuilder, DataType, list)
def eq(env: SqlQueryBuilder, dtype: DataType, value: List[Any]):
    column = env.backend.get_column(env.table, dtype.prop)
    return column.in_(value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType, list)
def eq(env: SqlQueryBuilder, fpr: ForeignProperty, dtype: DataType, value: list):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, dtype.prop)
    return column.in_(value)


@ufunc.resolver(SqlQueryBuilder, DataType, list)
def ne(env: SqlQueryBuilder, dtype: DataType, value: List[Any]):
    column = env.backend.get_column(env.table, dtype.prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType, list)
def ne(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: List[Any],
):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, fpr.right.prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list)
def ne(env: SqlQueryBuilder, fpr: ForeignProperty, value: List[Any]):
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, fpr.right.prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, object)
def _resolve_unresolved(env: SqlQueryBuilder, value: Any) -> Any:
    if isinstance(value, Unresolved):
        raise ValueError(f"Unresolved value {value!r}.")
    else:
        return value


@ufunc.resolver(SqlQueryBuilder, Bind)
def _resolve_unresolved(env: SqlQueryBuilder, field: Bind) -> sa.Column:
    prop = env.model.flatprops.get(field.name)
    if prop:
        return env.backend.get_column(env.table, prop)
    else:
        raise PropertyNotFound(env.model, property=field.name)


@ufunc.resolver(SqlQueryBuilder, GetAttr)
def _resolve_unresolved(env: SqlQueryBuilder, attr: GetAttr) -> sa.Column:
    fpr = env.call('_resolve_getattr', attr)
    table = env.joins.get_table(env, fpr)
    dtype = fpr.right
    return env.backend.get_column(table, dtype.prop)


@ufunc.resolver(SqlQueryBuilder, Expr, name='and')
def and_(env: SqlQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    args = [
        env.call('_resolve_unresolved', arg)
        for arg in args
        if arg is not None
    ]
    if len(args) > 1:
        return sa.and_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr, name='or')
def or_(env: SqlQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    args = [
        env.call('_resolve_unresolved', arg)
        for arg in args
        if arg is not None
    ]
    if len(args) > 1:
        return sa.or_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr, name='list')
def list_(env: SqlQueryBuilder, expr: Expr) -> List[Any]:
    args, kwargs = expr.resolve(env)
    return list(args)


@ufunc.resolver(SqlQueryBuilder, Expr)
def testlist(env: SqlQueryBuilder, expr: Expr) -> Tuple[Any]:
    args, kwargs = expr.resolve(env)
    return tuple(args)


@ufunc.resolver(SqlQueryBuilder)
def count(env: SqlQueryBuilder):
    return sa.func.count()


@ufunc.resolver(SqlQueryBuilder, Expr)
def select(env: SqlQueryBuilder, expr: Expr):
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

    if not env.columns:
        raise RuntimeError(
            f"{expr} didn't added anything to select list."
        )


@ufunc.resolver(SqlQueryBuilder, object)
def select(env: SqlQueryBuilder, value: Any) -> Selected:
    """For things like select(1, count())."""
    return Selected(item=env.add_column(value))


@ufunc.resolver(SqlQueryBuilder, Bind)
def select(env: SqlQueryBuilder, item: Bind, *, nested: bool = False):
    prop = _get_property_for_select(env, item.name, nested=nested)
    return env.call('select', prop)


@ufunc.resolver(SqlQueryBuilder, str)
def select(env: SqlQueryBuilder, item: str, *, nested: bool = False):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = _get_property_for_select(env, item, nested=nested)
    return env.call('select', prop)


def _get_property_for_select(
    env: SqlQueryBuilder,
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


@ufunc.resolver(SqlQueryBuilder, Property)
def select(env: SqlQueryBuilder, prop: Property) -> Selected:
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


@ufunc.resolver(SqlQueryBuilder, DataType)
def select(env: SqlQueryBuilder, dtype: DataType) -> Selected:
    table = env.backend.get_table(env.model)
    column = env.backend.get_column(table, dtype.prop, select=True)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(SqlQueryBuilder, DataType, object)
def select(env: SqlQueryBuilder, dtype: DataType, prep: Any) -> Selected:
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
        result = env.call('select', prep)
        return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(SqlQueryBuilder, PrimaryKey)
def select(
    env: SqlQueryBuilder,
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


@ufunc.resolver(SqlQueryBuilder, Ref, object)
def select(env: SqlQueryBuilder, dtype: Ref, prep: Any) -> Selected:
    fpr = ForeignProperty(None, dtype, dtype.model.properties['_id'].dtype)
    return Selected(
        prop=dtype.prop,
        prep=env.call('select', fpr, fpr.right.prop),
    )


@ufunc.resolver(SqlQueryBuilder, GetAttr)
def select(env: SqlQueryBuilder, attr: GetAttr) -> Selected:
    """For things like select(foo.bar.baz)."""
    fpr: ForeignProperty = env.call('_resolve_getattr', attr)
    return Selected(
        prop=fpr.right.prop,
        prep=env.call('select', fpr, fpr.right.prop),
    )


@ufunc.resolver(SqlQueryBuilder, ForeignProperty)
def select(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
) -> Selected:
    return env.call('select', fpr, fpr.right.prop)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, Property)
def select(
    env: SqlQueryBuilder,
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


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType)
def select(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
) -> Selected:
    table = env.joins.get_table(env, fpr)
    column = env.backend.get_column(table, dtype.prop, select=True)
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, DataType, object)
def select(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    prep: Any,
) -> Selected:
    result = env.call('select', fpr, prep)
    return Selected(prop=dtype.prop, prep=result)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, tuple)
def select(
    env: SqlQueryBuilder,
    fpr: ForeignProperty,
    prep: Tuple[Any],
) -> Tuple[Any]:
    return tuple(env.call('select', fpr, v) for v in prep)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, Bind)
def select(env: SqlQueryBuilder, fpr: ForeignProperty, item: Bind):
    model = fpr.right.prop.model
    prop = model.flatprops.get(item.name)
    if prop and authorized(env.context, prop, Action.SEARCH):
        return env.call('select', fpr, prop)
    else:
        raise PropertyNotFound(model, property=item.name)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, PrimaryKey)
def select(
    env: SqlQueryBuilder,
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


@ufunc.resolver(SqlQueryBuilder, list)
def select(
    env: SqlQueryBuilder,
    prep: List[Any],
) -> List[Any]:
    return [env.call('select', v) for v in prep]


@ufunc.resolver(SqlQueryBuilder, tuple)
def select(
    env: SqlQueryBuilder,
    prep: Tuple[Any],
) -> Tuple[Any]:
    return tuple(env.call('select', v) for v in prep)


@ufunc.resolver(SqlQueryBuilder, dict)
def select(
    env: SqlQueryBuilder,
    prep: Dict[str, Any],
) -> Dict[str, Any]:
    # TODO: Add tests.
    return {k: env.call('select', v) for k, v in prep.items()}


@ufunc.resolver(SqlQueryBuilder, Property)
def join_table_on(env: SqlQueryBuilder, prop: Property) -> Any:
    if prop.external.prepare is not NA:
        if isinstance(prop.external.prepare, Expr):
            result = env.resolve(prop.external.prepare)
        else:
            result = prop.external.prepare
        return env.call('join_table_on', prop.dtype, result)
    else:
        return env.call('join_table_on', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, DataType)
def join_table_on(env: SqlQueryBuilder, dtype: DataType) -> Tuple[Any]:
    column = env.backend.get_column(env.table, dtype.prop)
    return column


@ufunc.resolver(SqlQueryBuilder, PrimaryKey)
def join_table_on(env: SqlQueryBuilder, dtype: DataType) -> Any:
    model = dtype.prop.model
    pkeys = model.external.pkeys

    if not pkeys:
        raise RuntimeError(
            f"Can't join {dtype.prop} on right table without primary key."
        )

    if len(pkeys) == 1:
        prop = pkeys[0]
        result = env.call('join_table_on', prop)
    else:
        result = [
            env.call('join_table_on', prop)
            for prop in pkeys
        ]

    return result


@ufunc.resolver(SqlQueryBuilder, DataType, tuple)
def join_table_on(
    env: SqlQueryBuilder,
    dtype: DataType,
    prep: tuple,
) -> Tuple[Any]:
    return tuple(env.call('join_table_on', v) for v in prep)


@ufunc.resolver(SqlQueryBuilder, Bind)
def join_table_on(env: SqlQueryBuilder, item: Bind):
    prop = env.model.flatprops.get(item.name)
    if not prop or not authorized(env.context, prop, Action.SEARCH):
        raise PropertyNotFound(env.model, property=item.name)
    return env.call('join_table_on', prop)


@ufunc.resolver(SqlQueryBuilder, Bind, name='len')
def len_(env: SqlQueryBuilder, bind: Bind):
    prop = env.model.flatprops[bind.name]
    return env.call('len', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, str, name='len')
def len_(env: SqlQueryBuilder, bind: str):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = env.model.flatprops[bind]
    return env.call('len', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, DataType, name='len')
def len_(env: SqlQueryBuilder, dtype: DataType):
    column = env.backend.get_column(env.table, dtype.prop)
    return sa.func.length(column)


@ufunc.resolver(SqlQueryBuilder, Expr)
def sort(env: SqlQueryBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)
    env.sort = []
    for key in args:
        prop = env.model.properties[key.name]
        column = env.backend.get_column(env.table, prop)
        if isinstance(key, Negative):
            column = column.desc()
        else:
            column = column.asc()
        env.sort.append(column)


@ufunc.resolver(SqlQueryBuilder, int)
def limit(env: SqlQueryBuilder, n: int):
    env.limit = n


@ufunc.resolver(SqlQueryBuilder, int)
def offset(env: SqlQueryBuilder, n: int):
    env.offset = n


@ufunc.resolver(SqlQueryBuilder, Property, object, object)
def swap(env: SqlQueryBuilder, prop: Property, old: Any, new: Any) -> Any:
    return Expr('swap', old, new)


@ufunc.resolver(SqlQueryBuilder, Expr)
def file(env: SqlQueryBuilder, expr: Expr) -> Expr:
    args, kwargs = expr.resolve(env)
    return Expr(
        'file',
        name=env.call('select', kwargs['name'], nested=True),
        content=env.call('select', kwargs['content'], nested=True),
    )


class _FileSelected(TypedDict):
    name: Selected      # File name
    content: Selected   # File content


@ufunc.resolver(SqlResultBuilder, Expr)
def file(env: SqlResultBuilder, expr: Expr) -> FileData:
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


@ufunc.resolver(SqlQueryBuilder)
def cast(env: SqlQueryBuilder) -> Expr:
    return Expr('cast')


@ufunc.resolver(SqlResultBuilder)
def cast(env: SqlResultBuilder) -> Any:
    return env.call('cast', env.prop.dtype, env.this)


@ufunc.resolver(SqlResultBuilder, String, int)
def cast(env: SqlResultBuilder, dtype: String, value: int) -> str:
    return str(value)


@ufunc.resolver(SqlResultBuilder, String, type(None))
def cast(env: SqlResultBuilder, dtype: String, value: Optional[Any]) -> str:
    return ''
