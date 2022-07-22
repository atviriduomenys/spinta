from __future__ import annotations

from typing import List, Union, Any

import datetime
import dataclasses
from typing import Optional

import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import UUID

from spinta import exceptions
from spinta.auth import authorized
from spinta.core.ufuncs import Env, ufunc
from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Expr
from spinta.exceptions import EmptyStringSearch
from spinta.exceptions import UnknownMethod
from spinta.exceptions import FieldNotInResource
from spinta.components import Action, Model, Property
from spinta.utils.data import take
from spinta.types.datatype import DataType
from spinta.types.datatype import Array
from spinta.types.datatype import File
from spinta.types.datatype import Object
from spinta.types.datatype import Ref
from spinta.types.datatype import String
from spinta.types.datatype import Integer
from spinta.types.datatype import Number
from spinta.types.datatype import DateTime
from spinta.types.datatype import Date
from spinta.types.datatype import PrimaryKey
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.components import BackendFeatures


class PgQueryBuilder(Env):
    backend: PostgreSQL

    def init(self, backend: PostgreSQL, table: sa.Table):
        return self(
            backend=backend,
            table=table,
            # TODO: Select list must be taken from params.select.
            select=None,
            joins={},
            from_=table,
            sort=[],
            limit=None,
            offset=None,
            aggregate=False,
        )

    def build(self, where):
        if self.select is None:
            self.call('select', Expr('select'))

        if self.aggregate:
            select = []
        else:
            select = [
                self.table.c['_id'],
                self.table.c['_revision'],
            ]
        for sel in self.select.values():
            items = sel.item if isinstance(sel.item, list) else [sel.item]
            for item in items:
                if item is not None and item not in select:
                    select.append(item)
        qry = sa.select(select)

        qry = qry.select_from(self.from_)

        if where is not None:
            qry = qry.where(where)

        if self.sort:
            qry = qry.order_by(*self.sort)

        if self.limit is not None:
            qry = qry.limit(self.limit)

        if self.offset is not None:
            qry = qry.offset(self.offset)

        return qry

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownMethod(expr=repr(expr(*args, **kwargs)), name=expr.name)

    def get_joined_table(self, prop: ForeignProperty) -> sa.Table:
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            if len(fpr.chain) > 1:
                ltable = self.joins[fpr.chain[-2].name]
            else:
                ltable = self.backend.get_table(fpr.left.model)
            lrkey = self.backend.get_column(ltable, fpr.left)

            rmodel = fpr.right.model
            rtable = self.backend.get_table(rmodel).alias()
            rpkey = self.backend.get_column(rtable, rmodel.properties['_id'])

            condition = lrkey == rpkey
            self.joins[fpr.name] = rtable
            self.from_ = self.from_.outerjoin(rtable, condition)

        return self.joins[fpr.name]


class ForeignProperty:

    def __init__(
        self,
        fpr: Optional[ForeignProperty],
        left: Property,
        right: Property,
    ):
        if fpr is None:
            self.name = left.place
            self.chain = [self]
        else:
            self.name = fpr.name + '->' + left.place
            self.chain = fpr.chain + [self]

        self.left = left
        self.right = right

    def __repr__(self):
        return (
            'ForeignProperty('
            f'{self.name}->{self.right.name}:{self.right.dtype.name}'
            ')'
        )

    @property
    def place(self):
        return '.'.join(
            [fpr.left.place for fpr in self.chain] +
            [self.right.place]
        )


class Func:
    pass


@dataclasses.dataclass
class Lower(Func):
    dtype: DataType = None


@dataclasses.dataclass
class Negative(Func):
    arg: Any


@dataclasses.dataclass
class Positive(Func):
    arg: Any


class Star:
    pass


@dataclasses.dataclass
class Recurse(Func):
    args: List[Union[DataType, Func]] = None


@dataclasses.dataclass
class ReservedProperty(Func):
    dtype: DataType
    param: str


@ufunc.resolver(PgQueryBuilder, str, name='op')
def op_(env, arg: str):
    if arg == '*':
        return Star()
    else:
        raise NotImplementedError


@ufunc.resolver(PgQueryBuilder, Bind, Bind, name='getattr')
def getattr_(env, field, attr):
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.anem)
    return env.call('getattr', prop.dtype, attr)


@ufunc.resolver(PgQueryBuilder, Object, Bind, name='getattr')
def getattr_(env, dtype, attr):
    if attr.name in dtype.properties:
        return dtype.properties[attr.name].dtype
    else:
        raise FieldNotInResource(dtype, property=attr.name)


@ufunc.resolver(PgQueryBuilder, Array, Bind, name='getattr')
def getattr_(env, dtype, attr):
    return env.call('getattr', dtype.items.dtype, attr)


@ufunc.resolver(PgQueryBuilder, File, Bind, name='getattr')
def getattr_(env, dtype, attr):
    return ReservedProperty(dtype, attr.name)


@ufunc.resolver(PgQueryBuilder, Ref, Bind, name='getattr')
def getattr_(env, dtype, attr):
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype.prop, prop)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, Bind, name='getattr')
def getattr_(env, fpr, attr):
    prop = fpr.right.dtype.model.properties[attr.name]
    return ForeignProperty(fpr, fpr.right, prop)


@ufunc.resolver(PgQueryBuilder, Expr)
def select(env, expr):
    keys = [str(k) for k in expr.args]
    args, kwargs = expr.resolve(env)
    args = list(zip(keys, args)) + list(kwargs.items())

    env.select = {}

    if args:
        for key, arg in args:
            selected = env.call('select', arg)
            if selected is not None:
                env.select[key] = selected
    else:
        env.call('select', Star())

    assert env.select, args


@ufunc.resolver(PgQueryBuilder, Star)
def select(env, arg: Star) -> None:
    for prop in take(env.model.properties).values():
        # if authorized(env.context, prop, (Action.GETALL, Action.SEARCH)):
        # TODO: This line above should come from a getall(request),
        #       because getall can be used internally for example for
        #       writes.
        env.select[prop.place] = env.call('select', prop.dtype)


@ufunc.resolver(PgQueryBuilder, Bind)
def select(env, arg):
    if arg.name == '_type':
        return Selected(None, env.model.properties['_type'])
    prop = _get_property_for_select(env, arg.name)
    return env.call('select', prop.dtype)


def _get_property_for_select(env: PgQueryBuilder, name: str):
    prop = env.model.properties.get(name)
    if prop and authorized(env.context, prop, Action.SEARCH):
        return prop
    else:
        raise FieldNotInResource(env.model, property=name)


@dataclasses.dataclass
class Selected:
    item: Any
    prop: Property = None


@ufunc.resolver(PgQueryBuilder, DataType)
def select(env, dtype):
    table = env.backend.get_table(env.model)
    if dtype.prop.list is None:
        column = env.backend.get_column(table, dtype.prop, select=True)
    else:
        # XXX: Probably if dtype.prop is in a nested list, we need to get the
        #      first list. Because if I remember correctly, only first list is
        #      stored on the main table.
        column = env.backend.get_column(table, dtype.prop.list, select=True)
    return Selected(column, dtype.prop)


@ufunc.resolver(PgQueryBuilder, Object)
def select(env, dtype):
    columns = []
    for prop in take(dtype.properties).values():
        sel = env.call('select', prop.dtype)
        if isinstance(sel.item, list):
            columns += sel.item
        else:
            columns += [sel.item]
    return Selected(columns, dtype.prop)


@ufunc.resolver(PgQueryBuilder, File)
def select(env, dtype):
    table = env.backend.get_table(env.model)
    columns = [
        table.c[dtype.prop.place + '._id'],
        table.c[dtype.prop.place + '._content_type'],
        table.c[dtype.prop.place + '._size'],
    ]
    same_backend = dtype.backend.name == dtype.prop.model.backend.name
    if same_backend or BackendFeatures.FILE_BLOCKS in dtype.backend.features:
        columns += [
            table.c[dtype.prop.place + '._bsize'],
            table.c[dtype.prop.place + '._blocks'],
        ]
    return Selected(columns, dtype.prop)


@ufunc.resolver(PgQueryBuilder, ReservedProperty)
def select(env, prop):
    return env.call('select', prop.dtype, prop.param)


@ufunc.resolver(PgQueryBuilder, File, str)
def select(env, dtype, leaf):
    table = env.backend.get_table(env.model)
    if leaf == '_content':
        return env.call('select', dtype)
    else:
        column = table.c[dtype.prop.place + '.' + leaf]
        return Selected(column, dtype.prop)


@ufunc.resolver(PgQueryBuilder, Ref)
def select(env, dtype):
    table = env.backend.get_table(env.model)
    column = table.c[dtype.prop.place + '._id']
    return Selected(column, dtype.prop)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def select(env: PgQueryBuilder, fpr: ForeignProperty):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    column = column.label(fpr.place)
    return Selected(column, fpr.right)


@ufunc.resolver(PgQueryBuilder, int)
def limit(env, n):
    env.limit = n


@ufunc.resolver(PgQueryBuilder, int)
def offset(env, n):
    env.offset = n


@ufunc.resolver(PgQueryBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    if len(args) > 1:
        return sa.and_(*args)
    elif args:
        return args[0]


@ufunc.resolver(PgQueryBuilder, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call('or', args)


@ufunc.resolver(PgQueryBuilder, list, name='or')
def or_(env, args):
    if len(args) > 1:
        return sa.or_(*args)
    elif args:
        return args[0]


@ufunc.resolver(PgQueryBuilder)
def count(env):
    env.aggregate = True
    env.select = {
        'count()': Selected(sa.func.count().label('count()')),
    }


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


@ufunc.resolver(PgQueryBuilder, Bind, object, names=COMPARE)
def compare(env, op, field, value):
    prop = _get_from_flatprops(env.model, field.name)
    return env.call(op, prop.dtype, value)


def _get_from_flatprops(model: Model, prop: str):
    if prop in model.flatprops:
        return model.flatprops[prop]
    else:
        raise exceptions.FieldNotInResource(model, property=prop)


@ufunc.resolver(PgQueryBuilder, DataType, object, names=COMPARE)
def compare(env, op, dtype, value):
    raise exceptions.InvalidValue(dtype, op=op, arg=type(value).__name__)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType, object, names=COMPARE)
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: DataType,
    value: Any,
):
    raise exceptions.InvalidValue(dtype, op=op, arg=type(value).__name__)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, object, names=COMPARE)
def compare(env, op: str, fpr: ForeignProperty, value: Any):
    return env.call(op, fpr, fpr.right.dtype, value)


@ufunc.resolver(PgQueryBuilder, DataType, type(None))
def eq(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType, type(None))
def eq(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: type(None),
):
    table = env.get_joined_table(fpr)
    column = env.backend.get_column(table, fpr.right)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, PrimaryKey, str)
def eq(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: type(None),
):
    table = env.backend.get_table(fpr.left.model)
    column = env.backend.get_column(table, fpr.left)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


def _ensure_non_empty(op, s):
    if s == '':
        raise EmptyStringSearch(op=op)


@ufunc.resolver(PgQueryBuilder, String, str, names=[
    'eq', 'startswith', 'contains',
])
def compare(env, op, dtype, value):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, String, str, names=[
    'eq', 'startswith', 'contains',
])
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: String,
    value: str,
):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, PrimaryKey, str, names=[
    'eq', 'startswith', 'contains',
])
def compare(env, op, dtype, value):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    column = env.backend.get_column(env.table, dtype.prop)
    return _sa_compare(op, column, value)


@ufunc.resolver(PgQueryBuilder, (Integer, Number), (int, float), names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, (Integer, Number), (int, float), names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: Union[Integer, Number],
    value: Union[int, float],
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, DateTime, str, names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.datetime.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DateTime, str, names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: DateTime,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    value = datetime.datetime.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, Date, str, names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.date.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, Date, str, names=[
    'eq', 'lt', 'le', 'gt', 'ge',
])
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: Date,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    value = datetime.date.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, String)
def lower(env, dtype):
    return Lower(dtype)


@ufunc.resolver(PgQueryBuilder, Recurse)
def lower(env, recurse):
    return Recurse([env.call('lower', arg) for arg in recurse.args])


@ufunc.resolver(PgQueryBuilder, Lower, str, names=[
    'eq', 'startswith', 'contains',
])
def compare(env, op, fn, value):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    column = env.backend.get_column(env.table, fn.dtype.prop)
    column = sa.func.lower(column)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, fn.dtype.prop, cond)


def _sa_compare(op, column, value):
    if op == 'eq':
        return column == value

    if op == 'lt':
        return column < value

    if op == 'le':
        return column <= value

    if op == 'gt':
        return column > value

    if op == 'ge':
        return column >= value

    if op == 'contains':
        if isinstance(column.type, UUID):
            column = column.cast(sa.String)
        return column.contains(value)

    if op == 'startswith':
        if isinstance(column.type, UUID):
            column = column.cast(sa.String)
        return column.startswith(value)
    raise NotImplementedError


def _prepare_condition(env: PgQueryBuilder, prop: Property, cond):
    if prop.list is None:
        return cond

    main_table = env.table
    list_table = env.backend.get_table(prop.list, TableType.LIST)
    subqry = (
        sa.select(
            [list_table.c._rid],
            distinct=list_table.c._rid,
        ).
        where(cond).
        alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry,
        main_table.c._id == subqry.c._rid,
    )
    return subqry.c._rid.isnot(None)


@ufunc.resolver(PgQueryBuilder, DataType, type(None))
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType, type(None))
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: type(None),
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, String, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, String, str)
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, PrimaryKey, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, (Integer, Number), (int, float))
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, (Integer, Number), (int, float))
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: Union[Integer, Number],
    value: Union[int, float],
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, DateTime, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.datetime.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DateTime, str)
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DateTime,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    value = datetime.datetime.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, Date, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.date.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, Date, str)
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: Date,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    value = datetime.date.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, Array, (object, type(None)), names=[
    'eq', 'ne', 'lt', 'le', 'gt', 'ge', 'contains', 'startswith',
])
def compare(env, op, dtype, value):
    return env.call(op, dtype.items.dtype, value)


@ufunc.resolver(PgQueryBuilder, Lower, str)
def ne(env, fn, value):
    column = env.backend.get_column(env.table, fn.dtype.prop)
    column = sa.func.lower(column)
    return _ne_compare(env, fn.dtype.prop, column, value)


def _ne_compare(env: PgQueryBuilder, prop: Property, column, value):
    """Not equal operator is quite complicated thing and need explaining.

    If property is not defined within a list, just do `!=` comparison and be
    done with it.

    If property is in a list:

    - First check if there is at least one list item where field is not None
        (existance check).

    - Then check if there is no list items where field equals to given
        value.
    """

    if prop.list is None:
        return column != value

    main_table = env.backend.get_table(prop.model)
    list_table = env.backend.get_table(prop.list, TableType.LIST)

    # Check if at liest one value for field is defined
    subqry1 = (
        sa.select(
            [list_table.c._rid],
            distinct=list_table.c._rid,
        ).
        where(column != None).  # noqa
        alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry1,
        main_table.c._id == subqry1.c._rid,
    )

    # Check if given value exists
    subqry2 = (
        sa.select(
            [list_table.c._rid],
            distinct=list_table.c._rid,
        ).
        where(column == value).
        alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry2,
        main_table.c._id == subqry2.c._rid,
    )

    # If field exists and given value does not, then field is not equal to
    # value.
    return sa.and_(
        subqry1.c._rid != None,  # noqa
        subqry2.c._rid == None,
    )


FUNCS = [
    'lower',
    'upper',
]


@ufunc.resolver(PgQueryBuilder, Bind, names=FUNCS)
def func(env, name, field):
    prop = env.model.flatprops[field.name]
    return env.call(name, prop.dtype)


@ufunc.resolver(PgQueryBuilder, Bind)
def recurse(env, field):
    if field.name in env.model.leafprops:
        return Recurse([prop.dtype for prop in env.model.leafprops[field.name]])
    else:
        raise exceptions.FieldNotInResource(env.model, property=field.name)


@ufunc.resolver(PgQueryBuilder, Recurse, object, names=[
    'eq', 'ne', 'lt', 'le', 'gt', 'ge', 'contains', 'startswith',
])
def recurse(env, op, recurse, value):
    return env.call('or', [
        env.call(op, arg, value)
        for arg in recurse.args
    ])


@ufunc.resolver(PgQueryBuilder, Expr, name='any')
def any_(env, expr):
    args, kwargs = expr.resolve(env)
    op, field, *args = args
    if isinstance(op, Bind):
        op = op.name
    return env.call('or', [
        env.call(op, field, arg)
        for arg in args
    ])


@ufunc.resolver(PgQueryBuilder, Expr)
def sort(env, expr):
    args, kwargs = expr.resolve(env)
    env.sort = [
        env.call('sort', arg) for arg in args
    ]


@ufunc.resolver(PgQueryBuilder, Bind)
def sort(env, field):
    prop = _get_from_flatprops(env.model, field.name)
    return env.call('asc', prop.dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def sort(env: PgQueryBuilder, fpr: ForeignProperty):
    return env.call('asc', fpr, fpr.right.dtype)


@ufunc.resolver(PgQueryBuilder, Positive)
def sort(env, sign):
    return env.call('asc', sign.arg)


@ufunc.resolver(PgQueryBuilder, Negative)
def sort(env, sign):
    return env.call('desc', sign.arg)


@ufunc.resolver(PgQueryBuilder, DataType)
def asc(env, dtype):
    column = _get_sort_column(env, dtype.prop)
    return column.asc()


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def asc(env: PgQueryBuilder, fpr: ForeignProperty):
    return env.call('asc', fpr, fpr.right.dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType)
def asc(env: PgQueryBuilder, fpr: ForeignProperty, dtype: DataType):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    return column.asc()


@ufunc.resolver(PgQueryBuilder, DataType)
def desc(env, dtype):
    column = _get_sort_column(env, dtype.prop)
    return column.desc()


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def desc(env: PgQueryBuilder, fpr: ForeignProperty):
    return env.call('desc', fpr, fpr.right.dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType)
def desc(env: PgQueryBuilder, fpr: ForeignProperty, dtype: DataType):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.place]
    return column.desc()


@ufunc.resolver(PgQueryBuilder, Array, names=['asc', 'desc'])
def sort(env, name, dtype: Array):
    return env.call(name, dtype.items.dtype)


def _get_sort_column(env: PgQueryBuilder, prop: Property):
    column = env.backend.get_column(env.table, prop)

    if prop.list is None:
        return column

    main_table = env.table
    list_table = env.backend.get_table(prop.list, TableType.LIST)
    subqry = (
        sa.select(
            [list_table.c._rid, column.label('value')],
            distinct=list_table.c._rid,
        ).alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry,
        main_table.c._id == subqry.c._rid,
    )
    return subqry.c.value


@ufunc.resolver(PgQueryBuilder, Bind)
def negative(env, field) -> Negative:
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.name)
    return Negative(prop.dtype)


@ufunc.resolver(PgQueryBuilder, Bind)
def positive(env, field) -> Positive:
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.name)
    return Positive(prop.dtype)


@ufunc.resolver(PgQueryBuilder, DataType)
def negative(env, dtype) -> Negative:
    return Negative(dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def negative(env: PgQueryBuilder, fpr: ForeignProperty) -> Negative:
    return Negative(fpr)


@ufunc.resolver(PgQueryBuilder, DataType)
def positive(env, dtype) -> Positive:
    return Positive(dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def positive(env: PgQueryBuilder, fpr: ForeignProperty) -> Positive:
    return Positive(fpr)
