from __future__ import annotations

from typing import Any

import dataclasses
from typing import Optional

import sqlalchemy as sa
import sqlalchemy.sql.functions

from spinta.auth import authorized
from spinta.core.ufuncs import Env, ufunc
from spinta.core.ufuncs import Expr, Bind, Negative
from spinta.exceptions import UnknownExpr
from spinta.exceptions import PropertyNotFound
from spinta.components import Action, Property
from spinta.backends.components import Backend
from spinta.types.datatype import DataType
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.utils.data import take
from spinta.utils.schema import NA


class SqlQueryBuilder(Env):

    def init(self, backend: Backend, table: sa.Table):
        return self(
            backend=backend,
            table=table,
            select=None,
            joins={},
            from_=table,
            sort=[],
            limit=None,
            offset=None,
        )

    def build(self, where):
        if self.select is None:
            self.call('select', Expr('select'))

        select = []
        for sel in self.select.values():
            if sel.item is None:
                continue
            if isinstance(sel.item, list):
                select += sel.item
            else:
                select.append(sel.item)
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
        raise UnknownExpr(expr=str(expr(*args, **kwargs)), name=expr.name)

    def get_joined_table(self, prop: ForeignProperty) -> sa.Table:
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            ltable = self.backend.get_table(fpr.left.model)
            lrkeys = [self.backend.get_column(ltable, fpr.left)]

            rmodel = fpr.right.model
            rtable = self.backend.get_table(rmodel)
            rpkeys = []
            for rpk in fpr.left.dtype.refprops:
                if isinstance(rpk.dtype, PrimaryKey):
                    rpkeys += [
                        self.backend.get_column(rtable, rpk)
                        for rpk in rmodel.external.pkeys
                    ]
                else:
                    rpkeys += [
                        self.backend.get_column(rtable, rpk)
                    ]

            assert len(lrkeys) == len(rpkeys), (lrkeys, rpkeys)
            condition = []
            for lrk, rpk in zip(lrkeys, rpkeys):
                condition += [lrk == rpk]

            assert len(condition) > 0
            if len(condition) == 1:
                condition = condition[0]
            else:
                condition = sa.and_(condition)

            self.from_ = self.joins[fpr.name] = self.from_.join(rtable,
                                                                condition)

        model = fpr.right.model
        table = self.backend.get_table(model)
        return table


class ForeignProperty:

    def __init__(
        self, fpr: Optional[ForeignProperty],
        left: Property,
        right: Property,
    ):
        if fpr is None:
            self.name = left.place
            self.chain = [self]
        else:
            self.name += '->' + left.place
            self.chain = fpr.chain + [self]

        self.left = left
        self.right = right

    def __repr__(self):
        return f'<{self.name}->{self.right.name}:{self.right.dtype.name}>'


@dataclasses.dataclass
class Selected:
    item: Any
    prop: Property = None
    prep: Any = NA


@ufunc.resolver(SqlQueryBuilder, Bind, Bind)
def getattr(env, field, attr):
    prop = env.model.properties[field.name]
    return env.call('getattr', prop.dtype, attr)


@ufunc.resolver(SqlQueryBuilder, Ref, Bind)
def getattr(env, dtype, attr):
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype.prop, prop)


@ufunc.resolver(SqlQueryBuilder, str, object)
def eq(env, field, value):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = env.model.properties[field]
    column = env.backend.get_column(env.table, prop)
    return column == value


@ufunc.resolver(SqlQueryBuilder, Bind, object)
def eq(env, field, value):
    prop = env.model.properties[field.name]
    column = env.backend.get_column(env.table, prop)
    return column == value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, object)
def eq(env, fpr, value):
    table = env.get_joined_table(fpr)
    column = env.backend.get_column(table, fpr.right)
    return column == value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list)
def eq(env, fpr, value):
    table = env.get_joined_table(fpr)
    column = env.backend.get_column(table, fpr.right)
    return column.in_(value)


@ufunc.resolver(SqlQueryBuilder, sqlalchemy.sql.functions.Function, object)
def eq(env, func, value):
    return func == value


@ufunc.resolver(SqlQueryBuilder, str, object)
def ne(env, field, value):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = env.model.properties[field]
    column = env.backend.get_column(env.table, prop)
    return column != value


@ufunc.resolver(SqlQueryBuilder, Bind, object)
def ne(env, field, value):
    prop = env.model.properties[field.name]
    column = env.backend.get_column(env.table, prop)
    return column != value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, object)
def ne(env, fpr, value):
    table = env.get_joined_table(fpr)
    column = env.backend.get_column(table, fpr.right)
    return column != value


@ufunc.resolver(SqlQueryBuilder, str, list)
def ne(env, field, value):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = env.model.properties[field]
    column = env.backend.get_column(env.table, prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, Bind, list)
def ne(env, field, value):
    prop = env.model.properties[field.name]
    column = env.backend.get_column(env.table, prop)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list)
def ne(env, fpr, value):
    table = env.get_joined_table(fpr)
    column = env.backend.get_column(table, fpr.right)
    return ~column.in_(value)


@ufunc.resolver(SqlQueryBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    if len(args) > 1:
        return sa.and_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    if len(args) > 1:
        return sa.or_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr, name='list')
def list_(env, expr):
    args, kwargs = expr.resolve(env)
    return args


@ufunc.resolver(SqlQueryBuilder)
def count(env):
    env.select = {
        'count()': Selected(sa.func.count()),
    }


@ufunc.resolver(SqlQueryBuilder, Expr)
def select(env, expr):
    keys = [str(k) for k in expr.args]
    args, kwargs = expr.resolve(env)
    args = list(zip(keys, args)) + list(kwargs.items())

    env.select = {}

    if args:
        for key, arg in args:
            env.select[key] = env.call('select', arg)
    else:
        for prop in take(['_id', all], env.model.properties).values():
            if authorized(env.context, prop, Action.GETALL):
                env.select[prop.place] = env.call('select', prop.dtype)

    assert env.select, args


@ufunc.resolver(SqlQueryBuilder, Bind)
def select(env, arg):
    prop = _get_property_for_select(env, arg.name)
    return env.call('select', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, str)
def select(env, arg):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = _get_property_for_select(env, arg)
    return env.call('select', prop.dtype)


def _get_property_for_select(env: SqlQueryBuilder, name: str):
    prop = env.model.flatprops.get(name)
    if prop and authorized(env.context, prop, Action.SEARCH):
        return prop
    else:
        raise PropertyNotFound(env.model, property=name)


@ufunc.resolver(SqlQueryBuilder, DataType)
def select(env, dtype):
    if dtype.prop.external.prepare is not None:
        return env.call('select', dtype, dtype.prop.external.prepare)
    else:
        table = env.backend.get_table(env.model)
        column = env.backend.get_column(table, dtype.prop, select=True)
        return Selected(column, dtype.prop)


@ufunc.resolver(SqlQueryBuilder, DataType, object)
def select(env, dtype, prep):
    return Selected(None, dtype.prop, prep)


@ufunc.resolver(SqlQueryBuilder, PrimaryKey)
def select(env, dtype):
    table = env.backend.get_table(env.model)
    if env.model.external.pkeys:
        columns = [
            env.backend.get_column(table, prop, select=True)
            for prop in env.model.external.pkeys
        ]
    else:
        columns = [
            env.backend.get_column(table, prop, select=True)
            for prop in take(env.model.properties).values()
        ]
    return Selected(columns, dtype.prop)


@ufunc.resolver(SqlQueryBuilder, Ref)
def select(env, dtype):
    prop = dtype.prop
    table = env.backend.get_table(env.model)
    if isinstance(prop.external, list):
        raise NotImplementedError
    else:
        columns = [env.backend.get_column(table, prop, select=True)]
    return Selected(columns, dtype.prop)


@ufunc.resolver(SqlQueryBuilder, sqlalchemy.sql.functions.Function)
def select(env, func):
    return Selected(func)


@ufunc.resolver(SqlQueryBuilder, Bind, name='len')
def len_(env, bind):
    prop = env.model.flatprops[bind.name]
    return env.call('len', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, str, name='len')
def len_(env, bind):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = env.model.flatprops[bind]
    return env.call('len', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, DataType, name='len')
def len_(env, dtype):
    column = env.backend.get_column(env.table, dtype.prop)
    return sa.func.length(column)


@ufunc.resolver(SqlQueryBuilder, Expr)
def sort(env, expr):
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
def limit(env, n):
    env.limit = n


@ufunc.resolver(SqlQueryBuilder, int)
def offset(env, n):
    env.offset = n
