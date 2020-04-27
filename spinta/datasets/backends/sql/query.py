from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy.sql.functions

from spinta.core.ufuncs import Env, ufunc
from spinta.core.ufuncs import Expr, Bind, Negative
from spinta.exceptions import UnknownExpr
from spinta.components import Property
from spinta.backends.components import Backend
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref


class SqlQueryBuilder(Env):

    def init(self, backend: Backend, table: sa.Table):
        return self(
            backend=backend,
            table=table,
            select=[table],
            joins={},
            from_=table,
            sort=[],
            limit=None,
            offset=None,
        )

    def build(self, where):
        qry = sa.select(self.select)
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

            lmodel = fpr.left.model
            ltable = self.backend.get_table(lmodel.external.name)
            lrkeys = [fpr.left.external.name]

            rmodel = fpr.right.model
            rtable = self.backend.get_table(rmodel.external.name)
            rpkeys = fpr.left.dtype.refprops or rmodel.external.pkeys
            rpkeys = [k.external.name for k in rpkeys]

            assert len(lrkeys) == len(rpkeys), (lrkeys, rpkeys)
            condition = []
            for lrk, rpk in zip(lrkeys, rpkeys):
                condition += [ltable.c[lrk] == rtable.c[rpk]]

            assert len(condition) > 0
            if len(condition) == 1:
                condition = condition[0]
            else:
                condition = sa.and_(condition)

            self.from_ = self.joins[fpr.name] = self.from_.join(rtable, condition)

        model = fpr.right.model
        table = self.backend.get_table(model.external.name)
        return table


class ForeignProperty:

    def __init__(self, fpr: ForeignProperty, left: Property, right: Property):
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


@ufunc.resolver(SqlQueryBuilder, Bind, Bind)
def getattr(env, field, attr):
    prop = env.model.properties[field.name]
    return env.call('getattr', prop.dtype, attr)


@ufunc.resolver(SqlQueryBuilder, Ref, Bind)
def getattr(env, dtype, attr):
    prop = dtype.model.properties[attr.name]
    return ForeignProperty(None, dtype.prop, prop)


@ufunc.resolver(SqlQueryBuilder, Bind, str)
def eq(env, field, value):
    name = env.model.properties[field.name].external.name
    return env.table.c[name] == value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, str)
def eq(env, prop, value):
    table = env.get_joined_table(prop)
    name = prop.right.external.name
    return table.c[name] == value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list)
def eq(env, prop, value):
    table = env.get_joined_table(prop)
    name = prop.right.external.name
    return table.c[name].in_(value)


@ufunc.resolver(SqlQueryBuilder, sqlalchemy.sql.functions.Function, object)
def eq(env, func, value):
    return func == value


@ufunc.resolver(SqlQueryBuilder, Bind, str)
def ne(env, field, value):
    name = env.model.properties[field.name].external.name
    return env.table.c[name] != value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, str)
def ne(env, prop, value):
    table = env.get_joined_table(prop)
    name = prop.right.external.name
    return table.c[name] != value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, list)
def ne(env, prop, value):
    table = env.get_joined_table(prop)
    name = prop.right.external.name
    return ~table.c[name].in_(value)


@ufunc.resolver(SqlQueryBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    if len(args) > 1:
        return sa.and_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr, name='list')
def list_(env, expr):
    args, kwargs = expr.resolve(env)
    return args


@ufunc.resolver(SqlQueryBuilder, Expr)
def select(env, expr):
    args, kwargs = expr.resolve(env)
    if args:
        env.select = [
            env.call('select', arg)
            for arg in args
        ]
    else:
        env.select = [env.table]


@ufunc.resolver(SqlQueryBuilder, Bind)
def select(env, arg):
    prop = env.model.flatprops[arg.name]
    return env.call('select', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, str)
def select(env, arg):
    # XXX: Backwards compatible resolver, `str` arguments are deprecated.
    prop = env.model.flatprops[arg]
    return env.call('select', prop.dtype)


@ufunc.resolver(SqlQueryBuilder, DataType)
def select(env, dtype):
    table = env.backend.get_table(env.model.external.name)
    name = dtype.prop.external.name
    return table.c[name]


@ufunc.resolver(SqlQueryBuilder, sqlalchemy.sql.functions.Function)
def select(env, func):
    return func


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
    name = dtype.prop.external.name
    return sa.func.length(env.table.c[name])


@ufunc.resolver(SqlQueryBuilder, Expr)
def sort(env, expr):
    args, kwargs = expr.resolve(env)
    env.sort = []
    for key in args:
        col = env.model.properties[key.name].external.name
        if isinstance(key, Negative):
            field = env.table.c[col].desc()
        else:
            field = env.table.c[col].asc()
        env.sort.append(field)


@ufunc.resolver(SqlQueryBuilder, int)
def limit(env, n):
    env.limit = n


@ufunc.resolver(SqlQueryBuilder, int)
def offset(env, n):
    env.offset = n
