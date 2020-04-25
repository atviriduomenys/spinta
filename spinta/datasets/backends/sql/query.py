from __future__ import annotations

import sqlalchemy as sa

from spinta.core.ufuncs import Env, Expr, Bind, ufunc
from spinta.exceptions import UnknownExpr
from spinta.components import Property
from spinta.backends.components import Backend
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
        raise UnknownExpr(expr=str(expr(*args, **kwargs)))


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
    expr = Expr('getattr', prop.dtype, attr)
    return env.resolve(expr)


@ufunc.resolver(SqlQueryBuilder, Ref, Bind)
def getattr(env, dtype, attr):
    model = dtype.prop.model.manifest.models[dtype.object]
    prop = model.properties[attr.name]
    return ForeignProperty(None, dtype.prop, prop)


@ufunc.resolver(SqlQueryBuilder, Bind, str)
def eq(env, field, value):
    name = env.model.properties[field.name].external.name
    return env.table.c[name] == value


@ufunc.resolver(SqlQueryBuilder, ForeignProperty, str)
def eq(env, prop, value):
    for fpr in prop.chain:
        if fpr.name in env.joins:
            continue

        lmodel = fpr.left.model
        ltable = env.backend.schema.tables[lmodel.external.name]
        lrkeys = [fpr.left.external.name]

        rmodel = fpr.right.model
        rtable = env.backend.schema.tables[rmodel.external.name]
        rpkeys = [k.external.name for k in rmodel.external.pkeys]

        assert len(lrkeys) == len(rpkeys), (lrkeys, rpkeys)
        condition = []
        for lrk, rpk in zip(lrkeys, rpkeys):
            condition += [ltable.c[lrk] == rtable.c[rpk]]

        assert len(condition) > 0
        if len(condition) == 1:
            condition = condition[0]
        else:
            condition = sa.and_(condition)

        env.from_ = env.joins[fpr.name] = env.from_.join(rtable, condition)

    model = fpr.right.model
    table = env.backend.schema.tables[model.external.name]
    name = fpr.right.external.name
    return table.c[name] == value


@ufunc.resolver(SqlQueryBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    if len(args) > 1:
        return sa.and_(*args)
    elif args:
        return args[0]


@ufunc.resolver(SqlQueryBuilder, Expr)
def select(env, expr):
    args, kwargs = expr.resolve(env)
    if args:
        env.select = args
    else:
        env.select = [env.table]


@ufunc.resolver(SqlQueryBuilder, Expr)
def sort(env, expr):
    args, kwargs = expr.resolve(env)
    env.sort = []
    for key in args:
        col = env.model.properties[key].external.name
        field = env.table.c[col]
        env.sort.append(field)
