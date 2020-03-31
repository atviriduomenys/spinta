import sqlalchemy as sa

from spinta.core.ufuncs import Env, Expr, Bind, ufunc


class SqlQueryBuilder(Env):

    def init(self, table: sa.Table):
        return self(
            table=table,
            select=[table],
            joins=table,
            sort=[],
            limit=None,
            offset=None,
        )

    def build(self, where):
        qry = sa.select(self.select)
        qry = qry.select_from(self.joins)

        if where:
            qry = qry.where(where)

        if self.sort:
            qry = qry.order_by(*self.sort)

        if self.limit is not None:
            qry = qry.limit(self.limit)

        if self.offset is not None:
            qry = qry.offset(self.offset)

        return qry


@ufunc.resolver(SqlQueryBuilder, Bind, str)
def eq(env, field, value):
    return env.table.c[field.name] == value


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
