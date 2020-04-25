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

        if where is not None:
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
    name = env.model.properties[field.name].external.name
    return env.table.c[name] == value


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
