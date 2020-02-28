import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql.type_api import TypeEngine

from spinta.core.ufuncs import Env, Expr, Bind, ufunc


class Alembic(Env):
    pass


@ufunc.resolver(Alembic, Expr)
def create_table(env, expr):
    args, kwargs = expr.resolve(env)
    table, *args = args
    if isinstance(table, Bind):
        table = table.name
    return Expr(expr.name, table, *args, **kwargs)


@ufunc.executor(Alembic, Expr)
def create_table(env, expr):  # noqa
    env.op.create_table(*expr.args, **expr.kwargs)


@ufunc.resolver(Alembic, Bind)
def drop_table(env, table):
    return table.name


@ufunc.executor(Alembic, Expr)
def drop_table(env, expr):  # noqa
    env.op.drop_table(*expr.args, **expr.kwargs)


@ufunc.resolver(Alembic, str, TypeEngine)
def column(env, name, dtype, **kwargs):
    return sa.Column(name, dtype, **kwargs)


@ufunc.resolver(Alembic, Bind, TypeEngine)
def column(env, name, dtype, **kwargs):  # noqa
    return sa.Column(name.name, dtype, **kwargs)


@ufunc.resolver(Alembic)
def uuid(env):  # noqa
    return UUID()


@ufunc.resolver(Alembic)
def string(env):  # noqa
    return sa.String()


@ufunc.resolver(Alembic, int)
def string(env, length):  # noqa
    return sa.String(length)
