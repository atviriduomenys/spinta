import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import ARRAY

from spinta.exceptions import UnknownExpr
from spinta.core.ufuncs import Env, Expr, Bind, ufunc


TYPES = {
    'array': ARRAY,
    'string': sa.Text,
    'integer': sa.Integer,
    'number': sa.Float,
    'boolean': sa.Boolean,
    'binary': sa.LargeBinary,
    'date': sa.Date,
    'datetime': sa.DateTime,
    'json': JSONB,
    'uuid': UUID,
}


class Alembic(Env):

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownExpr(expr=str(expr(*args, **kwargs)), name=expr.name)


@ufunc.resolver(Alembic, Expr)
def alter_column(env, expr):
    args, kwargs = expr.resolve(env)
    table, name, *args = args
    if isinstance(table, Bind):
        table = table.name
    if 'type_' in kwargs and 'postgresql_using' not in kwargs:
        kwargs['postgresql_using'] = f'{name}::{kwargs["type_"]}'
    return Expr(expr.name, table, name, *args, **kwargs)


@ufunc.executor(Alembic, Expr)
def alter_column(env, expr):
    env.op.alter_column(*expr.args, **expr.kwargs)


@ufunc.resolver(Alembic, Expr)
def create_table(env, expr):
    args, kwargs = expr.resolve(env)
    table, *args = args
    if isinstance(table, Bind):
        table = table.name
    return Expr(expr.name, table, *args, **kwargs)


@ufunc.executor(Alembic, Expr)
def create_table(env, expr):
    env.op.create_table(*expr.args, **expr.kwargs)


@ufunc.resolver(Alembic, str)
def drop_table(env, table):
    return Expr('drop_table', table)


@ufunc.resolver(Alembic, Bind)
def drop_table(env, table):
    return Expr('drop_table', table.name)


@ufunc.executor(Alembic, Expr)
def drop_table(env, expr):
    env.op.drop_table(*expr.args, **expr.kwargs)


@ufunc.resolver(Alembic, Expr)
def column(env, expr):
    args, kwargs = expr.resolve(env)
    name, type_, *args = args
    if isinstance(name, Bind):
        name = name.name
    return sa.Column(name, type_, *args, **kwargs)


@ufunc.resolver(Alembic, Expr, names=list(TYPES))
def type_(env, expr):
    args, kwargs = expr.resolve(env)
    Type = TYPES[expr.name]
    return Type(*args, **kwargs)


@ufunc.resolver(Alembic, str)
def ref(env, reference, **kwargs):
    return sa.ForeignKey(reference, **kwargs)
