import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.components import Context
from spinta.types.money.components import Money


@commands.prepare.register(Context, PostgreSQL, Money)
def prepare(
    context: Context,
    backend: PostgreSQL,
    dtype: Money,
):
    prop = dtype.prop
    name = get_column_name(prop)

    columns = [
        sa.Column(f'{name}._amount', sa.Integer),
        sa.Column(f'{name}._currency', sa.String),
    ]
    return columns
