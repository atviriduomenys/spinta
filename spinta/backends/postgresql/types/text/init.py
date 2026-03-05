import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers.type import validate_type_assignment
from spinta.backends.postgresql.helpers.name import get_pg_column_name
from spinta.components import Context
from spinta.types.text.components import Text


@commands.prepare.register(Context, PostgreSQL, Text)
def prepare(context: Context, backend: PostgreSQL, dtype: Text, **kwargs):
    validate_type_assignment(context, backend, dtype)
    prop = dtype.prop
    name = get_column_name(prop)

    return sa.Column(
        get_pg_column_name(name),
        JSONB,
        unique=dtype.unique,
        nullable=False,
        default=sa.text("'{}'"),
        server_default=sa.text("'{}'"),
        comment=name,
    )
