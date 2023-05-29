import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import ExternalRef
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name


@commands.prepare.register(Context, PostgreSQL, ExternalRef)
def prepare(context: Context, backend: PostgreSQL, dtype: ExternalRef):
    if dtype.model.given.pkeys or dtype.explicit:
        columns = []
        for prop in dtype.refprops:
            original_place = prop.place
            prop.place = f"{dtype.prop.place}.{prop.place}"
            column = commands.prepare(
                context,
                backend,
                prop.dtype,
            )
            if isinstance(column, list):
                columns.extend(column)
            elif column is not None:
                columns.append(column)
            prop.place = original_place
        return columns

    else:
        column_name = get_column_name(dtype.prop) + '._id'
        column_type = commands.get_primary_key_type(context, backend)
        return sa.Column(
            column_name,
            column_type,
            nullable=not dtype.required,
            unique=dtype.unique,
        )
