from typing import List, Union

import sqlalchemy as sa

from sqlalchemy.sql.type_api import TypeEngine

from spinta import commands
from spinta.components import Context, Property
from spinta.types.datatype import Ref
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers import get_column_name


@commands.prepare.register(Context, PostgreSQL, Ref)
def prepare(context: Context, backend: PostgreSQL, dtype: Ref):
    pkey_type = commands.get_primary_key_type(context, backend)
    return get_pg_foreign_key(
        dtype.prop,
        table_name=get_pg_name(get_table_name(dtype.model)),
        model_name=dtype.prop.model.name,
        column_type=pkey_type,
    )


def get_pg_foreign_key(
    prop: Property,
    *,
    table_name: str,
    model_name: str,
    column_type: TypeEngine,
) -> List[Union[sa.Column, sa.Constraint]]:
    column_name = get_column_name(prop) + '._id'
    return [
        sa.Column(column_name, column_type),
        sa.ForeignKeyConstraint(
            [column_name], [f'{table_name}._id'],
            name=get_pg_name(f'fk_{model_name}_{column_name}'),
        )
    ]
