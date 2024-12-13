from typing import List, Union

import sqlalchemy as sa
from sqlalchemy.sql.type_api import TypeEngine

from spinta import commands
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers.name import get_pg_column_name, get_pg_table_name
from spinta.components import Context, Property
from spinta.types.datatype import Ref


@commands.prepare.register(Context, PostgreSQL, Ref)
def prepare(context: Context, backend: PostgreSQL, dtype: Ref, propagate: bool = True, **kwargs):
    pkey_type = commands.get_primary_key_type(context, backend)
    columns = []

    if not dtype.inherited:
        columns = get_pg_foreign_key(
            dtype.prop,
            table_name=get_pg_table_name(get_table_name(dtype.model)),
            model_name=dtype.prop.model.name,
            column_type=pkey_type,
        )

    if not propagate:
        return columns

    for key, value in dtype.properties.items():
        res = commands.prepare(context, backend, value, **kwargs)
        if res is not None:
            if isinstance(res, list):
                columns.extend(res)
            else:
                columns.append(res)
    return columns


def get_pg_foreign_key(
    prop: Property,
    *,
    table_name: str,
    model_name: str,
    column_type: TypeEngine
) -> List[Union[sa.Column, sa.Constraint]]:
    column_name = get_pg_column_name(get_column_name(prop) + '._id')
    if prop.list and prop.place == prop.list.place:
        column_name = '_id'
    nullable = not prop.dtype.required

    columns = [
        sa.Column(
            column_name,
            column_type,
            nullable=nullable,
            index=not prop.dtype.unique,
            unique=prop.dtype.unique
        ),
        sa.ForeignKeyConstraint(
            [column_name], [f'{table_name}._id'],
            ondelete='CASCADE' if prop.list else None,
            onupdate='CASCADE' if prop.list else None
        )
    ]
    return columns
