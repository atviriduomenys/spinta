import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import JSONB

from spinta import commands
from spinta.backends.postgresql.helpers.name import get_pg_column_name
from spinta.backends.postgresql.helpers.type import validate_type_assignment
from spinta.components import Context
from spinta.types.datatype import Array
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.components import PostgreSQL


@commands.prepare.register(Context, PostgreSQL, Array)
def prepare(context: Context, backend: PostgreSQL, dtype: Array, **kwargs):
    validate_type_assignment(context, backend, dtype)

    prop = dtype.prop

    columns = commands.prepare(context, backend, dtype.items, **kwargs)

    assert columns is not None
    if not isinstance(columns, list):
        columns = [columns]
    pkey_type = commands.get_primary_key_type(context, backend)
    # TODO: When all list items will have unique id, also add reference to
    #       parent list id.
    # if prop.list:
    #     parent_list_table_name = get_pg_name(
    #         get_table_name(prop.list, TableType.LIST)
    #     )
    #     columns += [
    #         # Parent list table id.
    #         sa.Column('_lid', pkey_type, sa.ForeignKey(
    #             f'{parent_list_table_name}._id', ondelete='CASCADE',
    #         )),
    #     ]

    list_table_identifier = get_table_identifier(prop, TableType.LIST)
    main_table_identifier = get_table_identifier(prop.model)
    if list_table_identifier.logical_qualified_name not in backend.tables:
        table = sa.Table(
            list_table_identifier.pg_table_name,
            backend.schema,
            # TODO: List tables eventually will have _id in order to uniquelly
            #       identify list item.
            # sa.Column('_id', pkey_type, primary_key=True),
            sa.Column(get_pg_column_name("_txn"), pkey_type, index=True, comment="_txn"),
            sa.Column(
                get_pg_column_name("_rid"),
                pkey_type,
                sa.ForeignKey(
                    f"{main_table_identifier.pg_qualified_name}._id",
                    ondelete="CASCADE",
                ),
                comment="_rid",
            ),
            *columns,
            schema=list_table_identifier.pg_schema_name,
            comment=list_table_identifier.logical_qualified_name,
        )
        backend.add_table(table, prop, TableType.LIST)

    if prop.list is None:
        # For fast whole resource access we also store whole list in a JSONB.
        return sa.Column(get_pg_column_name(prop.place), JSONB, comment=prop.place)
    return None
