import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import JSONB

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name


@commands.prepare.register(Context, PostgreSQL, Array)
def prepare(context: Context, backend: PostgreSQL, dtype: Array):
    prop = dtype.prop

    columns = commands.prepare(context, backend, dtype.items)
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

    name = get_pg_name(get_table_name(prop, TableType.LIST))
    main_table_name = get_pg_name(get_table_name(prop.model))
    table = sa.Table(
        name, backend.schema,
        # TODO: List tables eventually will have _id in order to uniquelly
        #       identify list item.
        # sa.Column('_id', pkey_type, primary_key=True),
        sa.Column('_txn', pkey_type, index=True),
        # Main table id (resource id).
        sa.Column('_rid', pkey_type, sa.ForeignKey(
            f'{main_table_name}._id', ondelete='CASCADE',
        )),
        *columns,
    )
    backend.add_table(table, prop, TableType.LIST)

    if prop.list is None:
        # For fast whole resource access we also store whole list in a JSONB.
        return sa.Column(prop.place, JSONB)
