import sqlalchemy as sa
from spinta import commands
from spinta.components import Context
from spinta.types.datatype import BackRef
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name


@commands.prepare.register(Context, PostgreSQL, BackRef)
def prepare(context: Context, backend: PostgreSQL, dtype: BackRef):
    def get_columns_for_many_to_many_table(_name):
        return _name.split("/:list/")

    name = get_pg_name(get_table_name(dtype.prop, TableType.LIST))
    main_table_name = get_pg_name(get_table_name(dtype.prop.model))
    pkey_type = commands.get_primary_key_type(context, backend)
    temp_columns = get_columns_for_many_to_many_table(name)
    table_for_many_to_many_relation = sa.Table(
        name, backend.schema,
        sa.Column('_txn', pkey_type, index=True),
        sa.Column(temp_columns[0]+"._id", pkey_type, sa.ForeignKey(
            f'{main_table_name}._id', ondelete='CASCADE',
        )),
        sa.Column(temp_columns[1]+"._id", pkey_type, sa.ForeignKey(
            f'{dtype.model}._id', ondelete='CASCADE'
        ))
    )

    backend.tables[name] = table_for_many_to_many_relation
