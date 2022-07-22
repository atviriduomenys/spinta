import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import BIGINT, ARRAY

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import File
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL, BackendFeatures
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers import get_column_name


@commands.prepare.register(Context, PostgreSQL, File)
def prepare(context: Context, backend: PostgreSQL, dtype: File):
    prop = dtype.prop
    model = prop.model

    # All properties receive model backend even if properties own backend
    # differs. Its properties responsibility to deal with foreign backends.
    assert model.backend.name == backend.name

    pkey_type = commands.get_primary_key_type(context, model.backend)

    same_backend = backend.name == dtype.backend.name

    if same_backend:
        # If dtype is on the same backend currently being prepared, then
        # create table for storing file blocks and also add file metadata
        # columns.
        table_name = get_pg_name(get_table_name(prop, TableType.FILE))
        table = sa.Table(
            table_name, model.backend.schema,
            sa.Column('_id', pkey_type, primary_key=True),
            sa.Column('_block', sa.LargeBinary),
        )
        model.backend.add_table(table, prop, TableType.FILE)

    name = get_column_name(prop)

    # Required file metadata on any backend.
    columns = [
        sa.Column(f'{name}._id', sa.String),
        sa.Column(f'{name}._content_type', sa.String),
        sa.Column(f'{name}._size', BIGINT),
    ]

    # Optional file metadata, depending on backend supported features.
    if same_backend or BackendFeatures.FILE_BLOCKS in dtype.backend.features:
        columns += [
            sa.Column(f'{name}._bsize', sa.Integer),
            sa.Column(f'{name}._blocks', ARRAY(pkey_type)),
        ]

    return columns
