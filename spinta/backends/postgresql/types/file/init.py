import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import BIGINT, ARRAY

from spinta import commands
from spinta.backends.postgresql.helpers.name import get_pg_column_name
from spinta.backends.postgresql.helpers.type import validate_type_assignment
from spinta.components import Context
from spinta.types.datatype import File
from spinta.backends.constants import TableType, BackendFeatures
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers import get_column_name


@commands.prepare.register(Context, PostgreSQL, File)
def prepare(context: Context, backend: PostgreSQL, dtype: File, **kwargs):
    validate_type_assignment(context, backend, dtype)
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
        table_name = get_table_name(prop, TableType.FILE)
        table = sa.Table(
            get_pg_name(table_name),
            model.backend.schema,
            sa.Column(get_pg_column_name("_id"), pkey_type, primary_key=True, comment="_id"),
            sa.Column(get_pg_column_name("_block"), sa.LargeBinary, comment="_block"),
            comment=table_name,
        )
        model.backend.add_table(table, prop, TableType.FILE)

    name = get_column_name(prop)
    nullable = not dtype.required

    # Required file metadata on any backend.
    columns = [
        sa.Column(get_pg_column_name(meta_id := f"{name}._id"), sa.String, nullable=nullable, comment=meta_id),
        sa.Column(
            get_pg_column_name(meta_content_type := f"{name}._content_type"),
            sa.String,
            nullable=nullable,
            comment=meta_content_type,
        ),
        sa.Column(get_pg_column_name(meta_size := f"{name}._size"), BIGINT, nullable=nullable, comment=meta_size),
    ]

    # Optional file metadata, depending on backend supported features.
    if same_backend or BackendFeatures.FILE_BLOCKS in dtype.backend.features:
        columns += [
            sa.Column(
                get_pg_column_name(meta_bsize := f"{name}._bsize"), sa.Integer, nullable=nullable, comment=meta_bsize
            ),
            sa.Column(
                get_pg_column_name(meta_blocks := f"{name}._blocks"),
                ARRAY(
                    pkey_type,
                ),
                nullable=nullable,
                comment=meta_blocks,
            ),
        ]

    return columns
