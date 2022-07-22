from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.components import BackendFeatures
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers import get_column_name
from spinta.components import Context
from spinta.types.datatype import File
from spinta.migrations import SchemaVersion


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), File)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: File,
):
    if current.backend is backend:
        file_blocks_table_name = get_pg_name(get_table_name(current.prop, TableType.FILE))
        file_table_columns = [
            {
                'name': 'column',
                'args': ['_id', {'name': 'uuid', 'args': []}],
            },
            {
                'name': 'column',
                'args': ['_block', {'name': 'binary', 'args': []}],
            },
        ]
        version.actions.append({
            'type': 'schema',
            'upgrade': {
                'name': 'create_table',
                'args': [file_blocks_table_name] + file_table_columns,
            },
            'downgrade': {
                'name': 'drop_table',
                'args': [file_blocks_table_name],
            },
        })
    pr = [
        {
            'name': 'column',
            'args': [
                f'{get_column_name(current.prop)}._id',
                {
                    'name': 'string',
                    'args': [],
                },
            ]
        },
        {
            'name': 'column',
            'args': [
                f'{get_column_name(current.prop)}._content_type',
                {
                    'name': 'string',
                    'args': [],
                },
            ]
        },
        {
            'name': 'column',
            'args': [
                f'{get_column_name(current.prop)}._size',
                {
                    'name': 'integer',
                    'args': [],
                },
            ]
        }
    ]
    if BackendFeatures.FILE_BLOCKS in current.backend.features:
        pr.extend(
            [
                {
                    'name': 'column',
                    'args': [
                        f'{get_column_name(current.prop)}._bsize',
                        {
                            'name': 'integer',
                            'args': [],
                        },
                    ]
                },
                {
                    'name': 'column',
                    'args': [
                        f'{get_column_name(current.prop)}._blocks',
                        {
                            'name': 'array',
                            'args': [{'name': 'uuid', 'args': []}],
                        },
                    ]
                }
            ]
        )
    return pr
