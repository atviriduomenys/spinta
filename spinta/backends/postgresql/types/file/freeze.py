from spinta import commands
from spinta.backends.components import BackendFeatures
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.helpers import get_pg_name, get_table_name
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
        list_table_name = get_pg_name(get_table_name(current.prop, TableType.FILE))
        list_table_columns = [
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
                'args': [list_table_name] + list_table_columns,
            },
            'downgrade': {
                'name': 'drop_table',
                'args': [list_table_name],
            },
        })
    pr = [
        {
            'name': 'column',
            'args': [
                f'{current.prop.place}._id',
                {
                    'name': 'string',
                    'args': [],
                },
            ]
        },
        {
            'name': 'column',
            'args': [
                f'{current.prop.place}._content_type',
                {
                    'name': 'string',
                    'args': [],
                },
            ]
        },
        {
            'name': 'column',
            'args': [
                f'{current.prop.place}._size',
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
                        f'{current.prop.place}._bsize',
                        {
                            'name': 'integer',
                            'args': [],
                        },
                    ]
                },
                {
                    'name': 'column',
                    'args': [
                        f'{current.prop.place}._blocks',
                        {
                            'name': 'array',
                            'args': [{'name': 'uuid', 'args': []}],
                        },
                    ]
                }
            ]
        )
    return pr
