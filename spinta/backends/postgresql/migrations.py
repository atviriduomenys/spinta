from typing import List

from spinta import commands
from spinta.backends.postgresql import PostgreSQL
from spinta.components import Context, Model
from spinta.migrations import (
    get_schema_from_changes,
    get_schema_changes,
    get_new_schema_version
)


METACOLS = {
    '_id': {'type': 'pk'},
    '_revision': {'type': 'string'},
}


@commands.new_schema_version.register()
def new_schema_version(
    context: Context,
    backend: PostgreSQL,
    model: Model,
    *,
    versions: List[dict],
):
    old, new, nextvnum = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    if changes:
        migrate = autogen_migration(old, new),
        version = get_new_schema_version(old, changes, migrate, nextvnum)
        return version
    else:
        return {}


def autogen_migration(old: dict, new: dict) -> dict:
    if old:
        table = new['name']
        old = {
            **METACOLS,
            **old.get('properties', {}),
        }
        new = {
            **METACOLS,
            **new.get('properties', {}),
        }
        migration = {
            'upgrade': [],
            'downgrade': [],
        }
        _gen_add_columns(migration, table, old, new)
        _gen_alter_columns(migration, table, old, new)
        if migration['upgrade'] or migration['downgrade']:
            return migration
        else:
            return {}
    else:
        return _gen_create_table(new)


def _gen_create_table(new: dict) -> dict:
    columns = {
        **METACOLS,
        **new.get('properties', {}),
    }

    # create table
    return {
        'upgrade': [
            {
                'create_table': {
                    'name': new['name'],
                    'columns': [
                        {
                            'name': name,
                            'type': prop['type'],
                        }
                        for name, prop in columns.items()
                    ]
                }
            }
        ],
        'downgrade': [
            {
                'drop_table': {
                    'name': new['name'],
                }
            }
        ]
    }


def _gen_add_columns(migration: dict, table: str, old: dict, new: dict) -> None:
    new_col_names = set(new) - set(old)
    for name in new_col_names:
        migration['upgrade'].append({
            'add_column': {
                'table': table,
                'name': name,
                'type': new[name]['type'],
            }
        })
        migration['downgrade'].append({
            'drop_column': {
                'table': table,
                'name': name,
            }
        })


def _gen_alter_columns(migration: dict, table: str, old: dict, new: dict) -> None:
    same_col_names = set(new) & set(old)
    for name in same_col_names:
        upgrade = {}
        downgrade = {}

        if old[name]['type'] != new[name]['type']:
            upgrade['type'] = new[name]['type']
            downgrade['type'] = old[name]['type']

        if upgrade:
            migration['upgrade'].append({
                'alter_column': {
                    'table': table,
                    'name': name,
                    **upgrade,
                },
            })
        if downgrade:
            migration['downgrade'].append({
                'alter_column': {
                    'table': table,
                    'name': name,
                    **downgrade
                },
            })
