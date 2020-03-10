from typing import List

from spinta import commands
from spinta.backends.postgresql import PostgreSQL
from spinta.components import Context, Model
from spinta import spyna
from spinta.migrations import (
    get_new_schema_version,
    get_schema_changes,
)


METACOLS = {
    '_id': {'type': 'pk'},
    '_revision': {'type': 'string'},
}


@commands.freeze.register()
def freeze(
    context: Context,
    backend: PostgreSQL,
    model: Model,
    *,
    versions: List[dict],
):
    old, new = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    if changes:
        migrate = autogen_migration(old, new)
        parents = get_parents(versions, new, context)
        version = get_new_schema_version(old, changes, migrate, parents)
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
            spyna.unparse({
                'name': 'create_table',
                'args': [new['name']] + [
                    {
                        'name': 'column',
                        'args': [
                            name,
                            {
                                'name': prop['type'],
                                'args': [],
                            },
                        ]
                    }
                    for name, prop in columns.items()
                ]
            }, pretty=True),
        ],
        'downgrade': [
            spyna.unparse({
                'name': 'drop_table',
                'args': [new['name']],
            }, pretty=True),
        ],
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
