from spinta.backends.postgresql.migrations import autogen_migration


def test_create_table():
    old = {
    }
    new = {
        'type': 'model',
        'name': 'country',
        'properties': {
            'title': {'type': 'string'},
        }
    }
    assert autogen_migration(old, new) == {
        'upgrade': [
            {
                'create_table': {
                    'name': 'country',
                    'columns': [
                        {'name': '_id', 'type': 'pk'},
                        {'name': '_revision', 'type': 'string'},
                        {'name': 'title', 'type': 'string'},
                    ]
                }
            }
        ],
        'downgrade': [
            {
                'drop_table': {
                    'name': 'country',
                }
            }
        ]
    }


def test_add_column():
    old = {
        'type': 'model',
        'name': 'country',
        'properties': {
            'title': {'type': 'string'},
        }
    }
    new = {
        'type': 'model',
        'name': 'country',
        'properties': {
            'title': {'type': 'string'},
            'code': {'type': 'string'},
        }
    }
    assert autogen_migration(old, new) == {
        'upgrade': [
            {
                'add_column': {
                    'table': 'country',
                    'name': 'code',
                    'type': 'string',
                }
            }
        ],
        'downgrade': [
            {
                'drop_column': {
                    'table': 'country',
                    'name': 'code',
                }
            }
        ]
    }


def test_alter_column():
    old = {
        'type': 'model',
        'name': 'country',
        'properties': {
            'title': {'type': 'string'},
            'area': {'type': 'integer'},
        }
    }
    new = {
        'type': 'model',
        'name': 'country',
        'properties': {
            'title': {'type': 'string'},
            'area': {'type': 'number'},
        }
    }
    assert autogen_migration(old, new) == {
        'upgrade': [
            {
                'alter_column': {
                    'table': 'country',
                    'name': 'area',
                    'type': 'number',
                }
            }
        ],
        'downgrade': [
            {
                'alter_column': {
                    'table': 'country',
                    'name': 'area',
                    'type': 'integer',
                }
            }
        ]
    }
