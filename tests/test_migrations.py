from spinta.backends.postgresql.migrations import (
    autogen_migration,
)
from spinta.migrations import (
    get_schema_from_changes,
    get_schema_changes,
    get_new_schema_version
)


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


def test_new_schema_version():
    versions = [
        {
            'type': 'model',
            'name': 'country',
            'properties': {
                'title': {'type': 'string'},
                'area': {'type': 'number'},
            }
        }
    ]
    old, new, nextvnum = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    migrate = autogen_migration(old, new)
    version = get_new_schema_version(old, changes, migrate, nextvnum)
    assert version == {
        'version': {
            'date': version['version']['date'],
            'number': 1,
        },
        'changes': version['changes'],
        'migrate': {
            'schema': {
                'upgrade': [
                    {
                        'create_table': {
                            'name': 'country',
                            'columns': [
                                {'name': '_id', 'type': 'pk'},
                                {'name': '_revision', 'type': 'string'},
                                {'name': 'title', 'type': 'string'},
                                {'name': 'area', 'type': 'number'},
                            ],
                        },
                    },
                ],
                'downgrade': [
                    {'drop_table': {'name': 'country'}},
                ],
            },
        },
    }
    assert sorted(version['changes'], key=lambda x: x['path']) == [
        {'op': 'add', 'path': '/name', 'value': 'country'},
        {'op': 'add', 'path': '/properties', 'value': {
            'title': {'type': 'string'},
            'area': {'type': 'number'},
        }},
        {'op': 'add', 'path': '/type', 'value': 'model'},
    ]

    old, new, nextvnum = get_schema_from_changes(versions + [version])
    changes = get_schema_changes(old, new)
    assert changes == []


def test_new_second_schema_version():
    versions = [
        {
            'type': 'model',
            'name': 'country',
            'properties': {
                'title': {'type': 'string'},
                'area': {'type': 'number'},
                'code': {'type': 'string'},
            }
        },
        {
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'create_table': {
                                'name': 'country',
                                'columns': [
                                    {'name': '_id', 'type': 'pk'},
                                    {'name': '_revision', 'type': 'string'},
                                    {'name': 'title', 'type': 'string'},
                                    {'name': 'area', 'type': 'integer'},
                                ],
                            },
                        },
                    ],
                    'downgrade': [
                        {'drop_table': {'name': 'country'}},
                    ],
                },
            },
            'changes': [
                {'op': 'add', 'path': '/name', 'value': 'country'},
                {'op': 'add', 'path': '/properties', 'value': {
                    'title': {'type': 'string'},
                    'area': {'type': 'integer'},
                }},
                {'op': 'add', 'path': '/type', 'value': 'model'},
            ],
            'version': {
                'date': None,
                'number': 1,
            },
        }
    ]
    old, new, nextvnum = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    migrate = autogen_migration(old, new)
    version = get_new_schema_version(old, changes, migrate, nextvnum)
    assert version == {
        'version': {
            'date': version['version']['date'],
            'number': 2,
        },
        'changes': [
            {'op': 'add', 'path': '/properties/code', 'value': {'type': 'string'}},
            {'op': 'replace', 'path': '/properties/area/type', 'value': 'number'},
        ],
        'migrate': {
            'schema': {
                'downgrade': [
                    {'drop_column': {'name': 'code', 'table': 'country'}},
                    {
                        'alter_column': {
                            'name': 'area',
                            'table': 'country',
                            'type': 'integer',
                        },
                    },
                ],
                'upgrade': [
                    {
                        'add_column': {
                            'name': 'code',
                            'table': 'country',
                            'type': 'string',
                        },
                    },
                    {
                        'alter_column': {
                            'name': 'area',
                            'table': 'country',
                            'type': 'number',
                        },
                    },
                ],
            },
        },
    }

    old, new, nextvnum = get_schema_from_changes(versions + [version])
    changes = get_schema_changes(old, new)
    assert changes == []
