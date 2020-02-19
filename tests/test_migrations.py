import uuid

import pytest

from spinta.backends.postgresql.migrations import (
    autogen_migration,
)
from spinta.exceptions import MultipleParentsError
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
    old, new, parents = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    migrate = autogen_migration(old, new)
    version = get_new_schema_version(old, changes, migrate, parents)
    uuid_str = version['version']['id']
    assert uuid_str == str(uuid.UUID(uuid_str))
    assert version == {
        'version': {
            'date': version['version']['date'],
            'id': uuid_str,
            'parents': [],
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

    old, new, parents = get_schema_from_changes(versions + [version])
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
                'id': '67ce825c-2780-4b75-aa87-189d5c37ef6b',
                'parents': [],
            },
        }
    ]
    old, new, parents = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    migrate = autogen_migration(old, new)
    version = get_new_schema_version(old, changes, migrate, parents)
    assert version == {
        'version': {
            'date': version['version']['date'],
            'id': version['version']['id'],
            'parents': ['67ce825c-2780-4b75-aa87-189d5c37ef6b'],
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

    old, new, parents = get_schema_from_changes(versions + [version])
    changes = get_schema_changes(old, new)
    assert changes == []


def test_schema_with_multiple_head_nodes():
    # test schema creator when resource has a diverging scheme version history
    # i.e. 1st (root) schema migration has 2 diverging branches
    #
    # have model with 3 schema versions, with 2nd and 3rd diverging from 1st:
    # 1st version: create table with title: string and area: integer
    # 2nd version (parent 1st): add column - code: string
    # 3rd version (parent 1st): alter column - area: number
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
            'version': {
                'date': None,
                'id': '67ce825c-2780-4b75-aa87-189d5c37ef6b',
                'parents': [],
            },
            'changes': [
                {'op': 'add', 'path': '/name', 'value': 'country'},
                {'op': 'add', 'path': '/properties', 'value': {
                    'title': {'type': 'string'},
                    'area': {'type': 'integer'},
                }},
                {'op': 'add', 'path': '/type', 'value': 'model'},
            ],
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
        },
        {
            'version': {
                'date': None,
                'id': '6530b2ba-552d-407f-9210-81dfcc4102e6',
                'parents': ['67ce825c-2780-4b75-aa87-189d5c37ef6b'],
            },
            'changes': [
                {'op': 'add', 'path': '/properties/code', 'value': {'type': 'string'}},
            ],
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'add_column': {
                                'name': 'code',
                                'table': 'country',
                                'type': 'string',
                            },
                        },
                    ],
                    'downgrade': [
                        {'drop_column': {'name': 'code', 'table': 'country'}},
                    ],
                },
            },
        },
        {
            'version': {
                'date': None,
                'id': 'a6aadbeb-47ea-4d5d-b992-e74ebc9792cb',
                'parents': ['67ce825c-2780-4b75-aa87-189d5c37ef6b'],
            },
            'changes': [
                {'op': 'replace', 'path': '/properties/area/type', 'value': 'number'},
            ],
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'alter_column': {
                                'name': 'area',
                                'table': 'country',
                                'type': 'number',
                            },
                        },
                    ],
                    'downgrade': [
                        {
                            'alter_column': {
                                'name': 'area',
                                'table': 'country',
                                'type': 'integer',
                            },
                        },
                    ],
                },
            },
        },
    ]

    with pytest.raises(MultipleParentsError):
        get_schema_from_changes(versions)
