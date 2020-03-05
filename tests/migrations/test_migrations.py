import uuid

import pytest

from spinta.backends.postgresql.migrations import (
    autogen_migration,
)
from spinta.exceptions import MultipleParentsError
from spinta.migrations import (
    build_schema_relation_graph,
    get_new_schema_version,
    get_parents,
    get_ref_model_names,
    get_schema_changes,
    get_schema_from_changes,
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


def test_new_schema_version(context):
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
    old, new = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    migrate = autogen_migration(old, new)
    parents = get_parents(versions, new, context)
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

    old, new = get_schema_from_changes(versions + [version])
    changes = get_schema_changes(old, new)
    assert changes == []


def test_new_second_schema_version(context):
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
    old, new = get_schema_from_changes(versions)
    changes = get_schema_changes(old, new)
    migrate = autogen_migration(old, new)
    parents = get_parents(versions, new, context)
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

    old, new = get_schema_from_changes(versions + [version])
    changes = get_schema_changes(old, new)
    assert changes == []


def test_schema_with_multiple_head_nodes(context):
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

    old, new = get_schema_from_changes(versions)
    with pytest.raises(MultipleParentsError):
        get_parents(versions, new, context)


def test_build_schema_relation_graph(config, tmpdir):
    # test new schema version, when multiple models have no foreign keys
    org_model = {
        'type': 'model',
        'name': 'org',
        'version': {
            'id': '365b3209-c00f-4357-9749-5f680d337834',
            'date': '2020-03-14 15:26:53'
        },
        'properties': {
            'title': {'type': 'string'},
            'country': {'type': 'ref', 'object': 'country'},
        }
    }
    country_model = {
        'type': 'model',
        'name': 'country',
        'version': {
            'id': '0cffc369-308a-4093-8a08-92dbddb64a56',
            'date': '2020-03-14 15:26:53'
        },
        'properties': {
            'title': {'type': 'string'},
        },
    }
    report_model = {
        'type': 'model',
        'name': 'report',
        'version': {
            'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
            'date': '2020-03-14 15:26:53'
        },
        'properties': {
            'title': {'type': 'string'},
        },
    }

    model_yaml_data = {
        'org': [org_model],
        'country': [country_model],
        'report': [report_model]
    }
    model_graph = build_schema_relation_graph(model_yaml_data)
    assert model_graph == {'org': {'country'}, 'country': set(), 'report': set()}


def test_get_ref_model_names_prop():
    # tests that all ref model names are found from first level properties
    props = {
        'scalar': {'type': 'string'},
        'ref_a': {'type': 'ref', 'object': 'model_a'},
        'ref_b': {'type': 'ref', 'object': 'model_b'},
    }
    assert get_ref_model_names(props) == ['model_a', 'model_b']


def test_get_ref_model_names_array():
    # tests that all ref model names are found from arrays
    props = {
        'scalar': {'type': 'string'},
        'obj': {
            'type': 'object',
            'properties': {
                'ref_a': {'type': 'ref', 'object': 'model_a'},
                'foo': {'type': 'integer'},
            }
        },
    }
    assert get_ref_model_names(props) == ['model_a']


def test_get_ref_model_names_obj():
    # tests that all ref model names are found from objects
    props = {
        'scalar': {'type': 'string'},
        'arr': {
            'type': 'array',
            'items': {'type': 'ref', 'object': 'model_a'},
        },
    }
    assert get_ref_model_names(props) == ['model_a']


def test_get_ref_model_names_complex():
    # tests that all ref model names are found from from complex property dict
    props = {
        'scalar': {'type': 'string'},
        'ref_a': {'type': 'ref', 'object': 'model_a'},
        'ref_b': {'type': 'ref', 'object': 'model_b'},
        'arr': {
            'type': 'array',
            'items': {'type': 'ref', 'object': 'model_arr'},
        },
        'obj': {
            'type': 'object',
            'properties': {
                'ref_a': {'type': 'ref', 'object': 'model_obj'},
                'foo': {'type': 'integer'},
            }
        }
    }
    assert get_ref_model_names(props) == ['model_a', 'model_b', 'model_arr', 'model_obj']
