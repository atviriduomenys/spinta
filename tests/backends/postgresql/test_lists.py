import pathlib

from spinta.commands import load
from spinta.components import Model
from spinta.backends.postgresql import _get_lists_data


def create_model(context, schema):
    manifest = context.get('store').manifests['default']
    backend = context.get('store').backends['default']
    model = {
        'type': 'model',
        'name': 'model',
        'path': pathlib.Path('model.yml'),
        'parent': manifest,
        'backend': backend,
        **schema,
    }
    return load(context, Model(), model, manifest)


def _get_lists_data_with_key(model, patch):
    items = _get_lists_data(model, patch)
    return [(prop.list.place, patch) for prop, patch in items]


def test_get_list_data_scalar(context):
    model = create_model(context, {
        'properties': {
            'scalar': {'type': 'string'},
        },
    })
    patch = {
        'scalar': 'ok',
    }
    assert _get_lists_data_with_key(model, patch) == []


def test_get_list_data_list_of_scalars(context):
    model = create_model(context, {
        'properties': {
            'scalars': {
                'type': 'array',
                'items': {'type': 'integer'},
            },
        },
    })
    patch = {
        'scalars': [1, 2, 3]
    }
    assert _get_lists_data_with_key(model, patch) == [
        ('scalars', {'scalars': 1}),
        ('scalars', {'scalars': 2}),
        ('scalars', {'scalars': 3}),
    ]


def test_get_list_data_list_of_dicts(context):
    model = create_model(context, {
        'properties': {
            'dicts': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'integer'},
                        'bar': {'type': 'integer'},
                    },
                },
            },
        },
    })
    patch = {
        'dicts': [
            {'foo': 1, 'bar': 2},
            {'foo': 3, 'bar': 4},
        ]
    }
    assert _get_lists_data_with_key(model, patch) == [
        ('dicts', {'dicts.foo': 1, 'dicts.bar': 2}),
        ('dicts', {'dicts.foo': 3, 'dicts.bar': 4}),
    ]


def test_get_list_data_list_of_lists_of_scalars(context):
    model = create_model(context, {
        'properties': {
            'lists': {
                'type': 'array',
                'items': {
                    'type': 'array',
                    'items': {'type': 'integer'},
                },
            },
        },
    })
    patch = {
        'lists': [
            [1, 2],
            [3, 4],
        ]
    }
    assert _get_lists_data_with_key(model, patch) == [
        ('lists', {'lists': 1}),
        ('lists', {'lists': 2}),
        ('lists', {'lists': 3}),
        ('lists', {'lists': 4}),
    ]


def test_get_list_data_mixed_nested_lists(context):
    model = create_model(context, {
        'properties': {
            'lists': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'integer'},
                        'lists': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'properties': {
                                    'bar': {'type': 'integer'},
                                    'scalars': {
                                        'type': 'array',
                                        'items': {'type': 'integer'},
                                    }
                                }
                            }
                        }
                    }
                },
            },
        },
    })
    patch = {
        'lists': [
            {
                'foo': 1,
                'lists': [
                    {
                        'bar': 1,
                        'scalars': [1, 2],
                    },
                    {
                        'bar': 2,
                        'scalars': [1, 2],
                    },
                ]
            },
            {
                'foo': 2,
                'lists': [
                    {
                        'bar': 1,
                        'scalars': [1, 2],
                    }
                ]
            },
        ]
    }
    assert _get_lists_data_with_key(model, patch) == [
        ('lists', {'lists.foo': 1}),
        ('lists.lists', {'lists.lists.bar': 1}),
        ('lists.lists.scalars', {'lists.lists.scalars': 1}),
        ('lists.lists.scalars', {'lists.lists.scalars': 2}),
        ('lists.lists', {'lists.lists.bar': 2}),
        ('lists.lists.scalars', {'lists.lists.scalars': 1}),
        ('lists.lists.scalars', {'lists.lists.scalars': 2}),
        ('lists', {'lists.foo': 2}),
        ('lists.lists', {'lists.lists.bar': 1}),
        ('lists.lists.scalars', {'lists.lists.scalars': 1}),
        ('lists.lists.scalars', {'lists.lists.scalars': 2}),
    ]


def test_get_list_data_nested_dicts_with_lists(context):
    model = create_model(context, {
        'properties': {
            'dict': {
                'type': 'object',
                'properties': {
                    'list': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'foo': {
                                    'type': 'object',
                                    'properties': {
                                        'bar': {'type': 'integer'},
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    })
    patch = {
        'dict': {
            'list': [
                {'foo': {'bar': 1}},
                {'foo': {'bar': 2}},
            ]
        }
    }
    assert _get_lists_data_with_key(model, patch) == [
        ('dict.list', {'dict.list.foo.bar': 1}),
        ('dict.list', {'dict.list.foo.bar': 2}),
    ]


def test_get_list_data_mixed(context):
    model = create_model(context, {
        'properties': {
            'scalar': {'type': 'string'},
            'ints': {
                'type': 'array',
                'items': {'type': 'integer'},
            }
        },
    })
    patch = {
        'scalar': 1,
        'ints': [1, 2],
    }
    assert _get_lists_data_with_key(model, patch) == [
        ('ints', {'ints': 1}),
        ('ints', {'ints': 2}),
    ]
