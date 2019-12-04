import pathlib

from spinta.commands import load
from spinta.commands import build_data_patch_for_write as patch
from spinta.components import Model


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


def test_build_data_patch_for_write(context):
    model = create_model(context, {
        'properties': {
            'scalar': {'type': 'string'},
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                        'bar': {'type': 'string'},
                    },
                },
            },
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                    'bar': {'type': 'string'},
                    'sub': {
                        'type': 'object',
                        'properties': {
                            'foos': {'type': 'string'},
                            'bars': {'type': 'string'},
                        },
                    }
                },
            },
        },
    })
    given = {'obj': {'foo': '42'}}
    saved = {}
    assert patch(context, model, given=given, saved=saved) == {
        'obj': {
            'foo': '42',
        },
    }
    assert patch(context, model, given=given, saved=saved, fill=True) == {
        'scalar': None,
        'list': [],
        'obj': {
            'foo': '42',
            'bar': None,
            'sub': {
                'foos': None,
                'bars': None,
            },
        },
    }


def test_array_fill(context):
    model = create_model(context, {
        'properties': {
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                        'bar': {'type': 'string'},
                    },
                },
            },
        },
    })
    given = {'list': [{'foo': '42'}]}
    saved = {}
    assert patch(context, model, given=given, saved=saved, fill=True) == {
        'list': [
            {
                'bar': None,
                'foo': '42',
            },
        ]
    }


def test_array_fill_saved(context):
    model = create_model(context, {
        'properties': {
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                        'bar': {'type': 'string'},
                    },
                },
            },
        },
    })
    given = {'list': [{'foo': '42'}]}
    saved = {'list': [{'foo': '42'}]}
    assert patch(context, model, given=given, saved=saved, fill=True) == {
        'list': [
            {
                'bar': None,
                'foo': '42',
            },
        ]
    }


def test_object_saved(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                    'bar': {'type': 'string'},
                    'baz': {'type': 'string'},
                },
            },
        },
    })
    given = {'obj': {'foo': '42', 'bar': '24'}}
    saved = {'obj': {'foo': '42', 'bar': None, 'baz': None}}
    assert patch(context, model, given=given, saved=saved, fill=False) == {
        'obj': {
            'bar': '24',
        }
    }


def test_object_fill_saved(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                    'bar': {'type': 'string'},
                },
            },
        },
    })
    given = {'obj': {'foo': '42'}}
    saved = {'obj': {'foo': '42'}}
    assert patch(context, model, given=given, saved=saved, fill=True) == {
        'obj': {
            'bar': None,
        }
    }
