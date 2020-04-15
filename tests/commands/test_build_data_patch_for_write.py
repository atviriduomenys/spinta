from spinta.commands import load
from spinta.commands import build_data_patch_for_write as patch
from spinta.components import Model


def create_model(context, schema):
    manifest = context.get('store').manifest
    data = {
        'type': 'model',
        'name': 'model',
        **schema,
    }
    model = Model()
    model.eid = '9244f3a6-a672-4aac-bb1c-831646264a51'
    return load(context, model, data, manifest)


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
    assert patch(context, model, given=given, saved=saved, update_action=True) == {
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
    assert patch(context, model, given=given, saved=saved, update_action=True) == {
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
    assert patch(context, model, given=given, saved=saved, update_action=True) == {
        'list': [
            {
                'bar': None,
                'foo': '42',
            },
        ]
    }


def test_array(context):
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
    given = {'list': [{'foo': '42', 'bar': '24'}]}
    saved = {'list': [{'foo': '42'}]}
    assert patch(context, model, given=given, saved=saved) == {
        'list': [
            {
                'foo': '42',
                'bar': '24',
            },
        ]
    }


def test_array_fill_saved_all_same(context):
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
    given = {'list': [{'foo': '42', 'bar': None}]}
    saved = {'list': [{'foo': '42', 'bar': None}]}
    assert patch(
        context,
        model,
        given=given,
        saved=saved,
        update_action=True
    ) == {}


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
    assert patch(context, model, given=given, saved=saved) == {
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
    assert patch(context, model, given=given, saved=saved, update_action=True) == {
        'obj': {
            'bar': None,
        }
    }


def test_object_fill_saved_all_same(context):
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
    given = {'obj': {'foo': '42', 'bar': None}}
    saved = {'obj': {'foo': '42', 'bar': None}}
    assert patch(context, model, given=given, saved=saved, update_action=True) == {
    }


def test_file_fill_saved(context):
    model = create_model(context, {
        'properties': {
            'file': {
                'type': 'file',
            },
        },
    })
    given = {'file': {'_id': 'test.txt', '_content_type': 'text/plain'}}
    saved = {'file': {'_id': 'test.txt'}}
    assert patch(context, model, given=given, saved=saved, update_action=True) == {
        'file': {
            '_content_type': 'text/plain',
        },
    }


def test_file_fill_saved_all_same(context):
    model = create_model(context, {
        'properties': {
            'file': {
                'type': 'file',
            },
        },
    })
    given = {'file': {'_id': 'test.txt', '_content_type': 'text/plain'}}
    saved = {'file': {'_id': 'test.txt', '_content_type': 'text/plain'}}
    assert patch(context, model, given=given, saved=saved, update_action=True) == {}
