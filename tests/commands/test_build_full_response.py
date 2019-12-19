import pathlib

from spinta.commands import load
from spinta.commands import build_full_response
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


def test_scalar_with_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'scalar': {'type': 'string'},
        }
    })

    patch = {'scalar': '42'}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'scalar': '42',
    }


def test_scalar_overwrite_saved(context):
    model = create_model(context, {
        'properties': {
            'scalar': {'type': 'string'},
        }
    })

    patch = {'scalar': '42'}
    saved = {'scalar': '13'}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'scalar': '42',
    }


def test_empty_scalar_with_non_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'scalar': {'type': 'string'},
        }
    })

    patch = {}
    saved = {'scalar': '42'}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {'scalar': '42'}


def test_obj_empty_patch_and_saved(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                },
            },
        },
    })

    # test empty patch and empty saved
    patch = {}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {'obj': {'foo': None}}


def test_obj_empty_saved_and_empty_obj(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                },
            },
        },
    })

    patch = {'obj': {}}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {'foo': None}
    }


def test_obj_with_default(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string', 'default': 'def_val'},
                },
            },
        },
    })

    patch = {'obj': {}}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {'foo': 'def_val'}
    }


def test_obj_with_default_and_non_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string', 'default': 'def_val'},
                },
            },
        },
    })

    patch = {'obj': {}}
    saved = {'obj': {'foo': 'non_default'}}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )

    # as nothing have changed - return already saved value
    assert full_patch == {
        'obj': {'foo': 'non_default'}
    }


def test_obj_non_empty_patch(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                },
            },
        },
    })

    patch = {'obj': {'foo': '42'}}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {'foo': '42'}
    }


def test_obj_non_empty_patch_and_saved(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                },
            },
        },
    })

    patch = {'obj': {'foo': '42'}}
    saved = {'obj': {'foo': '13'}}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {'foo': '42'}
    }


def test_obj_empty_patch_and_non_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                },
            },
        },
    })

    patch = {}
    saved = {'obj': {'foo': '13'}}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {'obj': {'foo': '13'}}


def test_obj_null_patch_and_non_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                },
            },
        },
    })

    patch = {'obj': {'foo': None}}
    saved = {'obj': {'foo': '13'}}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {'foo': None}
    }


def test_nested_obj(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                    'sub': {
                        'type': 'object',
                        'properties': {
                            'foos': {'type': 'string'},
                        },
                    },
                },
            },
        },
    })

    patch = {'obj': {'foo': None, 'sub': {'foos': '420'}}}
    saved = {'obj': {'foo': '13'}}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {'foo': None, 'sub': {'foos': '420'}}
    }


def test_complex_obj(context):
    model = create_model(context, {
        'properties': {
            'obj': {
                'type': 'object',
                'properties': {
                    'foo': {'type': 'string'},
                    'bar': {'type': 'string', 'default': 'default'},
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

    # test that missing values from patch are still filled with defaults
    patch = {'obj': {
        'foo': '42',
        'sub': {
            'foos': '420',
        }
    }}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {
            'foo': '42',
            'bar': 'default',
            'sub': {
                'foos': '420',
                'bars': None,
            }
        }
    }

    # test that missing obj values are filled in from saved values
    patch = {'obj': {
        'foo': '42',
        'sub': {
            'foos': '420',
        }
    }}
    saved = {'obj': {
        'foo': '00',
        'bar': None,  # XXX: if value is null, should it be converted to default
        'sub': {
            'foos': '000',
            'bars': 'abc',
        }
    }}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'obj': {
            'foo': '42',
            'bar': None,
            'sub': {
                'foos': '420',
                'bars': 'abc',
            }
        }
    }


def test_array_empty_patch_and_saved(context):
    model = create_model(context, {
        'properties': {
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                    },
                },
            },
        },
    })

    patch = {}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {'list': None}


def test_array_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                    },
                },
            },
        },
    })

    patch = {'list': []}
    saved = {}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'list': [],
    }


def test_array_non_empty_patch_and_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                    },
                },
            },
        },
    })

    patch = {'list': [{
        'foo': '42',
    }]}
    saved = {}

    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'list': [{
            'foo': '42',
        }]
    }


def test_array_non_empty_patch_and_non_empty_saved(context):
    model = create_model(context, {
        'properties': {
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                    },
                },
            },
        },
    })

    patch = {'list': []}
    saved = {'list': [{
        'foo': '42',
    }]}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'list': []
    }


def test_array_overwrite_saved(context):
    model = create_model(context, {
        'properties': {
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                    },
                },
            },
        },
    })
    patch = {'list': [{
        'foo': '49',
    }]}
    saved = {'list': [{
        'foo': '42',
    }]}
    full_patch = build_full_response(
        context, model,
        patch=patch,
        saved=saved
    )
    assert full_patch == {
        'list': [{
            'foo': '49',
        }]
    }
