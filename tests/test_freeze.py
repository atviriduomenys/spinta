import pathlib

import pytest

from spinta.testing.utils import create_manifest_files
from spinta.testing.utils import update_manifest_files
from spinta.testing.utils import read_manifest_files
from spinta.testing.utils import readable_manifest_files
from spinta.cli import freeze


@pytest.fixture()
def rc(rc, tmpdir):
    return rc.fork().add('test', {
        'manifests.default': {
            'type': 'backend',
            'backend': 'default',
            'sync': 'yaml',
        },
        'manifests.yaml.path': str(tmpdir),
    })


def test_create_model(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    assert cli.invoke(rc, freeze).exit_code == 0

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest) == {
        'country.yml': [
            {
                'type': 'model',
                'name': 'country',
                'id': 'country:0',
                'version': 'country:1',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
            {
                'id': 'country:1',
                'parents': [],
                'migrate': [
                    {
                        'type': 'schema',
                        'upgrade': [
                            "create_table(",
                            "    'country',",
                            "    column('_id', pk()),",
                            "    column('_revision', string()),",
                            "    column('name', string())",
                            ")"
                        ],
                        'downgrade': [
                            "drop_table('country')",
                        ],
                    },
                ],
            },
        ],
    }


def test_add_column(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {'op': 'add', 'path': '/properties/code', 'value': {
                'type': 'string',
            }}
        ],
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest) == {
        'country.yml': [
            {
                'type': 'model',
                'name': 'country',
                'id': 'country:0',
                'version': 'country:2',
                'properties': {
                    'name': {'type': 'string'},
                    'code': {'type': 'string'},
                },
            },
            {
                'id': 'country:2',
                'parents': [],
                'migrate': [
                    {
                        'type': 'schema',
                        'upgrade': [
                            "add_column('country', column('code', string()))",
                        ],
                        'downgrade': [
                            "drop_column('country', 'code')",
                        ],
                    },
                ],
            },
        ],
    }


def test_freeze_no_changes(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    cli.invoke(rc, freeze)
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest) == {
        'country.yml': [
            {
                'type': 'model',
                'name': 'country',
                'id': 'country:0',
                'version': 'country:1',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
            {
                'id': 'country:1',
                'parents': [],
                'migrate': [
                    {
                        'type': 'schema',
                        'upgrade': [
                            "create_table(",
                            "    'country',",
                            "    column('_id', pk()),",
                            "    column('_revision', string()),",
                            "    column('name', string())",
                            ")"
                        ],
                        'downgrade': [
                            "drop_table('country')",
                        ],
                    },
                ],
            },
        ],
    }


def test_freeze_array(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'names': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    }
                },
            },
        },
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country/:list/names',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('country._id', ondelete: 'CASCADE')),",
                "    column('names', string())",
                ")",
            ],
            'downgrade': [
                "drop_table('country/:list/names')",
            ],
        },
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('names', json())",
                ")"
            ],
            'downgrade': [
                "drop_table('country')",
            ],
        },
    ]


def test_freeze_array_with_object(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'notes': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'note': {'type': 'string'}
                        }
                    }
                }
            },
        },
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'downgrade': ["drop_table('country/:list/notes')"],
            'type': 'schema',
            'upgrade': [
                'create_table(',
                "    'country/:list/notes',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('country._id', ondelete: 'CASCADE')),",
                "    column('notes.note', string())",
                ')',
            ],
        },
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('notes', json())",
                ")"
            ],
            'downgrade': [
                "drop_table('country')",
            ],
        },
    ]


def test_freeze_object(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'report.yml': {
            'type': 'model',
            'name': 'report',
            'properties': {
                'str': {'type': 'string'},
                'note': {
                    'type': 'object',
                    'properties': {
                        'text': {'type': 'string'},
                        'number': {'type': 'integer'},
                        'list': {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        }
                    }
                },
            },
        },
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['report.yml'][-1]['migrate'] == [
        {
            'downgrade': ["drop_table('report/:list/note.list')"],
            'type': 'schema',
            'upgrade': [
                'create_table(',
                "    'report/:list/note.list',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('report._id', ondelete: 'CASCADE')),",
                "    column('note.list', string())",
                ')',
            ],
        },
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'report',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('note.list', json()),",
                "    column('note.number', integer()),",
                "    column('note.text', string()),",
                "    column('str', string())",
                ")"
            ],
            'downgrade': [
                "drop_table('report')",
            ],
        },
    ]


def test_freeze_file(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'flag': {'type': 'file'},
                'anthem': {
                    'type': 'file',
                    'backend': 'fs'
                },
            },
        },
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'downgrade': ["drop_table('country/:file/flag')"],
            'type': 'schema',
            'upgrade': [
                'create_table(',
                "    'country/:file/flag',",
                "    column('_id', uuid()),",
                "    column('_block', binary())",
                ')',
            ],
        },
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('anthem._id', string()),",
                "    column('anthem._content_type', string()),",
                "    column('anthem._size', integer()),",
                "    column('flag._id', string()),",
                "    column('flag._content_type', string()),",
                "    column('flag._size', integer()),",
                "    column('flag._bsize', integer()),",
                "    column('flag._blocks', array(uuid()))",
                ")"
            ],
            'downgrade': [
                "drop_table('country')",
            ],
        },
    ]


def test_freeze_list_of_files(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'flags': {
                    'type': 'array',
                    'items': {
                        'type': 'file'
                    }
                },
            },
        },
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'downgrade': ["drop_table('country/:file/flags')"],
            'type': 'schema',
            'upgrade': [
                'create_table(',
                "    'country/:file/flags',",
                "    column('_id', uuid()),",
                "    column('_block', binary())",
                ')',
            ],
        },
        {
            'downgrade': ["drop_table('country/:list/flags')"],
            'type': 'schema',
            'upgrade': [
                'create_table(',
                "    'country/:list/flags',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('country._id', ondelete: 'CASCADE')),",
                "    column('flags._id', string()),",
                "    column('flags._content_type', string()),",
                "    column('flags._size', integer()),",
                "    column('flags._bsize', integer()),",
                "    column('flags._blocks', array(uuid()))",
                ')',
            ],
        },
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('flags', json())",
                ")"
            ],
            'downgrade': [
                "drop_table('country')",
            ],
        },
    ]


def test_freeze_ref(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
        'city.yml': {
            'type': 'model',
            'name': 'city',
            'properties': {
                'country': {
                    'type': 'ref',
                    'model': 'country'
                },
                'name': {'type': 'string'}
            }
        }
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files['city.yml'][-1]['migrate'] == [
        {
            'downgrade': ["drop_table('city')"],
            'type': 'schema',
            'upgrade': [
                'create_table(',
                "    'city',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('country._id', ref('country._id')),",
                "    column('name', string())",
                ')',
            ],
        },
    ]
    assert manifest_files['country.yml'][-1]['migrate'] == [
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('name', string())",
                ")"
            ],
            'downgrade': ["drop_table('country')"],
        },
    ]


def test_add_reference_column(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
        'city.yml': {
            'type': 'model',
            'name': 'city',
            'properties': {
                'name': {'type': 'string'}
            },
        },
    })
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'add',
                'path': '/properties/city',
                'value': {
                    'type': 'ref',
                    'model': 'city',
                },
            }
        ]
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files['country.yml'][-1]['migrate'] == [
        {
            'downgrade': [
                "drop_column('country', 'city._id')",
            ],
            'type': 'schema',
            'upgrade': [
                "add_column(",
                "    'country',",
                "    column('city._id', ref('city._id'))",
                ")",
            ],
        },
    ]


def test_change_ref_model(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
        'continent.yml': {
            'type': 'model',
            'name': 'continent',
            'properties': {
                'name': {'type': 'string'},
            },
        },
        'city.yml': {
            'type': 'model',
            'name': 'city',
            'properties': {
                'country': {
                    'type': 'ref',
                    'model': 'country',
                },
                'name': {'type': 'string'},
            }
        }
    })
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'city.yml': [
            {
                'op': 'replace',
                'path': '/properties/country/model',
                'value': 'continent',
            }
        ]
    })

    with pytest.raises(NotImplementedError) as e:
        cli.invoke(rc, freeze, catch_exceptions=False)


def test_freeze_ref_in_array(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'cities': {
                    'type': 'array',
                    'items': {
                        'type': 'ref',
                        'model': 'city'
                    }
                },
            },
        },
        'city.yml': {
            'type': 'model',
            'name': 'city',
            'properties': {
                'name': {'type': 'string'}
            }
        }
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files['city.yml'][-1]['migrate'] == [
        {
            'downgrade': ["drop_table('city')"],
            'type': 'schema',
            'upgrade': [
                'create_table(',
                "    'city',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('name', string())",
                ')',
            ],
        },
    ]
    assert manifest_files['country.yml'][-1]['migrate'] == [
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country/:list/cities',",
                "    column('_txn', uuid()),",
                "    column('_rid', ref('country._id', ondelete: 'CASCADE')),",
                "    column('cities._id', ref('city._id'))",
                ")"
            ],
            'downgrade': ["drop_table('country/:list/cities')"],
        },
        {
            'type': 'schema',
            'upgrade': [
                "create_table(",
                "    'country',",
                "    column('_id', pk()),",
                "    column('_revision', string()),",
                "    column('cities', json())",
                ")"
            ],
            'downgrade': ["drop_table('country')"],
        }
    ]


def test_change_field_type_in_object(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'population': {
                    'type': 'object',
                    'properties': {
                        'amount': {'type': 'string'}
                    },
                },
            }
        }
    })

    cli.invoke(rc, freeze)
    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'replace',
                'path': '/properties/population/properties/amount/type',
                'value': 'integer',
            },
        ]
    })

    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'downgrade': [
                'alter_column(',
                "    'country',",
                "    'population.amount',",
                '    type_: string()',
                ')',
            ],
            'type': 'schema',
            'upgrade': [
                'alter_column(',
                "    'country',",
                "    'population.amount',",
                '    type_: integer()',
                ')',
            ],
        },
    ]


def test_change_field_type_in_list(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'flags': {
                    'type': 'array',
                    'items': {'type': 'string'}
                }
            }
        }
    })

    cli.invoke(rc, freeze)
    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'replace',
                'path': '/properties/flags/items/type',
                'value': 'integer',
            }
        ]
    })

    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'downgrade': [
                'alter_column(',
                "    'country/:list/flags',",
                "    'flags',",
                '    type_: string()',
                ')',
            ],
            'type': 'schema',
            'upgrade': [
                'alter_column(',
                "    'country/:list/flags',",
                "    'flags',",
                '    type_: integer()',
                ')',
            ],
        },
    ]


def test_add_field_to_object(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'population': {
                    'type': 'object',
                    'properties': {
                        'amount': {'type': 'string'}
                    },
                }
            }
        }
    })

    cli.invoke(rc, freeze)
    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'add',
                'path': '/properties/population/properties/code',
                'value': {'type': 'string'}
            },
        ]
    })

    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'type': 'schema',
            'upgrade': [
                "add_column(",
                "    'country',",
                "    column('population.code', string())",
                ")"],
            'downgrade': ["drop_column('country', 'population.code')"],
        },
    ]


def test_add_field(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'}
            }
        }
    })

    cli.invoke(rc, freeze)
    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'add',
                'path': '/properties/cities',
                'value': {
                    'type': 'object',
                    'properties': {
                        'name': {'type': 'string'}
                    }
                }
            }
        ]
    })

    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'type': 'schema',
            'upgrade': ["add_column('country', column('cities.name', string()))"],
            'downgrade': ["drop_column('country', 'cities.name')"],
        }
    ]


def test_freeze_nullable(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {
                    'type': 'string',
                    'nullable': True
                }
            }
        }
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'type': 'schema',
            'upgrade': ["create_table(",
                        "    'country',",
                        "    column('_id', pk()),",
                        "    column('_revision', string()),",
                        "    column('name', string(), nullable: true)",
                        ")"],
            'downgrade': ["drop_table('country')"],
        }
    ]


def test_change_nullable(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {
                    'type': 'string',
                    'nullable': True
                }
            }
        }
    })
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'replace',
                'path': '/properties/name/nullable',
                'value': False
            }
        ]
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert readable_manifest_files(manifest)['country.yml'][-1]['migrate'] == [
        {
            'downgrade': ["alter_column('country', 'name', nullable: true)"],
            'type': 'schema',
            'upgrade': ["alter_column('country', 'name', nullable: false)"],
        },
    ]


def test_add_nullable_column(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'add',
                'path': '/properties/flag',
                'value': {
                    'type': 'string',
                    'nullable': True,
                },
            }
        ]
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files['country.yml'][-1]['migrate'] == [
        {
            'downgrade': [
                "drop_column('country', 'flag')",
            ],
            'type': 'schema',
            'upgrade': [
                "add_column(",
                "    'country',",
                "    column('flag', string(), nullable: true)",
                ")",
            ],
        },
    ]


def test_delete_property(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
                'flag': {'type': 'string'}
            },
        },
    })
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'remove',
                'path': '/properties/flag',
            }
        ]
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files['country.yml'][-1]['migrate'] == []


def test_delete_property_from_object(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
                'info': {
                    'type': 'object',
                    'properties': {
                        'flag': {'type': 'string'},
                        'capital': {'type': 'string'},
                    }
                }
            },
        },
    })
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'remove',
                'path': '/properties/info/properties/flag',
            }
        ]
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files['country.yml'][-1]['migrate'] == []


def test_replace_all_properties(rc, cli):
    tmpdir = rc.get('manifests', 'yaml', 'path', cast=pathlib.Path)

    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
                'flag': {'type': 'string'}
            },
        },
    })
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {
                'op': 'replace',
                'path': '/properties',
                'value': {'capital': {'type': 'string'}},
            }
        ]
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    manifest_files = readable_manifest_files(manifest)
    assert manifest_files['country.yml'][-1]['migrate'] == [
        {
            'downgrade': ["drop_column('country', 'capital')"],
            'type': 'schema',
            'upgrade': ["add_column('country', column('capital', string()))"],
        }
    ]
