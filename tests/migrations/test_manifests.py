import pytest

from spinta.exceptions import MultipleParentsError
from spinta.migrations import make_migrations
from spinta.testing.utils import create_manifest_files, read_manifest_files
from spinta.testing.context import create_test_context


def test_new_version_new_manifest(context, config, tmpdir):
    # tests new manifest without any migrations created
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

    create_manifest_files(tmpdir, {
        'models/report.yml': report_model,
    })

    context = create_test_context(config)
    context.load({
        'manifests': {
            'default': {
                'path': str(tmpdir),
            }
        }
    })

    make_migrations(context)

    manifests = read_manifest_files(tmpdir)

    # assert that manifest has schema and 1 version
    assert len(manifests['models/report.yml']) == 2

    assert manifests['models/report.yml'][0]['version']['id'] == \
        manifests['models/report.yml'][1]['version']['id']


def test_new_version_no_changes(context, config, tmpdir):
    # tests manifest with one version and no changes in schema
    report_model = [
        {
            'type': 'model',
            'name': 'report',
            'version': {
                'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                'date': '2020-03-14 15:26:53',
            },
            'properties': {
                'title': {'type': 'string'},
            },
        },
        {
            'version': {
                'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                'date': '2020-03-14 15:26:53',
                'parents': [],
            },
            'changes': [
                {'op': 'add', 'path': '/name', 'value': 'report'},
                {'op': 'add', 'path': '/properties', 'value': {
                    'title': {'type': 'string'},
                }},
                {'op': 'add', 'path': '/version', 'value': {
                    'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                    'date': '2020-03-14 15:26:53',
                }},
                {'op': 'add', 'path': '/type', 'value': 'model'},
            ],
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'create_table': {
                                'name': 'report',
                                'columns': [
                                    {'name': '_id', 'type': 'pk'},
                                    {'name': '_revision', 'type': 'string'},
                                    {'name': 'title', 'type': 'string'},
                                ],
                            },
                        },
                    ],
                    'downgrade': [
                        {'drop_table': {'name': 'report'}},
                    ],
                },
            },
        },
    ]

    create_manifest_files(tmpdir, {
        'models/report.yml': report_model,
    })

    context = create_test_context(config)
    context.load({
        'manifests': {
            'default': {
                'path': str(tmpdir),
            }
        }
    })

    make_migrations(context)

    manifests = read_manifest_files(tmpdir)

    # assert that manifest has schema and 1 version (i.e. no changes)
    assert len(manifests['models/report.yml']) == 2

    assert report_model[0]['version']['id'] == \
        manifests['models/report.yml'][0]['version']['id']
    assert manifests['models/report.yml'][0]['version']['id'] == \
        manifests['models/report.yml'][1]['version']['id']


def test_new_version_with_changes(context, config, tmpdir):
    # tests manifest with one version and changes in schema
    report_model = [
        {
            'type': 'model',
            'name': 'report',
            'version': {
                'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                'date': '2020-03-14 15:26:53',
            },
            'properties': {
                'title': {'type': 'string'},
                'status': {'type': 'integer'},
            },
        },
        {
            'version': {
                'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                'date': '2020-03-14 15:26:53',
                'parents': [],
            },
            'changes': [
                {'op': 'add', 'path': '/name', 'value': 'report'},
                {'op': 'add', 'path': '/properties', 'value': {
                    'title': {'type': 'string'},
                }},
                {'op': 'add', 'path': '/type', 'value': 'model'},
            ],
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'create_table': {
                                'name': 'report',
                                'columns': [
                                    {'name': '_id', 'type': 'pk'},
                                    {'name': '_revision', 'type': 'string'},
                                    {'name': 'title', 'type': 'string'},
                                ],
                            },
                        },
                    ],
                    'downgrade': [
                        {'drop_table': {'name': 'report'}},
                    ],
                },
            },
        },
    ]

    create_manifest_files(tmpdir, {
        'models/report.yml': report_model,
    })

    context = create_test_context(config)
    context.load({
        'manifests': {
            'default': {
                'path': str(tmpdir),
            }
        }
    })

    make_migrations(context)

    manifests = read_manifest_files(tmpdir)

    # assert that manifest has schema and 2 versions
    assert len(manifests['models/report.yml']) == 3

    latest_manifest = manifests['models/report.yml'][-1]
    # make sure that latest migration has different version id
    assert report_model[0]['version']['id'] != latest_manifest['version']['id']
    # assert that schema version has been changed
    assert manifests['models/report.yml'][0]['version']['id'] == latest_manifest['version']['id']
    # assert that latest changes has new field
    assert latest_manifest['migrate']['schema']['upgrade'] == [
        {'add_column': {'table': 'report', 'name': 'status', 'type': 'integer'}}
    ]


def test_new_version_branching_versions(context, config, tmpdir):
    # tests manifest with 3 versions, where 2 versions branch from the first one.
    report_model = [
        {
            'type': 'model',
            'name': 'report',
            'version': {
                'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                'date': '2020-03-14 15:26:53',
            },
            'properties': {
                'title': {'type': 'string'},
            },
        },
        {
            'version': {
                'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                'date': '2020-03-14 15:26:53',
                'parents': [],
            },
            'changes': [
                {'op': 'add', 'path': '/name', 'value': 'report'},
                {'op': 'add', 'path': '/properties', 'value': {
                    'title': {'type': 'string'},
                }},
                {'op': 'add', 'path': '/version', 'value': {
                    'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                    'date': '2020-03-14 15:26:53',
                }},
                {'op': 'add', 'path': '/type', 'value': 'model'},
            ],
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'create_table': {
                                'name': 'report',
                                'columns': [
                                    {'name': '_id', 'type': 'pk'},
                                    {'name': '_revision', 'type': 'string'},
                                    {'name': 'title', 'type': 'string'},
                                ],
                            },
                        },
                    ],
                    'downgrade': [
                        {'drop_table': {'name': 'report'}},
                    ],
                },
            },
        },
        {
            'version': {
                'id': '815a70b4-a3e8-43d9-be9f-7f6c13291298',
                'date': '2020-03-16 12:26:00',
                'parents': ['a8ecf2ce-bfb7-49cd-b453-27898f8e03a2'],
            },
            'changes': [
                {'op': 'add', 'path': '/properties', 'value': {
                    'status': {'type': 'integer'},
                }},
            ],
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'add_column': {
                                'name': 'status',
                                'table': 'report',
                                'type': 'integer',
                            },
                        },
                    ],
                    'downgrade': [
                        {'drop_column': {'name': 'status', 'table': 'report'}},
                    ],
                },
            },
        },
        {
            'version': {
                'id': '617c0a8e-d71f-45ce-9881-691f2ceac2f7',
                'date': '2020-03-14 15:26:53',
                'parents': ['a8ecf2ce-bfb7-49cd-b453-27898f8e03a2'],
            },
            'changes': [
                {'op': 'add', 'path': '/properties', 'value': {
                    'report_type': {'type': 'string'},
                }},
            ],
            'migrate': {
                'schema': {
                    'upgrade': [
                        {
                            'add_column': {
                                'name': 'report_type',
                                'table': 'report',
                                'type': 'string',
                            },
                        },
                    ],
                    'downgrade': [
                        {'drop_column': {'name': 'report_type', 'table': 'report'}},
                    ],
                },
            },
        },
    ]

    create_manifest_files(tmpdir, {
        'models/report.yml': report_model,
    })

    context = create_test_context(config)
    context.load({
        'manifests': {
            'default': {
                'path': str(tmpdir),
            }
        }
    })

    with pytest.raises(MultipleParentsError):
        make_migrations(context)


def test_new_version_w_foreign_key(context, config, tmpdir):
    # test new schema version, when model has foreign keys
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

    create_manifest_files(tmpdir, {
        'models/org.yml': org_model,
        'models/country.yml': country_model,
    })

    context = create_test_context(config)
    context.load({
        'manifests': {
            'default': {
                'path': str(tmpdir),
            }
        }
    })

    make_migrations(context)

    manifests = read_manifest_files(tmpdir)

    assert manifests['models/org.yml'][-1]['version']['parents'] == [
        manifests['models/country.yml'][-1]['version']['id'],
    ]
