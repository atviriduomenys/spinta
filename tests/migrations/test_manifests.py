import pytest

from spinta.exceptions import MultipleParentsError
from spinta.cli import freeze
from spinta.testing.utils import create_manifest_files, read_manifest_files
from spinta.testing.utils import update_manifest_files
from spinta import spyna


def _summarize_ast(ast):
    return (ast['name'],) + tuple(
        arg for arg in ast['args']
        if not isinstance(arg, dict)
    )


def _summarize_actions(actions):
    return [
        {
            **action,
            'upgrade': _summarize_ast(spyna.parse(action['upgrade'])),
            'downgrade': _summarize_ast(spyna.parse(action['downgrade'])),
        }
        for action in actions
    ]


def configure(rc, path):
    return rc.fork().add('test', {
        'manifests.default': {
            'type': 'backend',
            'backend': 'default',
            'sync': 'yaml',
        },
        'manifests.yaml.path': str(path),
    })


def test_new_version_new_manifest(rc, cli, tmpdir):
    create_manifest_files(tmpdir, {
        'models/report.yml': {
            'type': 'model',
            'name': 'report',
            'version': {
                'id': 'a8ecf2ce-bfb7-49cd-b453-27898f8e03a2',
                'date': '2020-03-14 15:26:53'
            },
            'properties': {
                'title': {'type': 'string'},
            },
        },
    })
    rc = configure(rc, tmpdir)
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert len(manifest['models/report.yml']) == 2

    current, freezed = manifest['models/report.yml']
    assert current['version'] == freezed['id']


def test_new_version_no_changes(rc, cli, tmpdir):
    rc = configure(rc, tmpdir)

    create_manifest_files(tmpdir, {
        'models/report.yml': {
            'type': 'model',
            'name': 'report',
            'properties': {
                'title': {'type': 'string'},
            },
        },
    })

    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    assert len(manifest['models/report.yml']) == 2
    current = manifest['models/report.yml'][0]

    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    assert len(manifest['models/report.yml']) == 2
    freezed = manifest['models/report.yml'][1]

    current, freezed = manifest['models/report.yml']
    assert current['version'] == freezed['id']


def test_new_version_with_changes(rc, cli, tmpdir):
    rc = configure(rc, tmpdir)

    create_manifest_files(tmpdir, {
        'models/report.yml': {
            'type': 'model',
            'name': 'report',
            'properties': {
                'title': {'type': 'string'},
            },
        },
    })

    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'models/report.yml': [
            {'op': 'add', 'path': '/properties/status', 'value': {
                'type': 'integer',
            }}
        ],
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert len(manifest['models/report.yml']) == 3

    current, previous, freezed = manifest['models/report.yml']

    # make sure that latest migration has different version id
    assert current['version'] != previous['id']
    assert current['version'] == freezed['id']

    # assert that latest changes has new field
    assert freezed['migrate'] == [
        {
            'type': 'schema',
            'upgrade': "add_column('report', column('status', integer()))",
            'downgrade': "drop_column('report', 'status')",
        }
    ]


@pytest.mark.skip('TODO')
def test_new_version_branching_versions(rc, cli, tmpdir):
    # tests manifest with 3 versions, where 2 versions branch from the first
    # one.
    schema = [
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
        'models/report.yml': schema,
    })
    rc = configure(rc, tmpdir)
    with pytest.raises(MultipleParentsError):
        cli.invoke(rc, freeze)


@pytest.mark.skip('TODO')
def test_new_version_w_foreign_key(rc, cli, tmpdir):
    # test new schema version, when model has foreign keys
    create_manifest_files(tmpdir, {
        'models/org.yml': [
            {
                'type': 'model',
                'name': 'org',
                'version': {
                    'id': '365b3209-c00f-4357-9749-5f680d337834',
                    'date': '2020-03-14 15:26:53'
                },
                'properties': {
                    'title': {'type': 'string'},
                    'country': {'type': 'ref', 'model': 'country'},
                }
            }
        ],
        'models/country.yml': [
            {
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
        ],
    })
    rc = configure(rc, tmpdir)
    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    freezed = manifest['models/org.yml'][-1]
    assert freezed['version']['parents'] == [
        manifest['models/country.yml'][-1]['version']['id'],
    ]
