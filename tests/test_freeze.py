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
                            "add_column('country', 'code', string())",
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
