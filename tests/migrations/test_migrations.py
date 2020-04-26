import pytest

from spinta.testing.utils import (
    create_manifest_files,
    update_manifest_files,
    read_manifest_files,
)
from spinta.cli import freeze, migrate


def configure(rc, path):
    return rc.fork().add('test', {
        'manifests.default': {
            'type': 'backend',
            'backend': 'default',
            'sync': 'yaml',
        },
        'manifests.yaml.path': str(path),
    })


def test_create_table(rc, cli, tmpdir):
    rc = configure(rc, tmpdir)

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

    manifest = read_manifest_files(tmpdir)
    freezed = manifest['country.yml'][-1]
    assert freezed == {
        'id': freezed['id'],
        'date': freezed['date'],
        'parents': [],
        'changes': freezed['changes'],
        'migrate': [
            {
                'type': 'schema',
                'upgrade':
                    "create_table(\n"
                    "    'country',\n"
                    "    column('_id', pk()),\n"
                    "    column('_revision', string()),\n"
                    "    column('name', string())\n"
                    ")",
                'downgrade': "drop_table('country')",
            },
        ],
    }


@pytest.mark.skip('TODO')
def test_add_column(rc, cli, tmpdir):
    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })
    rc = configure(rc, tmpdir)
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {'op': 'add', 'path': '/properties/code', 'value': {
                'type': 'string',
            }},
        ],
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    freezed = manifest['country.yml'][-1]
    assert freezed == {
        'version': {
            'id': freezed['version']['id'],
            'date': freezed['version']['date'],
            'parents': [],
        },
        'changes': freezed['changes'],
        'migrate': [
            {
                'type': 'schema',
                'upgrade':
                    "add_column(\n"
                    "    'country',\n"
                    "    column('code', string())\n"
                    ")",
                'downgrade': "drop_column('country', 'code')",
            },
        ],
    }


@pytest.mark.skip('TODO')
def test_alter_column(rc, cli, tmpdir):
    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
                'area': {'type': 'integer'},
            },
        },
    })
    rc = configure(rc, tmpdir)
    cli.invoke(rc, freeze)

    update_manifest_files(tmpdir, {
        'country.yml': [
            {'op': 'replace', 'path': '/properties/area/type', 'value': 'number'},
        ],
    })
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert manifest['country.yml'][-1] == {
    }


@pytest.mark.skip('TODO')
def test_schema_with_multiple_head_nodes(rc, cli, tmpdir):
    # test schema creator when resource has a diverging scheme version history
    # i.e. 1st (root) schema migration has 2 diverging branches
    #
    # have model with 3 schema versions, with 2nd and 3rd diverging from 1st:
    # 1st version: create table with title: string and area: integer
    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'title': {'type': 'string'},
                'area': {'type': 'number'},
                'code': {'type': 'string'},
            }
        },
    })
    rc = configure(rc, tmpdir)
    cli.invoke(rc, freeze)
    manifest = read_manifest_files(tmpdir)
    version = manifest['country.yml'][-1]['version']['id']

    # 2nd version (parent 1st): add column - code: string
    update_manifest_files(tmpdir, {
        'country.yml': [
            {'op': 'add', 'path': '/properties/code', 'value': {
                'type': 'string',
            }},
        ],
    })
    cli.invoke(rc, freeze)

    # 3rd version (parent 1st): alter column - area: number
    update_manifest_files(tmpdir, {
        'country.yml': [
            {'op': 'replace', 'path': '/properties/area/type', 'value': 'number'},
        ],
    })
    cli.invoke(rc, freeze, ['-p', version])
    manifest = read_manifest_files(tmpdir)
    assert manifest['country.yml'][-1]['version']['id'] == version

    cli.invoke(rc, migrate)


@pytest.mark.skip('TODO')
def test_build_schema_relation_graph(rc, cli, tmpdir):
    # test new schema version, when multiple models have no foreign keys
    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'title': {'type': 'string'},
            },
        },
        'org.yml': {
            'type': 'model',
            'name': 'org',
            'properties': {
                'title': {'type': 'string'},
                'country': {'type': 'ref', 'model': 'country'},
            }
        },
        'report.yml': {
            'type': 'model',
            'name': 'report',
            'properties': {
                'title': {'type': 'string'},
                'org': {'type': 'ref', 'model': 'org'},
            },
        }
    })
    rc = configure(rc, tmpdir)
    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    version = {
        'country': manifest['country.yml'][-1]['version'],
        'org': manifest['org.yml'][-1]['version'],
        'report': manifest['report.yml'][-1]['version'],
    }
    assert version['country']['parent'] == []
    assert version['org']['parent'] == [version['country']['id']]
    assert version['report']['parent'] == [version['org']['id']]
