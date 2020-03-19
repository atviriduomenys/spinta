from spinta.testing.utils import create_manifest_files, read_manifest_files
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.utils.json import fix_data_for_json
from spinta.cli import freeze, migrate


def _summarize_ast(ast):
    return (ast['name'],) + tuple(
        arg for arg in ast['args']
        if not isinstance(arg, dict)
    )


def _summarize_actions(actions):
    return [
        {
            **action,
            'upgrade': _summarize_ast(action['upgrade']),
            'downgrade': _summarize_ast(action['downgrade']),
        }
        for action in actions
    ]


def configure(rc, path):
    return rc.fork().add('test', {
        'manifests.default': {
            'type': 'spinta',
            'backend': 'default',
            'sync': 'yaml',
        },
        'manifests.yaml.path': str(path),
    })


def test_create_model(postgresql, rc, cli, tmpdir, request):
    create_manifest_files(tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    rc = rc.fork().add('test', {
        'manifests.default': {
            'type': 'spinta',
            'backend': 'default',
            'sync': 'yaml',
        },
        'manifests.yaml.path': str(tmpdir),
    })

    cli.invoke(rc, freeze)

    manifest = read_manifest_files(tmpdir)
    assert manifest == {
        'country.yml': [
            {
                'type': 'model',
                'name': 'country',
                'version': {
                    'id': manifest['country.yml'][1]['version']['id'],
                    'date': manifest['country.yml'][1]['version']['date'],
                },
                'properties': {
                    'name': {'type': 'string'},
                },
            },
            {
                'version': {
                    'id': manifest['country.yml'][1]['version']['id'],
                    'date': manifest['country.yml'][1]['version']['date'],
                    'parents': [],
                },
                'changes': manifest['country.yml'][1]['changes'],
                'migrate': [
                    {
                        'type': 'schema',
                        'upgrade': (
                            "create_table(\n"
                            "    'country',\n"
                            "    column('_id', pk()),\n"
                            "    column('_revision', string()),\n"
                            "    column('name', string())\n"
                            ")"
                        ),
                        'downgrade': "drop_table('country')",
                    },
                ],
            },
        ],
    }

    cli.invoke(rc, migrate)

    context = create_test_context(rc, name='pytest/client')
    request.addfinalizer(context.wipe_all)

    client = create_test_client(context)
    client.authmodel('_version', ['getall', 'search'])

    data = client.get('/_version').json()['_data']

    assert len(data) == 1
    data = data[0]
    data['actions'] = _summarize_actions(data['actions'])
    assert data['actions'] == [
        {
            'type': 'schema',
            'upgrade': ('create_table', 'country'),
            'downgrade': ('drop_table', 'country'),
        },
    ]
    assert data['applied'] is None
    assert data['version'] == manifest['country.yml'][1]['version']['id']
    assert data['schema'] == fix_data_for_json(manifest['country.yml'][0])
