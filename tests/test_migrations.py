from spinta.testing.utils import create_manifest_files, read_manifest_files, readable_manifest_files
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.cli import bootstrap, freeze, migrate


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
    return rc.fork({
        'backends': ['default'],
        'manifests.default': {
            'type': 'backend',
            'backend': 'default',
            'sync': 'yaml',
        },
        'manifests.yaml.path': str(path),
    })


def test_create_model(postgresql, rc, cli, tmpdir, request):
    rc = configure(rc, tmpdir)

    cli.invoke(rc, bootstrap)

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
                            ")",
                        ],
                        'downgrade': [
                            "drop_table('country')",
                        ],
                    },
                ],
            },
        ],
    }

    cli.invoke(rc, migrate)

    context = create_test_context(rc, name='pytest/client')
    request.addfinalizer(context.wipe_all)

    client = create_test_client(context)
    client.authmodel('_schema/version', ['getall', 'search'])

    data = client.get('/_schema/version?select(type, name)').json()['_data']
    data = [d for d in data if not d['name'].startswith('_')]
    assert data == [
        {'type': 'model', 'name': 'country'},
    ]
