from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.data import listdata
from spinta.testing.utils import create_manifest_files, read_manifest_files, readable_manifest_files
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context


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


def test_create_model(
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    request,
):
    rc = configure(rc, tmp_path)

    cli.invoke(rc, ['bootstrap'])

    create_manifest_files(tmp_path, {
        'country.yml': {
            'type': 'model',
            'name': 'Country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    cli.invoke(rc, ['freeze'])

    manifest = read_manifest_files(tmp_path)
    assert readable_manifest_files(manifest) == {
        'country.yml': [
            {
                'type': 'model',
                'name': 'Country',
                'id': 'Country:0',
                'version': 'Country:1',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
            {
                'id': 'Country:1',
                'parents': [],
                'migrate': [
                    {
                        'type': 'schema',
                        'upgrade': [
                            "create_table(",
                            "    'Country',",
                            "    column('_id', pk()),",
                            "    column('_revision', string()),",
                            "    column('name', string())",
                            ")",
                        ],
                        'downgrade': [
                            "drop_table('Country')",
                        ],
                    },
                ],
            },
        ],
    }

    # When tests fail with
    #     File "spinta/backends/postgresql/commands/migrate.py", in execute
    #       for action in version['actions']:
    #   KeyError: 'actions'
    # Check if database is cleaned properly.
    #   import sqlalchemy as sa
    #   query = 'select "type", "name", "id" from "_schema/Version"'
    #   engine = sa.create_engine(postgresql)
    #   result = engine.execute(query)
    #   pp(result)
    # To fix that, check context.wipe_all method.
    # And probably delete everything manually:
    #   engine.execute('delete from "_schema/Version"')

    cli.invoke(rc, ['migrate'])

    context = create_test_context(rc, name='pytest/client')
    request.addfinalizer(context.wipe_all)

    client = create_test_client(context)
    client.authmodel('_schema/Version', ['getall', 'search'])

    resp = client.get('/_schema/Version?select(type, name)')
    data = [(t, n) for t, n in listdata(resp) if not t.startswith('_')]
    assert data == [
        ('Country', 'model'),
    ]
