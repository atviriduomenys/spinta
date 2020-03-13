from spinta.testing.utils import create_manifest_files, read_manifest_files
from spinta.testing.context import create_test_context
from spinta.components import Config, Store
from spinta import commands
from spinta.auth import AdminToken


def create(config, path, files):
    context = create_manifest_files(path, files)

    context = create_test_context(config)
    context.load({
        'manifests': {
            'yaml': {
                'path': str(path),
            }
        }
    })
    return context


def test_create_model(postgresql, config, tmpdir):
    context = create(config, tmpdir, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    store = context.get('store')
    commands.freeze(context, store)

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

    rc = config
    context = create_test_context(rc)
    context.set('auth.token', AdminToken())

    config = context.set('config', Config())
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.set('store', Store())
    commands.load(context, store, config)
    commands.check(context, store)

    commands.prepare(context, store)

    commands.bootstrap(context, store)
    commands.sync(context, store)
    commands.migrate(context, store)
