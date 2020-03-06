from spinta.testing.utils import create_manifest_files, read_manifest_files
from spinta.testing.context import create_test_context
from spinta.components import Config, Store
from spinta.migrations import freeze
from spinta import commands


def create(config, path, files):
    context = create_manifest_files(path, files)

    context = create_test_context(config)
    context.load({
        'manifests': {
            'default': {
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

    freeze(context)

    manifest = read_manifest_files(tmpdir)
    assert manifest == {
        'country.yml': [
            {
                'type': 'model',
                'name': 'country',
                'version': {
                    'date': manifest['country.yml'][1]['version']['date'],
                    'id': manifest['country.yml'][1]['version']['id'],
                    'parents': [],
                },
                'properties': {
                    'name': {'type': 'string'},
                },
            },
            {
                'version': {
                    'date': manifest['country.yml'][1]['version']['date'],
                    'id': manifest['country.yml'][1]['version']['id'],
                    'parents': [],
                },
                'changes': manifest['country.yml'][1]['changes'],
                'migrate': {
                    'schema': {
                        'upgrade': [
                            "create_table(\n"
                            "    'country',\n"
                            "    column('_id', pk()),\n"
                            "    column('_revision', string()),\n"
                            "    column('name', string())\n"
                            ")",
                        ],
                        'downgrade': [
                            "drop_table('country')",
                        ],
                    },
                },
            },
        ],
    }

    rc = config
    context = create_test_context(rc)

    config = context.set('config', Config())
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.set('store', Store())
    commands.load(context, store, config)
    commands.check(context, store)

    commands.bootstrap(context, store)
    commands.sync(context. store)
    commands.migrate(context, store)
