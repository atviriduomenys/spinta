from spinta.testing.utils import create_manifest_files, read_manifest_files
from spinta.testing.context import create_test_context
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
    print(manifest['country.yml'][1]['migrate']['schema']['upgrade'])
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

    store = context.get('store')
    commands.sync(context)
    commands.migrate(context, store)
