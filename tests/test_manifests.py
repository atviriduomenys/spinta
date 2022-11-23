from spinta import commands
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.utils import create_manifest_files
from spinta.testing.context import create_test_context
from spinta.components import Model
from spinta.manifests.components import Manifest


def show(c: Manifest):
    if isinstance(c, Manifest):
        res = {
            'type': c.type,
            'nodes': {},
        }
        for group, nodes in c.objects.items():
            if nodes:
                res['nodes'][group] = {
                    name: show(node)
                    for name, node in nodes.items()
                }
        return res
    if isinstance(c, Model):
        return {
            'backend': c.backend.name,
        }


def test_manifest_loading(postgresql, rc, cli: SpintaCliRunner, tmp_path, request):
    rc = rc.fork({
        'manifest': 'default',
        'manifests': {
            'default': {
                'type': 'backend',
                'sync': 'yaml',
            },
            'yaml': {
                'type': 'yaml',
                'path': str(tmp_path),
            }
        },
        'backends': ['default'],
    })

    create_manifest_files(tmp_path, {
        'country.yml': {
            'type': 'model',
            'name': 'country',
            'properties': {
                'name': {'type': 'string'},
            },
        },
    })

    cli.invoke(rc, ['freeze'])
    cli.invoke(rc, ['migrate'])

    context = create_test_context(rc)

    config = context.get('config')
    commands.load(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.prepare(context, store.manifest)

    request.addfinalizer(context.wipe_all)

    assert show(store.manifest) == {
        'type': 'backend',
        'nodes': {
            'ns': {
                '': None,
                '_schema': None,
            },
            'model': {
                '_ns': {
                    'backend': 'default',
                },
                '_schema': {
                    'backend': 'default',
                },
                '_schema/version': {
                    'backend': 'default',
                },
                '_txn': {
                    'backend': 'default',
                },
                'country': {
                    'backend': 'default',
                },
            },
        },
    }
