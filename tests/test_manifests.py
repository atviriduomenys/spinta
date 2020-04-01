from spinta import commands
from spinta.testing.utils import create_manifest_files
from spinta.testing.context import create_test_context
from spinta.manifests.components import Manifest
from spinta.cli import sync


def show(c: Manifest):
    if isinstance(c, Manifest):
        res = {'type': c.type}
        return res


def test_manifest_loading(rc, cli, tmpdir):
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
        'manifest': 'default',
        'manifests.default': {
            'type': 'internal',
            'sync': 'yaml',
        },
        'manifests.yaml': {
            'type': 'yaml',
            'path': str(tmpdir),
        },
    })

    cli.invoke(rc, sync)

    context = create_test_context(rc)

    config = context.get('config')
    commands.load(context, config, rc)

    store = context.get('store')
    commands.load(context, store, config)
    commands.link(context, store)

    assert show(store.manifest) == {
        'type': 'internal',
    }
