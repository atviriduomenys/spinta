from spinta import commands
from spinta.testing.utils import create_manifest_files
from spinta.testing.context import create_test_context
from spinta.cli import freeze


def test_getall_no_versions(rc, tmpdir):
    rc = rc.fork({
        'manifest': 'default',
        'manifests.default': {
            'type': 'yaml',
            'path': str(tmpdir),
        },
    })

    create_manifest_files(tmpdir, {
        'country.yml': [
            {
                'type': 'model',
                'name': 'country',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
        ],
    })

    context = create_test_context(rc, name='pytest/client')

    config = context.get('config')
    commands.load(context, config)

    store = context.get('store')
    commands.load(context, store)

    assert list(commands.getall(context, store.manifest, query=None)) == [
        {
            '_id': None,
            '_type': '_schema/version',
            'type': 'model',
            'name': 'country',
            'id': None,
            'created': None,
            'synced': None,
            'applied': None,
            'changes': None,
            'migrate': None,
            'parents': None,
            'schema': {
                'type': 'model',
                'name': 'country',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
        }
    ]


def test_getall_with_versions(rc, cli, tmpdir):
    rc = rc.fork({
        'manifest': 'default',
        'manifests.default': {
            'type': 'yaml',
            'path': str(tmpdir),
        },
    })

    create_manifest_files(tmpdir, {
        'country.yml': [
            {
                'type': 'model',
                'name': 'country',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
        ],
    })

    cli.invoke(rc, freeze)

    context = create_test_context(rc, name='pytest/client')

    config = context.get('config')
    commands.load(context, config)

    store = context.get('store')
    commands.load(context, store)

    assert list(commands.getall(context, store.manifest, query=None)) == [
        {
            '_id': None,
            '_type': '_schema/version',
            'type': 'model',
            'name': 'country',
            'id': None,
            'created': None,
            'synced': None,
            'applied': None,
            'changes': None,
            'migrate': None,
            'parents': None,
            'schema': {
                'type': 'model',
                'name': 'country',
                'properties': {
                    'name': {'type': 'string'},
                },
            },
        }
    ]
