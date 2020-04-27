from spinta import commands
from spinta.testing.context import create_test_context
from spinta.testing.tabular import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import render_tabular_manifest


def test_loading(postgresql, rc, cli, tmpdir, request):
    rc = rc.fork().add('test', {
        'manifest': 'default',
        'manifests.default': {
            'type': 'tabular',
            'path': str(tmpdir / 'manifest.csv'),
        },
    })

    table = striptable('''
    id | d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |        |         |       |        |     | Example |
       |   | data                 |             |           |        | default |       |        |     | Data    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | country      |             | code='lt' |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       | lower()   | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | city         |             |           |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | šalis       |           | ref    | country | 4     | open   |     | Country |
    ''')

    create_tabular_manifest(tmpdir / 'manifest.csv', table)

    context = create_test_context(rc)

    config = context.get('config')
    commands.load(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    assert render_tabular_manifest(store.manifest) == table
