import pathlib

from responses import GET

from spinta.store import Store


def test_csv(postgresql, responses):
    config = {
        'backends': {
            'default': {
                'type': 'postgresql',
                'dsn': postgresql,
            },
        },
        'manifests': {
            'default': {
                'path': pathlib.Path(__file__).parents[1] / 'manifest',
            },
        },
    }

    store = Store()
    store.add_types()
    store.add_commands()
    store.configure(config)
    store.prepare(internal=True)
    store.migrate(internal=True)
    store.prepare()
    store.migrate()

    responses.add(
        GET, 'http://example.com/countries.csv',
        status=200, content_type='text/plain; charset=utf-8',
        body=(
            'kodas,šalis\n'
            'lt,Lietuva\n'
            'lv,Latvija\n'
            'ee,Estija'
        ),
    )

    assert len(store.pull('csv')) == 3
    assert sorted([(x['code'], x['title']) for x in store.getall('country')]) == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]
