import datetime

from spinta.utils.itertools import consume
from spinta.commands import push


def test_app(app):
    resp = app.get('/')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
        ],
        'items': [
            ('capital', '/capital'),
            ('continent', '/continent'),
            ('country', '/country'),
            ('deeply', '/deeply'),
            ('org', '/org'),
            ('rinkimai', '/rinkimai'),
            ('tenure', '/tenure'),
        ],
        'datasets': [],
        'header': [],
        'data': [],
        'row': [],
        'formats': [],
    }


def test_directory(app):
    resp = app.get('/rinkimai')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', None),
        ],
        'items': [
            ('apygarda', '/rinkimai/apygarda'),
            ('apylinke', '/rinkimai/apylinke'),
            ('kandidatas', '/rinkimai/kandidatas'),
            ('turas', '/rinkimai/turas'),
        ],
        'datasets': [
            {'name': 'json', 'link': '/rinkimai/:source/json', 'canonical': False},
            {'name': 'xlsx', 'link': '/rinkimai/:source/xlsx', 'canonical': False},
        ],
        'header': [],
        'data': [],
        'row': [],
        'formats': [],
    }


def test_dataset(context, app):
    store = context.get('store')
    with context.enter():
        context.bind('transaction', store.backends['default'].transaction, write=True)
        consume(push(context, store, [
            {
                'type': 'rinkimai/:source/json',
                'id': 'Rinkimai 1',
                'pavadinimas': 'Rinkimai 1',
            },
        ]))

    resp = app.get('/rinkimai/:source/json')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', None),
            (':changes', '/rinkimai/:source/json/:changes'),
        ],
        'items': [],
        'datasets': [],
        'header': ['id', 'pavadinimas'],
        'data': [
            [
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': None, 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/rinkimai/:source/json/:format/csv'),
            ('JSON', '/rinkimai/:source/json/:format/json'),
            ('JSONL', '/rinkimai/:source/json/:format/jsonl'),
            ('ASCII', '/rinkimai/:source/json/:format/asciitable'),
        ],
    }


def test_nested_dataset(context, app):
    store = context.get('store')
    with context.enter():
        context.bind('transaction', store.backends['default'].transaction, write=True)
        consume(push(context, store, [
            {
                'type': 'deeply/nested/model/name/:source/nested/dataset/name',
                'id': '42',
                'name': 'Nested One',
            },
        ]))

    resp = app.get('deeply/nested/model/name/:source/nested/dataset/name')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('deeply', '/deeply'),
            ('nested', '/deeply/nested'),
            ('model', '/deeply/nested/model'),
            ('name', '/deeply/nested/model/name'),
            (':source/nested/dataset/name', None),
            (':changes', '/deeply/nested/model/name/:source/nested/dataset/name/:changes'),
        ],
        'items': [],
        'datasets': [],
        'header': ['id', 'name'],
        'data': [
            [
                {'color': None, 'link': '/deeply/nested/model/name/e2ff1ff0f7d663344abe821582b0908925e5b366/:source/nested/dataset/name', 'value': 'e2ff1ff0'},
                {'color': None, 'link': None, 'value': 'Nested One'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/deeply/nested/model/name/:source/nested/dataset/name/:format/csv'),
            ('JSON', '/deeply/nested/model/name/:source/nested/dataset/name/:format/json'),
            ('JSONL', '/deeply/nested/model/name/:source/nested/dataset/name/:format/jsonl'),
            ('ASCII', '/deeply/nested/model/name/:source/nested/dataset/name/:format/asciitable'),
        ],
    }


def test_dataset_key(context, app):
    store = context.get('store')
    with context.enter():
        context.bind('transaction', store.backends['default'].transaction, write=True)
        consume(push(context, store, [
            {
                'type': 'rinkimai/:source/json',
                'id': 'Rinkimai 1',
                'pavadinimas': 'Rinkimai 1',
            },
        ]))

    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', '/rinkimai/:source/json'),
            ('df6b9e04', None),
            (':changes', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes'),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/asciitable'),
        ],
        'datasets': [],
        'items': [],
        'header': [],
        'data': [],
        'row': [
            ('id', {
                'color': None,
                'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json',
                'value': 'df6b9e04ac9e2467690bcad6d9fd673af6e1919b',
            }),
            ('pavadinimas', {'color': None, 'link': None, 'value': 'Rinkimai 1'}),
            ('type', {'color': None, 'link': None, 'value': 'rinkimai/:source/json'}),
        ],
    }


def test_changes_single_object(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    store = context.get('store')
    with context.enter():
        context.bind('transaction', store.backends['default'].transaction, write=True)
        consume(push(context, store, [
            {
                'type': 'rinkimai/:source/json',
                'id': 'Rinkimai 1',
                'pavadinimas': 'Rinkimai 1',
            },
        ]))
        consume(push(context, store, [
            {
                'type': 'rinkimai/:source/json',
                'id': 'Rinkimai 1',
                'pavadinimas': 'Rinkimai 2',
            },
        ]))

    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', '/rinkimai/:source/json'),
            ('df6b9e04', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/asciitable'),
        ],
        'datasets': [],
        'items': [],
        'header': [
            'change_id',
            'transaction_id',
            'datetime',
            'action',
            'id',
            'pavadinimas',
        ],
        'data': [
            [
                {'color': None, 'link': None, 'value': resp.context['data'][0][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][0][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'update'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_changes_object_list(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    store = context.get('store')
    with context.enter():
        context.bind('transaction', store.backends['default'].transaction, write=True)
        consume(push(context, store, [
            {
                'type': 'rinkimai/:source/json',
                'id': 'Rinkimai 1',
                'pavadinimas': 'Rinkimai 1',
            },
        ]))
        consume(push(context, store, [
            {
                'type': 'rinkimai/:source/json',
                'id': 'Rinkimai 1',
                'pavadinimas': 'Rinkimai 2',
            },
        ]))

    resp = app.get('/rinkimai/:source/json/:changes')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', '/rinkimai/:source/json'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/:source/json/:changes/:format/csv'),
            ('JSON', '/rinkimai/:source/json/:changes/:format/json'),
            ('JSONL', '/rinkimai/:source/json/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/:source/json/:changes/:format/asciitable'),
        ],
        'datasets': [],
        'items': [],
        'header': [
            'change_id',
            'transaction_id',
            'datetime',
            'action',
            'id',
            'pavadinimas',
        ],
        'data': [
            [
                {'color': None, 'link': None, 'value': resp.context['data'][0][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][0][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'update'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_count(context, app):
    store = context.get('store')
    with context.enter():
        context.bind('transaction', store.backends['default'].transaction, write=True)
        consume(push(context, store, [
            {
                'type': 'rinkimai/:source/json',
                'id': 1,
                'pavadinimas': 'Rinkimai 1',
            },
            {
                'type': 'rinkimai/:source/json',
                'id': 2,
                'pavadinimas': 'Rinkimai 2',
            },
        ]))

    resp = app.get('/rinkimai/:source/json/:count')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context['header'] == ['count']
    assert resp.context['data'] == [[{'color': None, 'link': None, 'value': 2}]]
