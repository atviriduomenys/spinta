def test_app(app):
    resp = app.get('/')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
        ],
        'items': [
            ('country', '/country'),
            ('deeply', '/deeply'),
            ('org', '/org'),
            ('rinkimai', '/rinkimai'),
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
            ('json', '/rinkimai/:source/json'),
            ('xlsx', '/rinkimai/:source/xlsx'),
        ],
        'header': [],
        'data': [],
        'row': [],
        'formats': [],
    }


def test_dataset(store, app):
    store.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ])

    resp = app.get('/rinkimai/:source/json')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', None),
        ],
        'items': [],
        'datasets': [],
        'header': ['type', 'id', 'pavadinimas'],
        'data': [
            [
                {'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/rinkimai/:source/json/:format/csv'),
            ('JSON', '/rinkimai/:source/json/:format/json'),
        ],
    }


def test_nested_dataset(store, app):
    store.push([
        {
            'type': 'deeply/nested/model/name/:source/nested/dataset/name',
            'id': '42',
            'name': 'Nested One',
        },
    ])

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
        ],
        'items': [],
        'datasets': [],
        'header': ['type', 'id', 'name'],
        'data': [
            [
                {'link': '/deeply/nested/model/name/e2ff1ff0f7d663344abe821582b0908925e5b366/:source/nested/dataset/name', 'value': 'e2ff1ff0'},
                {'link': None, 'value': 'Nested One'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/deeply/nested/model/name/:source/nested/dataset/name/:format/csv'),
            ('JSON', '/deeply/nested/model/name/:source/nested/dataset/name/:format/json'),
        ],
    }


def test_dataset_key(store, app):
    store.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ])

    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json')
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', '/rinkimai/:source/json'),
            ('df6b9e04', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/json'),
        ],
        'datasets': [],
        'items': [],
        'header': [],
        'data': [],
        'row': [
            ('type', {'link': None, 'value': 'rinkimai/:source/json'}),
            ('id', {
                'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json',
                'value': 'df6b9e04ac9e2467690bcad6d9fd673af6e1919b',
            }),
            ('pavadinimas', {'link': None, 'value': 'Rinkimai 1'}),
        ],
    }
