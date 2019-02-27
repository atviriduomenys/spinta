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
        'data': [],
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
        'data': [],
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
        'data': [
            ['id', 'pavadinimas'],
            ['Rinkimai 1', 'Rinkimai 1'],
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
        'data': [
            ['id', 'name'],
            ['42', 'Nested One'],
        ],
    }
