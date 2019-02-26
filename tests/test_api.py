def test_app(app):
    resp = app.get('/')
    assert resp.status_code == 200
    assert resp.template.name == 'base.html'
    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
        ],
        'items': [
            ('country', '/country'),
            ('org', '/org'),
            ('rinkimai', '/rinkimai'),
        ],
        'datasets': [],
    }

    resp = app.get('/rinkimai')
    assert resp.status_code == 200
    assert resp.template.name == 'base.html'

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
            ('xlsx', '/rinkimai/xlsx'),
            ('json', '/rinkimai/json'),
        ],
    }
