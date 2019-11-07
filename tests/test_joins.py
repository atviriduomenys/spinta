def test_select_with_joins(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('continent/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('country/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('capital/:dataset/dependencies/:resource/continents', ['insert', 'search'])

    app.post('/', json={
        '_data': [
            {
                '_type': 'continent/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '1',
                'title': 'Europe',
            },
            {
                '_type': 'country/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '2',
                'title': 'Lithuania',
                'continent': '1',
            },
            {
                '_type': 'capital/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '3',
                'title': 'Vilnius',
                'country': '2',
            },
        ]
    })

    # XXX: Maybe we should require `search` scope also for linked models? Now,
    #      we only have access to `continent`, but using foreign keys, we can
    #      also access country and continent.
    resp = app.get('/capital/:dataset/dependencies/:resource/continents?select(_id,title,country.title,country.continent.title)')
    assert resp.json() == {
        '_data': [
            {
                'country.continent.title': 'Europe',
                'country.title': 'Lithuania',
                'title': 'Vilnius',
                '_id': '3',
            },
        ],
    }
