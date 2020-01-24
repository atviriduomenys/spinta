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
                '_id': '356a192b7913b04c54574d18c28d46e6395428ab',
                'title': 'Europe',
            },
            {
                '_type': 'country/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': 'da4b9237bacccdf19c0760cab7aec4a8359010b0',
                'title': 'Lithuania',
                'continent': '356a192b7913b04c54574d18c28d46e6395428ab',
            },
            {
                '_type': 'country/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '77de68daecd823babbb58edb1c8e14d7106e83bb',
                'title': 'Latvia',
                'continent': '356a192b7913b04c54574d18c28d46e6395428ab',
            },
            {
                '_type': 'capital/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '77de68daecd823babbb58edb1c8e14d7106e83bb',
                'title': 'Vilnius',
                'country': 'da4b9237bacccdf19c0760cab7aec4a8359010b0',
            },
            {
                '_type': 'capital/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': '1b6453892473a467d07372d45eb05abc2031647a',
                'title': 'Kaunas',
                'country': 'da4b9237bacccdf19c0760cab7aec4a8359010b0',
            },
            {
                '_type': 'capital/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': 'ac3478d69a3c81fa62e60f5c3696165a4e5e6ac4',
                'title': 'Ryga',
                'country': '77de68daecd823babbb58edb1c8e14d7106e83bb',
            },
        ]
    })

    # XXX: Maybe we should require `search` scope also for linked models? Now,
    #      we only have access to `continent`, but using foreign keys, we can
    #      also access country and continent.
    resp = app.get('/capital/:dataset/dependencies/:resource/continents?select(_id,title,country.title,country.continent.title)&sort(+_id)')
    assert resp.json()['_data'] == [
        {
            '_id': '1b6453892473a467d07372d45eb05abc2031647a',
            'title': 'Kaunas',
            'country.title': 'Lithuania',
            'country.continent.title': 'Europe',
        },
        {
            '_id': '77de68daecd823babbb58edb1c8e14d7106e83bb',
            'title': 'Vilnius',
            'country.title': 'Lithuania',
            'country.continent.title': 'Europe',
        },
        {
            '_id': 'ac3478d69a3c81fa62e60f5c3696165a4e5e6ac4',
            'title': 'Ryga',
            'country.title': 'Latvia',
            'country.continent.title': 'Europe',
        },
    ]
