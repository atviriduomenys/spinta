def _get_data_table(context: dict):
    return [tuple(context['header'])] + [
        tuple(cell['value'] for cell in row)
        for row in context['data']
    ]


def test_select_with_joins(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('continent/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('country/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('capital/:dataset/dependencies/:resource/continents', ['insert', 'search'])

    resp = app.post('/', json={
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
    assert resp.status_code == 200, resp.json()

    resp = app.get(
        '/capital/:dataset/dependencies/:resource/continents?'
        'select(_id,title,country.title,country.continent.title)&'
        'format(html)'
    )
    assert _get_data_table(resp.context) == [
        ('_id', 'title', 'country.title', 'country.continent.title'),
        ('3', 'Vilnius', 'Lithuania', 'Europe'),
    ]
