import datetime


def test_export_json(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REV')

    context.push([
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': '2',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            'type': 'country/:ds/csv/:rs/countries',
            'id': '2',
            'code': 'lv',
            'title': 'Latvia',
        },
    ])

    app.authorize([
        'spinta_country_ds_csv_rs_countries_getall',
        'spinta_country_ds_csv_rs_countries_search',
        'spinta_country_ds_csv_rs_countries_getone',
        'spinta_country_ds_csv_rs_countries_changes',
    ])

    assert app.get('country/:ds/csv/:rs/countries/:sort/code/:format/json').json() == {
        'data': [
            {
                'code': 'lt',
                'id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
                'revision': 'REV',
                'title': 'Lithuania',
                'type': 'country/:ds/csv/:rs/countries',
            },
            {
                'code': 'lv',
                'id': '2',
                'revision': 'REV',
                'title': 'Latvia',
                'type': 'country/:ds/csv/:rs/countries',
            },
        ],
    }

    assert app.get('country/69a33b149af7a7eeb25026c8cdc09187477ffe21/:ds/csv/:rs/countries/:format/json').json() == {
        'code': 'lt',
        'id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
        'revision': 'REV',
        'title': 'Lithuania',
        'type': 'country/:ds/csv/:rs/countries',
    }

    changes = app.get('country/:ds/csv/:rs/countries/:changes/:format/json').json()['data']
    assert changes == [
        {
            'change_id': changes[0]['change_id'],
            'transaction_id': changes[0]['transaction_id'],
            'datetime': '2019-03-06T16:15:00.816308',
            'action': 'insert',
            'id': '69a33b149af7a7eeb25026c8cdc09187477ffe21',
            'change': {'code': 'lt', 'title': 'Lithuania'},
        },
        {
            'change_id': changes[1]['change_id'],
            'transaction_id': changes[1]['transaction_id'],
            'datetime': '2019-03-06T16:15:00.816308',
            'action': 'insert',
            'id': '2',
            'change': {'code': 'lv', 'title': 'LATVIA'},
        },
        {
            'change_id': changes[2]['change_id'],
            'transaction_id': changes[2]['transaction_id'],
            'datetime': '2019-03-06T16:15:00.816308',
            'action': 'patch',
            'id': '2',
            'change': {'title': 'Latvia'},
        },
    ]

    assert app.get('country/:ds/csv/:rs/countries/:changes/1000/:format/json').json() == {
        'data': []
    }
