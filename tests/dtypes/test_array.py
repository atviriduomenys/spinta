from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from _pytest.fixtures import FixtureRequest


def test_getall_level4(rc, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type    | ref     | access | level
    example/lvl4             |         |         |        |
      |   |   | Country      |         |         |        |
      |   |   |   | name     | string  |         | open   |
      |   |   |   | cities[] | ref     | City    | open   | 4
      |   |   | City         |         | name    |        |
      |   |   |   | name     | string  |         | open   |
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields', 'spinta_getone'])
    app.authmodel('example/lvl4/City', ['insert'])
    vilnius_id = 'e06708e5-7857-46f5-95e7-36fa7bcf1c1c'
    kaunas_id = '214ec691-dea6-4a0b-8f51-ee61eaa6117e'
    app.post('/example/lvl4/City', json={
        '_id': vilnius_id,
        'name': 'Vilnius'
    })
    app.post('/example/lvl4/City', json={
        '_id': kaunas_id,
        'name': 'Kaunas'
    })

    lietuva_id = '6ffebdd7-2c5f-4e1f-b38b-e7a5c35e7216'
    app.authmodel('example/lvl4/Country', ['insert', 'getall', 'search'])
    app.post('/example/lvl4/Country', json={
        '_id': lietuva_id,
        'name': 'Lietuva',
        'cities': [
            {'_id': vilnius_id},
            {'_id': kaunas_id}
        ]
    })

    result = app.get('/example/lvl4/Country')
    assert result.json()['_data'][0]['cities'] == []

    result = app.get('/example/lvl4/Country?expand()')
    assert result.json()['_data'][0]['cities'] == [
        {'_id': vilnius_id},
        {'_id': kaunas_id},
    ]

    result = app.get(f'/example/lvl4/Country/{lietuva_id}')
    assert result.json()['cities'] == [
        {'_id': vilnius_id},
        {'_id': kaunas_id},
    ]


def test_getall_level3(rc, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type    | ref     | access | level
    example/lvl3             |         |         |        |
      |   |   | Country      |         |         |        |
      |   |   |   | name     | string  |         | open   |
      |   |   |   | cities[] | ref     | City    | open   | 3
      |   |   | City         |         | name    |        |
      |   |   |   | name     | string  |         | open   |
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields', 'spinta_getone'])
    app.authmodel('example/lvl3/City', ['insert'])
    vilnius_id = 'e06708e5-7857-46f5-95e7-36fa7bcf1c1c'
    kaunas_id = '214ec691-dea6-4a0b-8f51-ee61eaa6117e'
    app.post('/example/lvl3/City', json={
        '_id': vilnius_id,
        'name': 'Vilnius'
    })
    app.post('/example/lvl3/City', json={
        '_id': kaunas_id,
        'name': 'Kaunas'
    })

    lietuva_id = '6ffebdd7-2c5f-4e1f-b38b-e7a5c35e7216'
    app.authmodel('example/lvl3/Country', ['insert', 'getall', 'search'])
    app.post('/example/lvl3/Country', json={
        '_id': lietuva_id,
        'name': 'Lietuva',
        'cities': [
            {'name': 'Vilnius'},
            {'name': 'Kaunas'}
        ]
    })

    result = app.get('/example/lvl3/Country')
    assert result.json()['_data'][0]['cities'] == []

    result = app.get('/example/lvl3/Country?expand()')
    assert result.json()['_data'][0]['cities'] == [
        {'name': 'Vilnius'},
        {'name': 'Kaunas'},
    ]

    result = app.get(f'/example/lvl3/Country/{lietuva_id}')
    assert result.json()['cities'] == [
        {'name': 'Vilnius'},
        {'name': 'Kaunas'},
    ]


def test_getall_simple_type(rc, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type    | ref     | access | level
    example/simple           |         |         |        |
      |   |   | Country      |         |         |        |
      |   |   |   | name     | string  |         | open   |
      |   |   |   | cities[] | string  |         | open   |
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields', 'spinta_getone'])

    lietuva_id = '6ffebdd7-2c5f-4e1f-b38b-e7a5c35e7216'
    app.authmodel('example/simple/Country', ['insert', 'getall', 'search'])
    app.post('/example/simple/Country', json={
        '_id': lietuva_id,
        'name': 'Lietuva',
        'cities': [
            'Vilnius',
            'Kaunas'
        ]
    })

    result = app.get(f'/example/simple/Country/{lietuva_id}')
    assert result.json()['cities'] == [
        'Vilnius',
        'Kaunas'
    ]


def test_array_shortcut_inherit_access_open(rc, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access
    example/dtypes/array/open        |         |          |
                                |         |          |
      |   |   | Language        |         |          |
      |   |   |   | name        | string  |          | open
                                |         |          |
      |   |   | Country         |         |          |
      |   |   |   | name        | string  |          | open
      |   |   |   | languages[] | ref     | Language | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/array/open', ['insert', 'getone', 'getall'])

    LIT = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    app.post('example/dtypes/array/open/Language', json={
        '_id': LIT,
        'name': 'Lithuanian',
    })

    LT = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    app.post('example/dtypes/array/open/Country', json={
        '_id': LT,
        'name': 'Lithuania',
        'languages': [
            {'_id': LIT}
        ]
    })

    result = app.get('/example/dtypes/array/open/Country')
    assert result.json()['_data'][0]['languages'] == []

    result = app.get('/example/dtypes/array/open/Country?expand()')
    assert result.json()['_data'][0]['languages'] == [
        {'_id': LIT},
    ]

    result = app.get(f'/example/dtypes/array/open/Country/{LT}')
    assert result.json()['languages'] == [
        {'_id': LIT},
    ]


def test_array_shortcut_inherit_access_private(rc, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access
    example/dtypes/array/private        |         |          |
                                |         |          |
      |   |   | Language        |         |          |
      |   |   |   | name        | string  |          | open
                                |         |          |
      |   |   | Country         |         |          |
      |   |   |   | name        | string  |          | open
      |   |   |   | languages[] | ref     | Language | private
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/array/private', ['insert', 'getone', 'getall'])

    LIT = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    app.post('example/dtypes/array/private/Language', json={
        '_id': LIT,
        'name': 'Lithuanian',
    })

    LT = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    app.post('example/dtypes/array/private/Country', json={
        '_id': LT,
        'name': 'Lithuania',
        'languages': [
            {'_id': LIT}
        ]
    })
    result = app.get('/example/dtypes/array/private/Country')
    assert 'languages' not in result.json()['_data'][0]

    result = app.get('/example/dtypes/array/private/Country?expand()')
    assert 'languages' not in result.json()['_data'][0]

    result = app.get(f'/example/dtypes/array/private/Country/{LT}')
    assert 'languages' not in result.json()
