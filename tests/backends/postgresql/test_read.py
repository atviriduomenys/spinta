import uuid
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.auth import AdminToken
from spinta.cli.helpers.push.utils import get_data_checksum
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import encode_page_values_manually, listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes


def _prep_context(context: Context):
    context.set('auth.token', AdminToken())


@pytest.mark.manifests('internal_sql', 'csv')
def test_getall(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    _prep_context(context)
    app = create_test_client(context)
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_wipe', 'spinta_search', 'spinta_set_meta_fields'])
    lithuania_id = 'd3482081-1c30-43a4-ae6f-faf6a40c954a'
    vilnius_id = '3aed7394-18da-4c17-ac29-d501d5dd0ed7'
    app.post('/example/Country', json={
        '_id': lithuania_id,
        'name': 'Lithuania'
    })
    app.post('/example/City', json={
        '_id': vilnius_id,
        'country': {
            '_id': lithuania_id
        },
        'name': 'Vilnius'
    })
    response = app.get(f'/example/City?select(_id, _revision, _type, country.name)').json()['_data']
    revision = response[0]['_revision']
    assert response == [
        {
            '_id': '3aed7394-18da-4c17-ac29-d501d5dd0ed7',
            '_revision': revision,
            '_type': 'example/City',
            'country': {'name': 'Lithuania'},
        },
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_getall_pagination_disabled(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/getall/test', ['insert', 'getall', 'search'])
    app.post('/example/getall/test/Test', json={'value': 0})
    app.post('/example/getall/test/Test', json={'value': 3})
    resp_2 = app.post('/example/getall/test/Test', json={'value': 410}).json()
    app.post('/example/getall/test/Test', json={'value': 707})
    app.post('/example/getall/test/Test', json={'value': 1000})
    encoded_page = {
        "value": resp_2["value"],
        "_id": resp_2["_id"]
    }
    encoded_page = encode_page_values_manually(encoded_page)
    response = app.get(f'/example/getall/test/Test?page({encoded_page}, disable: true)')
    json_response = response.json()
    assert len(json_response["_data"]) == 5


@pytest.mark.manifests('internal_sql', 'csv')
def test_getall_pagination_enabled(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/getall/test', ['insert', 'getall', 'search'])
    app.post('/example/getall/test/Test', json={'value': 0})
    app.post('/example/getall/test/Test', json={'value': 3})
    resp_2 = app.post('/example/getall/test/Test', json={'value': 410}).json()
    app.post('/example/getall/test/Test', json={'value': 707})
    app.post('/example/getall/test/Test', json={'value': 1000})
    encoded_page = {
        "value": resp_2["value"],
        "_id": resp_2["_id"]
    }
    encoded_page = encode_page_values_manually(encoded_page)
    response = app.get(f'/example/getall/test/Test?page("{encoded_page}")')
    json_response = response.json()
    assert len(json_response["_data"]) == 2


@pytest.mark.manifests('internal_sql', 'csv')
def test_get_date(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/date/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | date |         | open    | 
            ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/date/test', ['insert', 'getall', 'search'])
    app.post('/example/date/test/Test', json={'date': '2020-01-01'})
    response = app.get(f'/example/date/test/Test')
    json_response = response.json()
    assert len(json_response['_data']) == 1
    assert json_response['_data'][0]['date'] == '2020-01-01'


@pytest.mark.manifests('internal_sql', 'csv')
def test_get_datetime(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/datetime/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | datetime |         | open    | 
            ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/datetime/test', ['insert', 'getall', 'search'])
    app.post('/example/datetime/test/Test', json={'date': '2020-01-01T10:00:10'})
    response = app.get(f'/example/datetime/test/Test')
    json_response = response.json()
    assert len(json_response['_data']) == 1
    assert json_response['_data'][0]['date'] == '2020-01-01T10:00:10'


@pytest.mark.manifests('internal_sql', 'csv')
def test_get_time(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/time/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | time |         | open    | 
            ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/time/test', ['insert', 'getall', 'search'])
    app.post('/example/time/test/Test', json={'date': '10:00:10'})
    response = app.get(f'/example/time/test/Test')
    json_response = response.json()
    assert len(json_response['_data']) == 1
    assert json_response['_data'][0]['date'] == '10:00:10'


@pytest.mark.manifests('internal_sql', 'csv')
def test_get_paginate_with_none_simple(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/page/null/simple |         |         |         |
              |   |   | Test         |         | id      | open    | 
              |   |   |   | id       | integer |         |         | 
              |   |   |   | name     | string  |         |         |
            ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/page/null/simple', ['insert', 'getall', 'search'])
    app.post('/example/page/null/simple/Test', json={
        'id': 0,
        'name': 'Test0'
    })
    app.post('/example/page/null/simple/Test', json={
        'id': None,
        'name': 'Test1'
    })
    app.post('/example/page/null/simple/Test', json={
        'id': 1,
        'name': 'Test2'
    })
    app.post('/example/page/null/simple/Test', json={
        'id': 2,
        'name': 'Test3'
    })

    response = app.get(f'/example/page/null/simple/Test?page(size:1)')
    json_response = response.json()
    assert len(json_response['_data']) == 4
    assert listdata(response, 'id', 'name') == [
        (0, 'Test0'),
        (1, 'Test2'),
        (2, 'Test3'),
        (None, 'Test1'),
    ]

    response = app.get(f'/example/page/null/simple/Test?page(size:2)')
    json_response = response.json()
    assert len(json_response['_data']) == 4
    assert listdata(response, 'id', 'name') == [
        (0, 'Test0'),
        (1, 'Test2'),
        (2, 'Test3'),
        (None, 'Test1'),
    ]

    response = app.get(f'/example/page/null/simple/Test?page(size:100)')
    json_response = response.json()
    assert len(json_response['_data']) == 4
    assert listdata(response, 'id', 'name') == [
        (0, 'Test0'),
        (1, 'Test2'),
        (2, 'Test3'),
        (None, 'Test1'),
    ]

    response = app.get(f'/example/page/null/simple/Test?sort(id, name)&page(size:100)')
    json_response = response.json()
    assert len(json_response['_data']) == 4
    assert listdata(response, 'id', 'name') == [
        (0, 'Test0'),
        (1, 'Test2'),
        (2, 'Test3'),
        (None, 'Test1'),
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_get_paginate_with_none_multi_key(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
            d | r | b | m | property | type    | ref      | access  | uri
            example/page/null/multi  |         |          |         |
              |   |   | Test         |         | id, name | open    | 
              |   |   |   | id       | integer |          |         | 
              |   |   |   | name     | string  |          |         |
            ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/page/null/multi', ['insert', 'getall', 'search'])
    app.post('/example/page/null/multi/Test', json={
        'id': 0,
        'name': 'Test0'
    })
    app.post('/example/page/null/multi/Test', json={
        'id': 0,
        'name': 'Test1'
    })
    app.post('/example/page/null/multi/Test', json={
        'id': 0,
        'name': None
    })
    app.post('/example/page/null/multi/Test', json={
        'id': 1,
        'name': 'Test2'
    })
    app.post('/example/page/null/multi/Test', json={
        'id': 2,
        'name': 'Test3'
    })
    app.post('/example/page/null/multi/Test', json={
        'id': None,
        'name': 'Test'
    })
    app.post('/example/page/null/multi/Test', json={
        'id': None,
        'name': 'Test1'
    })
    app.post('/example/page/null/multi/Test', json={
        'id': None,
        'name': None
    })

    response = app.get(f'/example/page/null/multi/Test?page(size:1)')
    json_response = response.json()
    assert len(json_response['_data']) == 8
    assert listdata(response, 'id', 'name') == [
        (0, 'Test0'),
        (0, 'Test1'),
        (0, None),
        (1, 'Test2'),
        (2, 'Test3'),
        (None, 'Test'),
        (None, 'Test1'),
        (None, None),
    ]

    response = app.get(f'/example/page/null/multi/Test?page(size:3)')
    json_response = response.json()
    assert len(json_response['_data']) == 8
    assert listdata(response, 'id', 'name') == [
        (0, 'Test0'),
        (0, 'Test1'),
        (0, None),
        (1, 'Test2'),
        (2, 'Test3'),
        (None, 'Test'),
        (None, 'Test1'),
        (None, None),
    ]

    response = app.get(f'/example/page/null/multi/Test?sort(-id,-name)&page(size:3)')
    json_response = response.json()
    assert len(json_response['_data']) == 8
    assert listdata(response, 'id', 'name') == [
        (0, 'Test0'),
        (0, 'Test1'),
        (0, None),
        (1, 'Test2'),
        (2, 'Test3'),
        (None, 'Test'),
        (None, 'Test1'),
        (None, None),
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_join_with_base(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property   | type                 | ref     | prepare   | access
    datasets/basetest          |                      |         |           |
      |   |   |   |            |                      |         |           |
      |   |   | Place          |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       | string               |         |           | open
      |   |   |   | koord      | geometry(point,4326) |         |           | open
      |   |   |   |            |                      |         |           |
      |   | Place              |                      | name    |           |
      |   |   |   |            |                      |         |           |
      |   |   | Location       |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | koord      |                      |         |           | open
      |   |   |   | population | integer              |         |           | open
      |   |   |   | type       | string               |         |           | open
      |   |   |   |            | enum                 |         | "city"    |
      |   |   |   |            |                      |         | "country" |
      |   |   |   |            |                      |         |           |
      |   | Location           |                      | name    |           |
      |   |   | Test           |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   |            |                      |         |           |
      |   |   | Country        |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population |                      |         |           | open
      |   |   |   |            |                      |         |           |
      |   |   | City           |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population |                      |         |           | open
      |   |   |   | koord      |                      |         |           | open
      |   |   |   | type       |                      |         |           | open
      |   |   |   | country    | ref                  | Country |           | open
      |   |   |   | testfk     | ref                  | Test    |           | open
      |   |   |   |            |                      |         |           |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    app = create_test_client(context)
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_wipe', 'spinta_search', 'spinta_set_meta_fields'])
    LTU = "d55e65c6-97c9-4cd3-99ff-ae34e268289b"
    VLN = "2074d66e-0dfd-4233-b1ec-199abc994d0c"
    TST = "2074d66e-0dfd-4233-b1ec-199abc994d0e"

    resp = app.post('/datasets/basetest/Place', json={
        '_id': LTU,
        'id': 1,
        'name': 'Lithuania',
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/Location', json={
        '_id': LTU,
        'id': 1,
        'population': 2862380,
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/Country', json={
        '_id': LTU,
        'id': 1,
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/Place', json={
        '_id': VLN,
        'id': 2,
        'name': 'Vilnius',
        'koord': "SRID=4326;POINT (54.68677 25.29067)"
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/Location', json={
        '_id': VLN,
        'id': 2,
        'population': 625349,
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/Place', json={
        '_id': TST,
        'id': 3,
        'name': 'TestFK',
        'koord': "SRID=4326;POINT (54.68677 25.29067)"
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/Location', json={
        '_id': TST,
        'id': 3,
        'population': 625349,
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/Test', json={
        '_id': TST,
        'id': 3,
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/basetest/City', json={
        '_id': VLN,
        'id': 2,
        'country': {'_id': LTU},
        'testfk': {'_id': TST}
    })
    assert resp.status_code == 201

    resp = app.get('/datasets/basetest/Location?select(_id,id,name,population,type)')
    assert resp.status_code == 200
    assert listdata(resp, 'name', sort='name') == ['Lithuania', 'TestFK', 'Vilnius']

    resp = app.get('/datasets/basetest/City?select(id,name,country.name,testfk.name)')
    assert resp.status_code == 200
    assert listdata(resp, 'name', 'country.name', 'testfk.name')[0] == ('Vilnius', 'Lithuania', 'TestFK')

    resp = app.get('/datasets/basetest/City?select(_id,id,name,population,type,koord)')
    assert resp.status_code == 200
    assert listdata(resp, 'name','population', 'koord', sort='name')[0] == ('Vilnius', 625349, 'POINT (54.68677 25.29067)')


@pytest.mark.manifests('internal_sql', 'csv')
def test_invalid_inherit_check(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property   | type                 | ref     | prepare   | access
    datasets/invalid/base          |                      |         |           |
      |   |   |   |            |                      |         |           |
      |   |   | Place          |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       | string               |         |           | open
      |   |   |   | koord      | geometry(point,4326) |         |           | open
      |   |   |   |            |                      |         |           |
      |   | Place              |                      | name    |           |
      |   |   |   |            |                      |         |           |
      |   |   | Location       |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population | integer              |         |           | open
      |   |   |   |            |                      |         |           |
      |   | Location           |                      | name    |           |
      |   |   |   |            |                      |         |           |
      |   |   | Country        |                      | id      |           |
      |   |   |   | id         | integer              |         |           | open
      |   |   |   | name       |                      |         |           | open
      |   |   |   | population |                      |         |           | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    app = create_test_client(context)
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_wipe', 'spinta_search', 'spinta_set_meta_fields'])
    LTU = "d55e65c6-97c9-4cd3-99ff-ae34e268289b"

    resp = app.post('/datasets/invalid/base/Place', json={
        '_id': LTU,
        'id': 1,
        'name': 'Lithuania',
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/invalid/base/Location', json={
        '_id': LTU,
        'id': 1,
        'population': 2862380,
    })
    assert resp.status_code == 201

    resp = app.post('/datasets/invalid/base/Country', json={
        '_id': LTU,
        'id': 1,
        'name': 'Lietuva'
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InheritPropertyValueMissmatch"]

    resp = app.post('/datasets/invalid/base/Country', json={
        '_id': LTU,
        'id': 1,
        'name': 'Lithuania'
    })
    assert resp.status_code == 201


@pytest.mark.manifests('internal_sql', 'csv')
def test_checksum_result(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property   | type                 | ref      | prepare   | access | level
    datasets/result/checksum   |                      |          |           |        |
      |   |   | Country        |                      | id, name |           |        |
      |   |   |   | id         | integer              |          |           | open   |
      |   |   |   | name       | string               |          |           | open   |
      |   |   |   | population | integer              |          |           | open   |
      |   |   |   |            |                      |          |           |        |
      |   |   | City           |                      | id       |           |        |
      |   |   |   | id         | integer              |          |           | open   |
      |   |   |   | name       | string               |          |           | open   |
      |   |   |   | country    | ref                  | Country  |           | open   |
      |   |   |   | add        | object               |          |           | open   |
      |   |   |   | add.name   | string               |          |           | open   |
      |   |   |   | add.add    | object               |          |           | open   |
      |   |   |   | add.add.c  | ref                  | Country  |           | open   | 3
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )

    app = create_test_client(context)
    app.authorize(['spinta_insert', 'spinta_getall', 'spinta_wipe', 'spinta_search', 'spinta_set_meta_fields'])
    lt_id = str(uuid.uuid4())
    lv_id = str(uuid.uuid4())

    country_data = {
        lt_id: {
            'id': 0,
            'name': 'Lithuania',
            'population': 1000
        },
        lv_id: {
            'id': 1,
            'name': 'Latvia',
            'population': 500
        }
    }

    resp = app.post('/datasets/result/checksum/Country', json={
        '_id': lt_id,
        **country_data[lt_id]
    })
    assert resp.status_code == 201
    resp = app.post('/datasets/result/checksum/Country', json={
        '_id': lv_id,
        **country_data[lv_id]
    })
    assert resp.status_code == 201

    vilnius_id = str(uuid.uuid4())
    kaunas_id = str(uuid.uuid4())
    riga_id = str(uuid.uuid4())
    liepaja_id = str(uuid.uuid4())

    city_data = {
        vilnius_id: {
            'id': 0,
            'name': 'Vilnius',
            'country': {
                '_id': lt_id
            },
            'add': {
                'name': "VLN",
                'add': {
                    'c': {
                        'id': 0,
                        'name': 'Lithuania'
                    }
                }
            }
        },
        kaunas_id: {
            'id': 1,
            'name': 'Kaunas',
            'country': {
                '_id': lt_id
            },
            'add': {
                'name': "KN",
                'add': {
                    'c': {
                        'id': 0,
                        'name': 'Lithuania'
                    }
                }
            }
        },
        riga_id: {
            'id': 2,
            'name': 'Riga',
            'country': {
                '_id': lv_id
            },
            'add': {
                'name': "RG",
                'add': {
                    'c': {
                        'id': 1,
                        'name': 'Latvia'
                    }
                }
            }
        },
        liepaja_id: {
            'id': 3,
            'name': 'Liepaja',
            'country': {
                '_id': lv_id
            },
            'add': {
                'name': "LP",
                'add': {
                    'c': {
                        'id': 1,
                        'name': 'Latvia'
                    }
                }
            }
        }
    }

    resp = app.post('/datasets/result/checksum/City', json={
        '_id': vilnius_id,
        **city_data[vilnius_id]
    })
    assert resp.status_code == 201
    resp = app.post('/datasets/result/checksum/City', json={
        '_id': kaunas_id,
        **city_data[kaunas_id]
    })
    assert resp.status_code == 201
    resp = app.post('/datasets/result/checksum/City', json={
        '_id': riga_id,
        **city_data[riga_id]
    })
    assert resp.status_code == 201
    resp = app.post('/datasets/result/checksum/City', json={
        '_id': liepaja_id,
        **city_data[liepaja_id]
    })
    assert resp.status_code == 201

    resp = app.get('/datasets/result/checksum/Country?select(_id, id, checksum())')
    assert resp.status_code == 200
    assert listdata(resp, '_id', 'id', 'checksum()', sort='id', full=True) == [
        {
            '_id': lt_id,
            'id': 0,
            'checksum()': get_data_checksum(
                country_data[lt_id]
            )
        },
        {
            '_id': lv_id,
            'id': 1,
            'checksum()': get_data_checksum(
                country_data[lv_id]
            )
        },
    ]

    resp = app.get('/datasets/result/checksum/City?select(_id, id, checksum())')
    assert resp.status_code == 200
    assert listdata(resp, '_id', 'id', 'checksum()', sort='id', full=True) == [
        {
            '_id': vilnius_id,
            'id': 0,
            'checksum()': get_data_checksum(
                city_data[vilnius_id]
            )
        },
        {
            '_id': kaunas_id,
            'id': 1,
            'checksum()': get_data_checksum(
                city_data[kaunas_id]
            )
        },
        {
            '_id': riga_id,
            'id': 2,
            'checksum()': get_data_checksum(
                city_data[riga_id]
            )
        },
        {
            '_id': liepaja_id,
            'id': 3,
            'checksum()': get_data_checksum(
                city_data[liepaja_id]
            )
        },
    ]
