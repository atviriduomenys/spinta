from typing import Any
from typing import Dict
from typing import List
from unittest.mock import Mock

from spinta import commands
from spinta import spyna
from spinta.components import Context
from spinta.auth import AdminToken
from spinta.testing.client import create_test_client
from spinta.testing.data import encode_page_values_manually, listdata
from spinta.testing.manifest import prepare_manifest, bootstrap_manifest
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from _pytest.fixtures import FixtureRequest

from spinta.testing.utils import get_error_codes


def _prep_context(context: Context):
    context.set('auth.token', AdminToken())


def _prep_conn(context: Context, data: List[Dict[str, Any]]):
    txn = Mock()
    conn = txn.connection.execution_options.return_value = Mock()
    conn.execute.return_value = data
    context.set('transaction', txn)
    return conn


def test_getall(rc: RawConfig):
    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''')
    _prep_context(context)
    conn = _prep_conn(context, [
        {
            '_id': '3aed7394-18da-4c17-ac29-d501d5dd0ed7',
            '_revision': '9f308d61-5401-4bc2-a9da-bc9de85ad91d',
            'country.name': 'Lithuania',
        }
    ])
    model = manifest.models['example/City']
    backend = model.backend
    query = asttoexpr(spyna.parse('select(_id, country.name)'))
    rows = commands.getall(context, model, backend, query=query)
    rows = list(rows)

    assert str(conn.execute.call_args.args[0]) == (
        'SELECT'
        ' "example/City"._id,'
        ' "example/City"._revision,'
        ' "example/Country_1".name AS "country.name" \n'
        'FROM'
        ' "example/City" '
        'LEFT OUTER JOIN "example/Country" AS "example/Country_1"'
        ' ON "example/City"."country._id" = "example/Country_1"._id'
    )
    page = rows[0]['_page']
    assert rows == [
        {
            '_id': '3aed7394-18da-4c17-ac29-d501d5dd0ed7',
            '_page': page,
            '_revision': '9f308d61-5401-4bc2-a9da-bc9de85ad91d',
            '_type': 'example/City',
            'country': {'name': 'Lithuania'},
        },
    ]


def test_getall_pagination_disabled(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            ''', backend=postgresql, request=request)
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


def test_getall_pagination_enabled(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/getall/test      |         |         |         |
              |   |   | Test         |         | value   |         | 
              |   |   |   | value    | integer |         | open    | 
            ''', backend=postgresql, request=request)
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


def test_get_date(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/date/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | date |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/date/test', ['insert', 'getall', 'search'])
    app.post('/example/date/test/Test', json={'date': '2020-01-01'})
    response = app.get(f'/example/date/test/Test')
    json_response = response.json()
    assert len(json_response['_data']) == 1
    assert json_response['_data'][0]['date'] == '2020-01-01'


def test_get_datetime(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/datetime/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | datetime |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/datetime/test', ['insert', 'getall', 'search'])
    app.post('/example/datetime/test/Test', json={'date': '2020-01-01T10:00:10'})
    response = app.get(f'/example/datetime/test/Test')
    json_response = response.json()
    assert len(json_response['_data']) == 1
    assert json_response['_data'][0]['date'] == '2020-01-01T10:00:10'


def test_get_time(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type    | ref     | access  | uri
            example/time/test      |         |         |         |
              |   |   | Test         |         | date   |         | 
              |   |   |   | date    | time |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/time/test', ['insert', 'getall', 'search'])
    app.post('/example/time/test/Test', json={'date': '10:00:10'})
    response = app.get(f'/example/time/test/Test')
    json_response = response.json()
    assert len(json_response['_data']) == 1
    assert json_response['_data'][0]['date'] == '10:00:10'


def test_join_with_base(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
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
    ''', backend=postgresql, request=request)

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


def test_invalid_inherit_check(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
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
    ''', backend=postgresql, request=request)

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
