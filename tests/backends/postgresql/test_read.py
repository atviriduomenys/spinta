from typing import Any
from typing import Dict
from typing import List
from unittest.mock import Mock

from spinta import commands
from spinta import spyna
from spinta.components import Context, decode_page_values
from spinta.auth import AdminToken
from spinta.testing.client import create_test_client
from spinta.testing.data import encode_page_values_manually
from spinta.testing.manifest import prepare_manifest, bootstrap_manifest
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from _pytest.fixtures import FixtureRequest


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
    encoded_page = encode_page_values_manually(encoded_page).decode("ascii")
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
    encoded_page = encode_page_values_manually(encoded_page).decode("ascii")
    response = app.get(f'/example/getall/test/Test?page("{encoded_page}")')
    json_response = response.json()
    assert len(json_response["_data"]) == 2
