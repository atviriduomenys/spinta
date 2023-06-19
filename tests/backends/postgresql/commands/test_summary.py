from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from _pytest.fixtures import FixtureRequest


def float_equals(a: float, b: float, epsilon=1e-9):
    return abs(a - b) < epsilon


def dict_equals(dict1: dict, dict2: dict, epsilon=1e-9):
    if len(dict1) != len(dict2):
        return False

    for key in dict1:
        if key not in dict2:
            return False

        value1 = dict1[key]
        value2 = dict2[key]

        if isinstance(value1, float) and isinstance(value2, float):
            if not float_equals(value1, value2, epsilon):
                return False
        elif value1 != value2:
            return False

    return True


def test_summary_integer_no_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/integer', ['insert', 'getall', 'search'])
    app.post('/example/summary/integer/Test', json={'value': 0})
    app.post('/example/summary/integer/Test', json={'value': 3})
    resp_410 = app.post('/example/summary/integer/Test', json={'value': 410})
    resp_707 = app.post('/example/summary/integer/Test', json={'value': 707})
    resp_1000 = app.post('/example/summary/integer/Test', json={'value': 1000})
    response = app.get('/example/summary/integer/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': 5,
        'count': 2,
        '_type': 'example/summary/integer/Test'
    })
    assert dict_equals(json_response["_data"][40], {
        'bin': 405,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        '_id': resp_410.json()["_id"],
    })
    assert dict_equals(json_response["_data"][70], {
        'bin': 705,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        '_id': resp_707.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': 995,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        '_id': resp_1000.json()["_id"],
    })


def test_summary_integer_with_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/integer', ['insert', 'getall', 'search'])
    resp_0 = app.post('/example/summary/integer/Test', json={'value': 0})
    resp_3 = app.post('/example/summary/integer/Test', json={'value': 3})
    resp_21 = app.post('/example/summary/integer/Test', json={'value': 21})
    resp_55 = app.post('/example/summary/integer/Test', json={'value': 55})
    resp_64 = app.post('/example/summary/integer/Test', json={'value': 64})
    response = app.get('/example/summary/integer/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': 0.32,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        "_id": resp_0.json()["_id"]
    })
    assert dict_equals(json_response["_data"][4], {
        'bin': 2.88,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        '_id': resp_3.json()["_id"],
    })
    assert dict_equals(json_response["_data"][32], {
        'bin': 20.8,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        '_id': resp_21.json()["_id"],
    })
    assert dict_equals(json_response["_data"][85], {
        'bin': 54.72,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        '_id': resp_55.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': 63.68,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        '_id': resp_64.json()["_id"],
    })


def test_summary_integer_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/integer', ['insert', 'getall', 'search'])
    app.post('/example/summary/integer/Test', json={})
    response = app.get('/example/summary/integer/Test/:summary/value')
    json_response = response.json()
    assert json_response["_data"] == []


def test_summary_integer_single_item(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/integer', ['insert', 'getall', 'search'])
    resp_64 = app.post('/example/summary/integer/Test', json={'value': 64})
    response = app.get('/example/summary/integer/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': 64.5,
        'count': 1,
        '_type': 'example/summary/integer/Test',
        "_id": resp_64.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': 163.5,
        'count': 0,
        '_type': 'example/summary/integer/Test'
    })


def test_summary_number_no_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/number', ['insert', 'getall', 'search'])
    app.post('/example/summary/number/Test', json={'value': 0.0})
    app.post('/example/summary/number/Test', json={'value': 0.003})
    resp_410 = app.post('/example/summary/number/Test', json={'value': 0.410})
    resp_707 = app.post('/example/summary/number/Test', json={'value': 0.707})
    resp_1000 = app.post('/example/summary/number/Test', json={'value': 1.0})
    response = app.get('/example/summary/number/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': 0.005,
        'count': 2,
        '_type': 'example/summary/number/Test'
    })
    assert dict_equals(json_response["_data"][40], {
        'bin': 0.405,
        'count': 1,
        '_type': 'example/summary/number/Test',
        '_id': resp_410.json()["_id"],
    })
    assert dict_equals(json_response["_data"][70], {
        'bin': 0.705,
        'count': 1,
        '_type': 'example/summary/number/Test',
        '_id': resp_707.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': 0.995,
        'count': 1,
        '_type': 'example/summary/number/Test',
        '_id': resp_1000.json()["_id"],
    })


def test_summary_number_with_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/number', ['insert', 'getall', 'search'])
    resp_0 = app.post('/example/summary/number/Test', json={'value': 0.0})
    resp_3 = app.post('/example/summary/number/Test', json={'value': 0.03})
    resp_21 = app.post('/example/summary/number/Test', json={'value': 0.21})
    resp_55 = app.post('/example/summary/number/Test', json={'value': 0.55})
    resp_64 = app.post('/example/summary/number/Test', json={'value': 0.64})
    response = app.get('/example/summary/number/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': 0.0032,
        'count': 1,
        '_type': 'example/summary/number/Test',
        "_id": resp_0.json()["_id"]
    })
    assert dict_equals(json_response["_data"][4], {
        'bin': 0.0288,
        'count': 1,
        '_type': 'example/summary/number/Test',
        '_id': resp_3.json()["_id"],
    })
    assert dict_equals(json_response["_data"][32], {
        'bin': 0.208,
        'count': 1,
        '_type': 'example/summary/number/Test',
        '_id': resp_21.json()["_id"],
    })
    assert dict_equals(json_response["_data"][85], {
        'bin': 0.5472,
        'count': 1,
        '_type': 'example/summary/number/Test',
        '_id': resp_55.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': 0.6368,
        'count': 1,
        '_type': 'example/summary/number/Test',
        '_id': resp_64.json()["_id"],
    })


def test_summary_number_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/number', ['insert', 'getall', 'search'])
    app.post('/example/summary/number/Test', json={})
    response = app.get('/example/summary/number/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 0


def test_summary_number_single_item(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/number', ['insert', 'getall', 'search'])
    resp_5 = app.post('/example/summary/number/Test', json={'value': 5})
    response = app.get('/example/summary/number/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': 5.5,
        'count': 1,
        '_type': 'example/summary/number/Test',
        "_id": resp_5.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': 104.5,
        'count': 0,
        '_type': 'example/summary/number/Test'
    })


def test_summary_boolean(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/boolean    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | boolean |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/boolean', ['insert', 'getall', 'search'])
    app.post('/example/summary/boolean/Test', json={'value': True})
    app.post('/example/summary/boolean/Test', json={'value': True})
    app.post('/example/summary/boolean/Test', json={'value': True})
    resp = app.post('/example/summary/boolean/Test', json={'value': False})
    response = app.get('/example/summary/boolean/Test/:summary/value')
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(json_response["_data"][0], {
        'bin': False,
        'count': 1,
        '_type': 'example/summary/boolean/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][1], {
        'bin': True,
        'count': 3,
        '_type': 'example/summary/boolean/Test'
    })


def test_summary_boolean_missing(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/boolean    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | boolean |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/boolean', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/boolean/Test', json={'value': False})
    response = app.get('/example/summary/boolean/Test/:summary/value')
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(json_response["_data"][0], {
        'bin': False,
        'count': 1,
        '_type': 'example/summary/boolean/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][1], {
        'bin': True,
        'count': 0,
        '_type': 'example/summary/boolean/Test'
    })


def test_summary_boolean_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/boolean    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | boolean |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/boolean', ['insert', 'getall', 'search'])
    app.post('/example/summary/boolean/Test', json={})
    response = app.get('/example/summary/boolean/Test/:summary/value')
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(json_response["_data"][0], {
        'bin': False,
        'count': 0,
        '_type': 'example/summary/boolean/Test'
    })
    assert dict_equals(json_response["_data"][1], {
        'bin': True,
        'count': 0,
        '_type': 'example/summary/boolean/Test'
    })


def test_summary_string_enum_inside(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
              |   |   |   |          | enum   |         | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/string', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/string/Test', json={"value": "TEST1"})
    app.post('/example/summary/string/Test', json={"value": "TEST2"})
    app.post('/example/summary/string/Test', json={"value": "TEST2"})
    app.post('/example/summary/string/Test', json={"value": "TEST3"})
    app.post('/example/summary/string/Test', json={"value": "TEST3"})
    app.post('/example/summary/string/Test', json={"value": "TEST3"})
    response = app.get('/example/summary/string/Test/:summary/value')
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {
        'bin': "TEST1",
        'count': 1,
        '_type': 'example/summary/string/Test',
        '_id': resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][1], {
        'bin': "TEST2",
        'count': 2,
        '_type': 'example/summary/string/Test'
    })
    assert dict_equals(json_response["_data"][2], {
        'bin': "TEST3",
        'count': 3,
        '_type': 'example/summary/string/Test'
    })


def test_summary_string_enum_outside(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   |   |          | enum   | test    | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string | test    |        |         | open   |

            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/string', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/string/Test', json={"value": "TEST1"})
    app.post('/example/summary/string/Test', json={"value": "TEST2"})
    app.post('/example/summary/string/Test', json={"value": "TEST2"})
    app.post('/example/summary/string/Test', json={"value": "TEST3"})
    app.post('/example/summary/string/Test', json={"value": "TEST3"})
    app.post('/example/summary/string/Test', json={"value": "TEST3"})
    response = app.get('/example/summary/string/Test/:summary/value')
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {
        'bin': "TEST1",
        'count': 1,
        '_type': 'example/summary/string/Test',
        '_id': resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][1], {
        'bin': "TEST2",
        'count': 2,
        '_type': 'example/summary/string/Test'
    })
    assert dict_equals(json_response["_data"][2], {
        'bin': "TEST3",
        'count': 3,
        '_type': 'example/summary/string/Test'
    })


def test_summary_string_missing(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
              |   |   |   |          | enum   |         | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/string', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/string/Test', json={"value": "TEST1"})
    app.post('/example/summary/string/Test', json={"value": "TEST2"})
    app.post('/example/summary/string/Test', json={"value": "TEST2"})
    response = app.get('/example/summary/string/Test/:summary/value')
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {
        'bin': "TEST1",
        'count': 1,
        '_type': 'example/summary/string/Test',
        '_id': resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][1], {
        'bin': "TEST2",
        'count': 2,
        '_type': 'example/summary/string/Test'
    })
    assert dict_equals(json_response["_data"][2], {
        'bin': "TEST3",
        'count': 0,
        '_type': 'example/summary/string/Test'
    })


def test_summary_string_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
              |   |   |   |          | enum   |         | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/string', ['insert', 'getall', 'search'])
    response = app.get('/example/summary/string/Test/:summary/value')
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {
        'bin': "TEST1",
        'count': 0,
        '_type': 'example/summary/string/Test'
    })
    assert dict_equals(json_response["_data"][1], {
        'bin': "TEST2",
        'count': 0,
        '_type': 'example/summary/string/Test'
    })
    assert dict_equals(json_response["_data"][2], {
        'bin': "TEST3",
        'count': 0,
        '_type': 'example/summary/string/Test'
    })


def test_summary_string_no_enum(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/string', ['insert', 'getall', 'search'])
    app.post('/example/summary/string/Test', json={"value": "TEST2"})
    response = app.get('/example/summary/string/Test/:summary/value')

    assert response.status_code == 500


def test_summary_time(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time |         | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    app.post('/example/summary/time/Test', json={'value': '10:00:00'})
    app.post('/example/summary/time/Test', json={'value': '10:00:05'})
    resp_middle = app.post('/example/summary/time/Test', json={'value': '14:08:03'})
    resp_last = app.post('/example/summary/time/Test', json={'value': '22:12:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '10:03:39.600000',
        'count': 2,
        '_type': 'example/summary/time/Test'
    })
    assert dict_equals(json_response["_data"][33], {
        'bin': '14:05:13.200000',
        'count': 1,
        '_type': 'example/summary/time/Test',
        '_id': resp_middle.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '22:08:20.400000',
        'count': 1,
        '_type': 'example/summary/time/Test',
        '_id': resp_last.json()["_id"],
    })


def test_summary_time_single_given_no_units_minutes(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time |   | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '18:00:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '18:00:30',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '19:39:30',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_no_units_seconds(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time |   | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '23:00:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '23:00:00.500000',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '23:01:39.500000',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_no_units_milliseconds(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time |   | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '23:59:59'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '23:59:59.000500',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '23:59:59.099500',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_custom_units_minutes(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 5T | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '10:00:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '10:02:30',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '18:17:30',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_custom_units_minutes_ignore(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 5T | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '22:00:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '22:00:30',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '23:39:30',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_custom_units_seconds(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 15S | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '10:00:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '10:00:07.500000',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '10:24:52.500000',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_custom_units_seconds_ignore(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 15S | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '23:50:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '23:50:00.500000',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '23:51:39.500000',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_custom_units_milliseconds(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 200L | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '10:00:00'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '10:00:00.100000',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '10:00:19.900000',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_single_given_custom_units_milliseconds_ignore(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 200L | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/time/Test', json={'value': '23:59:59'})
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '23:59:59.000500',
        'count': 1,
        '_type': 'example/summary/time/Test',
        "_id": resp.json()["_id"]
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '23:59:59.099500',
        'count': 0,
        '_type': 'example/summary/time/Test'
    })


def test_summary_time_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 200L | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/time', ['insert', 'getall', 'search'])
    response = app.get('/example/summary/time/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 0
    assert json_response["_data"] == []


def test_summary_datetime(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime |   | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/datetime', ['insert', 'getall', 'search'])
    app.post('/example/summary/datetime/Test', json={'value': '2023-01-01T10:00:00'})
    app.post('/example/summary/datetime/Test', json={'value': '2023-01-02T10:00:05'})
    resp_middle = app.post('/example/summary/datetime/Test', json={'value': '2023-09-12T14:08:03'})
    resp_last = app.post('/example/summary/datetime/Test', json={'value': '2023-12-29T22:12:00'})
    response = app.get('/example/summary/datetime/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '2023-01-03 05:30:03.600000',
        'count': 2,
        '_type': 'example/summary/datetime/Test'
    })
    assert dict_equals(json_response["_data"][70], {
        'bin': '2023-09-13 23:38:27.600000',
        'count': 1,
        '_type': 'example/summary/datetime/Test',
        '_id': resp_middle.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '2023-12-28 02:41:56.400000',
        'count': 1,
        '_type': 'example/summary/datetime/Test',
        '_id': resp_last.json()["_id"],
    })


def test_summary_datetime_single_given_no_units(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime |   | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/datetime', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/datetime/Test', json={'value': '2023-01-01T10:00:00'})
    response = app.get('/example/summary/datetime/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '2023-01-01 22:00:00',
        'count': 1,
        '_type': 'example/summary/datetime/Test',
        '_id': resp.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '2023-04-10 22:00:00',
        'count': 0,
        '_type': 'example/summary/datetime/Test',
    })


def test_summary_datetime_single_given_custom_units_years(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime | 10Y | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/datetime', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/datetime/Test', json={'value': '2023-01-01T00:00:00'})
    response = app.get('/example/summary/datetime/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '2028-01-01 05:02:24',
        'count': 1,
        '_type': 'example/summary/datetime/Test',
        '_id': resp.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '3017-12-31 18:57:36',
        'count': 0,
        '_type': 'example/summary/datetime/Test',
    })


def test_summary_datetime_single_given_custom_units_months(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime | 2M | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/datetime', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/datetime/Test', json={'value': '2023-01-01T00:00:00'})
    response = app.get('/example/summary/datetime/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '2023-01-31 10:26:24',
        'count': 1,
        '_type': 'example/summary/datetime/Test',
        '_id': resp.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '2039-08-01 13:33:36',
        'count': 0,
        '_type': 'example/summary/datetime/Test',
    })


def test_summary_datetime_single_given_custom_units_quarters(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(rc, '''
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime | 3Q | open    | 
            ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/summary/datetime', ['insert', 'getall', 'search'])
    resp = app.post('/example/summary/datetime/Test', json={'value': '2023-01-01T00:00:00'})
    response = app.get('/example/summary/datetime/Test/:summary/value')
    json_response = response.json()
    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {
        'bin': '2023-05-17 23:16:48',
        'count': 1,
        '_type': 'example/summary/datetime/Test',
        '_id': resp.json()["_id"],
    })
    assert dict_equals(json_response["_data"][99], {
        'bin': '2097-08-17 00:43:12',
        'count': 0,
        '_type': 'example/summary/datetime/Test',
    })
