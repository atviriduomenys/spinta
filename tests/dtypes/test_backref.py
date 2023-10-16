import pytest

from pytest import FixtureRequest

from spinta.core.config import RawConfig
from spinta.exceptions import NoBackRefReferencesFound, NoReferencesFound, MultipleBackRefReferencesFound
from spinta.testing.client import create_test_client
from spinta.testing.data import are_lists_of_dicts_equal
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import error


def test_backref_one_to_one_level_4(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/oto/level4  |         |          |        |
                                |         |          |        |
      |   |   | Leader          |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country     | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | leader      | ref     | Leader   | open   | 4
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/oto/level4', ['insert', 'getone', 'getall'])

    leader_id = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    empty_leader_id = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/oto/level4/Leader', json={
        '_id': leader_id,
        'id': 0,
        'name': 'Test',
    })
    app.post('example/dtypes/backref/oto/level4/Leader', json={
        '_id': empty_leader_id,
        'id': 1,
        'name': 'Empty',
    })

    country_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    app.post('example/dtypes/backref/oto/level4/Country', json={
        '_id': country_id,
        'id': 0,
        'name': 'Lithuania',
        'leader': {
            "_id": leader_id
        }
    })
    result = app.get('example/dtypes/backref/oto/level4/Leader')
    result_json = result.json()['_data']
    assert 'country' in result_json[0]
    assert result_json[0]['country']['_id'] == country_id

    assert 'country' in result_json[1]
    assert result_json[1]['country'] is None

    # Check unique constraint
    result = app.post('example/dtypes/backref/oto/level4/Country', json={
        'id': 1,
        'name': 'Lithuania_0',
        'leader': {
            "_id": leader_id
        }
    })
    assert result.status_code == 400
    assert error(result, 'code') == {
        'code': 'UniqueConstraint'
    }


def test_backref_one_to_one_level_3(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/oto/level3  |         |          |        |
                                |         |          |        |
      |   |   | Leader          |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country     | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | leader      | ref     | Leader   | open   | 3
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/oto/level3', ['insert', 'getone', 'getall'])

    leader_id = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    empty_leader_id = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/oto/level3/Leader', json={
        '_id': leader_id,
        'id': 0,
        'name': 'Test',
    })
    app.post('example/dtypes/backref/oto/level3/Leader', json={
        '_id': empty_leader_id,
        'id': 1,
        'name': 'Empty',
    })

    country_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    app.post('example/dtypes/backref/oto/level3/Country', json={
        '_id': country_id,
        'id': 0,
        'name': 'Lithuania',
        'leader': {
            'id': 0,
            'name': 'Test'
        }
    })
    result = app.get('example/dtypes/backref/oto/level3/Leader')
    result_json = result.json()['_data']
    assert 'country' in result_json[0]
    assert result_json[0]['country'] == {
        'id': 0,
        'name': 'Lithuania'
    }

    assert 'country' in result_json[1]
    assert result_json[1]['country'] is None

    # Check unique constraint
    result = app.post('example/dtypes/backref/oto/level3/Country', json={
        'id': 1,
        'name': 'Lithuania_0',
        'leader': {
            'id': 0,
            'name': 'Test'
        }
    })
    assert result.status_code == 400
    assert error(result, 'code') == {
        'code': 'UniqueConstraint'
    }


def test_backref_one_to_many_level_4(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/otm/level4  |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country     | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   | 4
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/otm/level4', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    russian = "6e285646-f637-43a4-be34-79d616064f25"
    empty_language = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/otm/level4/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/otm/level4/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/otm/level4/Language', json={
        '_id': russian,
        'id': 2,
        'name': 'Russian',
    })
    app.post('example/dtypes/backref/otm/level4/Language', json={
        '_id': empty_language,
        'id': 3,
        'name': 'Empty',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    poland_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    app.post('example/dtypes/backref/otm/level4/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language': [
            {
                "_id": lithuanian
            },
            {
                "_id": english
            }
        ]
    })
    app.post('example/dtypes/backref/otm/level4/Country', json={
        '_id': poland_id,
        'id': 1,
        'name': 'Poland',
        'language': [
            {
                "_id": russian
            }
        ]
    })
    result = app.get('example/dtypes/backref/otm/level4/Language')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert result_json[0]['country'] == {
        '_id': lithuania_id
    }
    assert result_json[1]['country'] == {
        '_id': lithuania_id
    }
    assert result_json[2]['country'] == {
        '_id': poland_id
    }
    assert result_json[3]['country'] is None

    # Check unique constraint
    result = app.post('example/dtypes/backref/otm/level4/Country', json={
        'id': 2,
        'name': 'Poland',
        'language': [
            {
                "_id": russian
            }
        ]
    })
    assert result.status_code == 400
    assert error(result, 'code') == {
        'code': 'UniqueConstraint'
    }


def test_backref_one_to_many_level_3(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/otm/level3  |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country     | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   | 3
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/otm/level3', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    russian = "6e285646-f637-43a4-be34-79d616064f25"
    empty_language = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/otm/level3/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/otm/level3/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/otm/level3/Language', json={
        '_id': russian,
        'id': 2,
        'name': 'Russian',
    })
    app.post('example/dtypes/backref/otm/level3/Language', json={
        '_id': empty_language,
        'id': 3,
        'name': 'Empty',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    poland_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    app.post('example/dtypes/backref/otm/level3/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language': [
            {
                "id": 0,
                "name": "Lithuanian"
            },
            {
                "id": 1,
                "name": "English"
            },
        ]
    })
    app.post('example/dtypes/backref/otm/level3/Country', json={
        '_id': poland_id,
        'id': 1,
        'name': 'Poland',
        'language': [
            {
                "id": 2,
                "name": "Russian"
            },
        ]
    })
    result = app.get('example/dtypes/backref/otm/level3/Language')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert result_json[0]['country'] == {
        'id': 0,
        'name': 'Lithuania'
    }
    assert result_json[1]['country'] == {
        'id': 0,
        'name': 'Lithuania'
    }
    assert result_json[2]['country'] == {
        'id': 1,
        'name': 'Poland'
    }
    assert result_json[3]['country'] is None

    # Check unique constraint
    result = app.post('example/dtypes/backref/otm/level3/Country', json={
        'id': 2,
        'name': 'Poland',
        'language': [
            {
                "id": 2,
                "name": "Russian"
            }
        ]
    })
    assert result.status_code == 400
    assert error(result, 'code') == {
        'code': 'CompositeUniqueConstraint'
    }


def test_backref_many_to_one_level_4(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/mto/level4  |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language    | ref     | Language | open   | 4
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/mto/level4', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    empty_language = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/mto/level4/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/mto/level4/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/mto/level4/Language', json={
        '_id': empty_language,
        'id': 3,
        'name': 'Empty',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    england_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    us_id = 'e33b20aa-7311-49d5-86a4-566961dda218'
    app.post('example/dtypes/backref/mto/level4/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language': {
            "_id": lithuanian
        }
    })
    app.post('example/dtypes/backref/mto/level4/Country', json={
        '_id': england_id,
        'id': 1,
        'name': 'England',
        'language': {
            "_id": english
        }
    })
    app.post('example/dtypes/backref/mto/level4/Country', json={
        '_id': us_id,
        'id': 2,
        'name': 'US',
        'language': {
            "_id": english
        }
    })
    result = app.get('example/dtypes/backref/mto/level4/Language?expand()')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert are_lists_of_dicts_equal(result_json[0]['country'], [
        {
            '_id': lithuania_id
        }
    ])
    assert are_lists_of_dicts_equal(result_json[1]['country'], [
        {
            '_id': england_id
        },
        {
            '_id': us_id
        }
    ])
    assert result_json[2]['country'] == []


def test_backref_many_to_one_level_3(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/mto/level3  |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language    | ref     | Language | open   | 3
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/mto/level3', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    empty_language = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/mto/level3/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/mto/level3/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/mto/level3/Language', json={
        '_id': empty_language,
        'id': 3,
        'name': 'Empty',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    england_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    us_id = 'e33b20aa-7311-49d5-86a4-566961dda218'
    app.post('example/dtypes/backref/mto/level3/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language': {
            'id': 0,
            'name': "Lithuanian"
        }
    })
    app.post('example/dtypes/backref/mto/level3/Country', json={
        '_id': england_id,
        'id': 1,
        'name': 'England',
        'language': {
            'id': 1,
            'name': 'English',
        }
    })
    app.post('example/dtypes/backref/mto/level3/Country', json={
        '_id': us_id,
        'id': 2,
        'name': 'US',
        'language': {
            'id': 1,
            'name': 'English',
        }
    })
    result = app.get('example/dtypes/backref/mto/level3/Language?expand()')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert are_lists_of_dicts_equal(result_json[0]['country'], [
        {
            'id': 0,
            'name': "Lithuania"
        }
    ])
    assert are_lists_of_dicts_equal(result_json[1]['country'], [
        {
            'id': 1,
            'name': "England"
        },
        {
            'id': 2,
            'name': 'US'
        }
    ])
    assert result_json[2]['country'] == []


def test_backref_many_to_many_level_4(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/mtm/level4  |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   | 4
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/mtm/level4', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    russian = "6e285646-f637-43a4-be34-79d616064f25"
    empty_language = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/mtm/level4/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/mtm/level4/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/mtm/level4/Language', json={
        '_id': russian,
        'id': 2,
        'name': 'Russian',
    })
    app.post('example/dtypes/backref/mtm/level4/Language', json={
        '_id': empty_language,
        'id': 3,
        'name': 'Empty',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    poland_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    app.post('example/dtypes/backref/mtm/level4/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language': [
            {
                "_id": lithuanian
            },
            {
                "_id": english
            }
        ]
    })
    app.post('example/dtypes/backref/mtm/level4/Country', json={
        '_id': poland_id,
        'id': 1,
        'name': 'Poland',
        'language': [
            {
                "_id": russian
            },
            {
                "_id": english
            }
        ]
    })
    result = app.get('example/dtypes/backref/mtm/level4/Language?expand()')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert are_lists_of_dicts_equal(result_json[0]['country'], [
        {
            '_id': lithuania_id
        }
    ])

    assert are_lists_of_dicts_equal(result_json[1]['country'], [
        {
            '_id': lithuania_id
        },
        {
            '_id': poland_id
        }
    ])
    assert are_lists_of_dicts_equal(result_json[2]['country'], [
        {
            '_id': poland_id
        }
    ])
    assert result_json[3]['country'] == []


def test_backref_many_to_many_level_3(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/mtm/level3  |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   | 3
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/mtm/level3', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    russian = "6e285646-f637-43a4-be34-79d616064f25"
    empty_language = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/mtm/level3/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/mtm/level3/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/mtm/level3/Language', json={
        '_id': russian,
        'id': 2,
        'name': 'Russian',
    })
    app.post('example/dtypes/backref/mtm/level3/Language', json={
        '_id': empty_language,
        'id': 3,
        'name': 'Empty',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    poland_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    app.post('example/dtypes/backref/mtm/level3/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language': [
            {
                'id': 0,
                'name': 'Lithuanian'
            },
            {
                'id': 1,
                'name': 'English'
            }
        ]
    })
    app.post('example/dtypes/backref/mtm/level3/Country', json={
        '_id': poland_id,
        'id': 1,
        'name': 'Poland',
        'language': [
            {
                'id': 2,
                'name': 'Russian'
            },
            {
                'id': 1,
                'name': 'English'
            }
        ]
    })
    result = app.get('example/dtypes/backref/mtm/level3/Language?expand()')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert are_lists_of_dicts_equal(result_json[0]['country'], [
        {
            'id': 0,
            'name': 'Lithuania'
        }
    ])

    assert are_lists_of_dicts_equal(result_json[1]['country'], [
        {
            'id': 0,
            'name': 'Lithuania'
        },
        {
            'id': 1,
            'name': 'Poland'
        }
    ])
    assert are_lists_of_dicts_equal(result_json[2]['country'], [
        {
            'id': 1,
            'name': 'Poland'
        }
    ])
    assert result_json[3]['country'] == []


def test_backref_x_to_many_expand(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/xtm/expand |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/xtm/expand', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    russian = "6e285646-f637-43a4-be34-79d616064f25"
    app.post('example/dtypes/backref/xtm/expand/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/xtm/expand/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/xtm/expand/Language', json={
        '_id': russian,
        'id': 2,
        'name': 'Russian',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    poland_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    app.post('example/dtypes/backref/xtm/expand/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language': [
            {
                "_id": lithuanian
            },
            {
                "_id": english
            }
        ]
    })
    app.post('example/dtypes/backref/xtm/expand/Country', json={
        '_id': poland_id,
        'id': 1,
        'name': 'Poland',
        'language': [
            {
                "_id": russian
            },
            {
                "_id": english
            }
        ]
    })
    result = app.get('example/dtypes/backref/xtm/expand/Language')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert result_json[0]['country'] == []
    assert result_json[1]['country'] == []
    assert result_json[2]['country'] == []

    result = app.get('example/dtypes/backref/xtm/expand/Language?expand()')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert are_lists_of_dicts_equal(result_json[0]['country'], [
        {
            '_id': lithuania_id
        }
    ])

    assert are_lists_of_dicts_equal(result_json[1]['country'], [
        {
            '_id': lithuania_id
        },
        {
            '_id': poland_id
        }
    ])
    assert are_lists_of_dicts_equal(result_json[2]['country'], [
        {
            '_id': poland_id
        }
    ])


def test_backref_error_modify_backref_insert(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/error/modify/insert |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/error/modify/insert', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    result = app.post('example/dtypes/backref/error/modify/insert/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
        'country': []
    })
    assert result.status_code == 400
    assert error(result, 'code') == {
        'code': 'CannotModifyBackRefProp'
    }


def test_backref_error_modify_backref_put(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/error/modify/put |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/error/modify/put', ['insert', 'getone', 'getall', 'update'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    result = app.post('example/dtypes/backref/error/modify/put/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian'
    })
    _rev = result.json()['_revision']

    result = app.put(f'example/dtypes/backref/error/modify/put/Language/{lithuanian}', json={
        '_revision': _rev,
        'id': 0,
        'name': 'Lithuanian',
        'country': []
    })
    assert result.status_code == 400
    assert error(result, 'code') == {
        'code': 'CannotModifyBackRefProp'
    }


def test_backref_error_modify_backref_patch(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/error/modify/patch |         |          |        |
                                |         |          |        |
      |   |   | Language        |         | id, name |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | country[]   | backref | Country  | open   |
                                |         |          |        |
      |   |   | Country         |         | id       |        |
      |   |   |   | id          | integer |          | open   |
      |   |   |   | name        | string  |          | open   |
      |   |   |   | language[]  | ref     | Language | open   |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/error/modify/patch', ['insert', 'getone', 'getall', 'patch'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    result = app.post('example/dtypes/backref/error/modify/patch/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian'
    })
    _rev = result.json()['_revision']

    result = app.patch(f'example/dtypes/backref/error/modify/patch/Language/{lithuanian}', json={
        '_revision': _rev,
        'id': 0,
        'name': 'Lithuanian',
        'country': []
    })
    assert result.status_code == 400
    assert error(result, 'code') == {
        'code': 'CannotModifyBackRefProp'
    }


def test_backref_multiple_same(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/multiple/same |         |          |        |
                                        |         |                             |        |
      |   |   | Language                |         | id, name                    |        |
      |   |   |   | id                  | integer |                             | open   |
      |   |   |   | name                | string  |                             | open   |
      |   |   |   | country_primary[]   | backref | Country[language_primary]   | open   |
      |   |   |   | country_secondary[] | backref | Country[language_secondary] | open   |
                                        |         |                             |        |
      |   |   | Country                 |         | id                          |        |
      |   |   |   | id                  | integer |                             | open   |
      |   |   |   | name                | string  |                             | open   |
      |   |   |   | language_primary    | ref     | Language                    | open   |
      |   |   |   | language_secondary  | ref     | Language                    | open   |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/multiple/same', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    russian = "6e285646-f637-43a4-be34-79d616064f25"
    app.post('example/dtypes/backref/multiple/same/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/multiple/same/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/multiple/same/Language', json={
        '_id': russian,
        'id': 2,
        'name': 'Russian',
    })

    lithuania_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    poland_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    app.post('example/dtypes/backref/multiple/same/Country', json={
        '_id': lithuania_id,
        'id': 0,
        'name': 'Lithuania',
        'language_primary': {
            "_id": lithuanian
        },
        'language_secondary': {
            "_id": english
        }
    })
    app.post('example/dtypes/backref/multiple/same/Country', json={
        '_id': poland_id,
        'id': 1,
        'name': 'Poland',
        'language_primary': {
            "_id": english
        },
        'language_secondary': {
            "_id": russian
        }
    })

    result = app.get('example/dtypes/backref/multiple/same/Language?expand()')
    assert result.status_code == 200
    result_json = result.json()['_data']
    assert are_lists_of_dicts_equal(result_json[0]['country_primary'], [
        {
            '_id': lithuania_id
        }
    ])
    assert result_json[0]['country_secondary'] == []

    assert are_lists_of_dicts_equal(result_json[1]['country_primary'], [
        {
            '_id': poland_id
        }
    ])
    assert are_lists_of_dicts_equal(result_json[1]['country_secondary'], [
        {
            '_id': lithuania_id
        }
    ])

    assert result_json[2]['country_primary'] == []
    assert are_lists_of_dicts_equal(result_json[2]['country_secondary'], [
        {
            '_id': poland_id
        }
    ])


def test_backref_multiple_all_types(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property    | type    | ref      | access | level
    example/dtypes/backref/multiple/all |         |          |        |
                                        |         |                             |        |
      |   |   | Language                |         | id, name                    |        |
      |   |   |   | id                  | integer |                             | open   |
      |   |   |   | name                | string  |                             | open   |
      |   |   |   | country_0           | backref | Country[language_0]         | open   |
      |   |   |   | country_1[]         | backref | Country[language_1]         | open   |
      |   |   |   | country_0_array     | backref | Country[language_0_array]   | open   |
      |   |   |   | country_1_array[]   | backref | Country[language_1_array]   | open   |
                                        |         |                             |        |
      |   |   | Country                 |         | id                          |        |
      |   |   |   | id                  | integer |                             | open   |
      |   |   |   | name                | string  |                             | open   |
      |   |   |   | language_0          | ref     | Language                    | open   |
      |   |   |   | language_1          | ref     | Language                    | open   |
      |   |   |   | language_0_array[]  | ref     | Language                    | open   |
      |   |   |   | language_1_array[]  | ref     | Language                    | open   |
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/dtypes/backref/multiple/all', ['insert', 'getone', 'getall'])

    lithuanian = "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
    english = "1cccf6f6-9fe2-4055-b003-915a7c1abee8"
    russian = "6e285646-f637-43a4-be34-79d616064f25"
    empty_language = "090875d3-aaaa-422f-984a-733acc0a7c77"
    app.post('example/dtypes/backref/multiple/all/Language', json={
        '_id': lithuanian,
        'id': 0,
        'name': 'Lithuanian',
    })
    app.post('example/dtypes/backref/multiple/all/Language', json={
        '_id': english,
        'id': 1,
        'name': 'English',
    })
    app.post('example/dtypes/backref/multiple/all/Language', json={
        '_id': russian,
        'id': 2,
        'name': 'Russian',
    })
    app.post('example/dtypes/backref/multiple/all/Language', json={
        '_id': empty_language,
        'id': 3,
        'name': 'Empty',
    })

    lithuania_0_id = 'd73306fb-4ee5-483d-9bad-d86f98e1869c'
    lithuania_1_id = '84eb5386-ec69-4df8-a1d2-d3ce439b7054'
    poland_0_id = '25c3f7bf-67f5-47cc-8d40-297c499e8067'
    poland_1_id = '04091c91-b789-44cf-8024-e3dce45421cc'
    app.post('example/dtypes/backref/multiple/all/Country', json={
        '_id': lithuania_0_id,
        'id': 0,
        'name': 'Lithuania',
        'language_0': {
            "_id": lithuanian
        },
        'language_1': {
            "_id": lithuanian
        },
        'language_0_array': [
            {
                "_id": lithuanian
            }
        ],
        'language_1_array': [
            {
                "_id": lithuanian
            },
            {
                "_id": english
            },
        ],
    })
    app.post('example/dtypes/backref/multiple/all/Country', json={
        '_id': lithuania_1_id,
        'id': 1,
        'name': 'Lithuania',
        'language_1': {
            "_id": lithuanian
        },
        'language_1_array': [
            {
                "_id": lithuanian
            },
            {
                "_id": english
            },
            {
                '_id': russian
            }
        ],
    })
    app.post('example/dtypes/backref/multiple/all/Country', json={
        '_id': poland_0_id,
        'id': 2,
        'name': 'Poland',
        'language_0': {
            "_id": english
        },
        'language_1': {
            "_id": english
        },
        'language_0_array': [
            {
                "_id": english
            }
        ],
        'language_1_array': [
            {
                "_id": english
            },
            {
                "_id": russian
            },
        ],
    })
    app.post('example/dtypes/backref/multiple/all/Country', json={
        '_id': poland_1_id,
        'id': 3,
        'name': 'Poland',
        'language_0': {
            "_id": russian
        },
        'language_1': {
            "_id": english
        },
        'language_0_array': [
            {
                "_id": russian
            }
        ],
        'language_1_array': [
            {
                "_id": english
            },
        ],
    })

    result = app.get('example/dtypes/backref/multiple/all/Language?expand()')
    assert result.status_code == 200
    result_json = result.json()['_data']

    # Lithuanian
    assert result_json[0]['country_0'] == {
        '_id': lithuania_0_id
    }
    assert are_lists_of_dicts_equal(result_json[0]['country_1'], [
        {
            '_id': lithuania_0_id
        },
        {
            '_id': lithuania_1_id
        }
    ])
    assert result_json[0]['country_0_array'] == {
        '_id': lithuania_0_id
    }
    assert are_lists_of_dicts_equal(result_json[0]['country_1_array'], [
        {
            '_id': lithuania_0_id
        },
        {
            '_id': lithuania_1_id
        }
    ])

    # English
    assert result_json[1]['country_0'] == {
        '_id': poland_0_id
    }
    assert are_lists_of_dicts_equal(result_json[1]['country_1'], [
        {
            '_id': poland_0_id
        },
        {
            '_id': poland_1_id
        }
    ])
    assert result_json[1]['country_0_array'] == {
        '_id': poland_0_id
    }

    assert are_lists_of_dicts_equal(result_json[1]['country_1_array'], [
        {
            '_id': lithuania_0_id
        },
        {
            '_id': lithuania_1_id
        },
        {
            '_id': poland_0_id
        },
        {
            '_id': poland_1_id
        }
    ])

    # Russian
    assert result_json[2]['country_0'] == {
        '_id': poland_1_id
    }
    assert result_json[2]['country_1'] == []
    assert result_json[2]['country_0_array'] == {
        '_id': poland_1_id
    }

    assert are_lists_of_dicts_equal(result_json[2]['country_1_array'], [
        {
            '_id': lithuania_1_id
        },
        {
            '_id': poland_0_id
        }
    ])

    # Empty
    assert result_json[3]['country_0'] is None
    assert result_json[3]['country_1'] == []
    assert result_json[3]['country_0_array'] is None
    assert result_json[3]['country_1_array'] == []


def test_backref_error_no_ref(
    rc: RawConfig,
    postgresql: str,
):
    with pytest.raises(NoBackRefReferencesFound):
        bootstrap_manifest(rc, '''
        d | r | b | m | property    | type    | ref      | access | level
        example/dtypes/backref/error/no_ref |         |          |        |
                                    |         |          |        |
          |   |   | Language        |         | id, name |        |
          |   |   |   | id          | integer |          | open   |
          |   |   |   | name        | string  |          | open   |
          |   |   |   | country     | backref | Country  | open   |
                                    |         |          |        |
          |   |   | Country         |         | id       |        |
          |   |   |   | id          | integer |          | open   |
          |   |   |   | name        | string  |          | open   |
        ''', backend=postgresql)


def test_backref_error_invalid_ref(
    rc: RawConfig,
    postgresql: str
):
    with pytest.raises(NoReferencesFound):
        bootstrap_manifest(rc, '''
        d | r | b | m | property    | type    | ref      | access | level
        example/dtypes/backref/error/no_ref |         |          |        |
                                    |         |          |        |
          |   |   | Language        |         | id, name |        |
          |   |   |   | id          | integer |          | open   |
          |   |   |   | name        | string  |          | open   |
          |   |   |   | country     | backref | Country[name] | open   |
                                    |         |          |        |
          |   |   | Country         |         | id       |        |
          |   |   |   | id          | integer |          | open   |
          |   |   |   | name        | string  |          | open   |
          |   |   |   | language    | ref     | Language | open   |
        ''', backend=postgresql)


def test_backref_error_multiple_ref(
    rc: RawConfig,
    postgresql: str
):
    with pytest.raises(MultipleBackRefReferencesFound):
        bootstrap_manifest(rc, '''
        d | r | b | m | property    | type    | ref      | access | level
        example/dtypes/backref/error/no_ref |         |          |        |
                                    |         |          |        |
          |   |   | Language        |         | id, name |        |
          |   |   |   | id          | integer |          | open   |
          |   |   |   | name        | string  |          | open   |
          |   |   |   | country     | backref | Country  | open   |
                                    |         |          |        |
          |   |   | Country         |         | id       |        |
          |   |   |   | id          | integer |          | open   |
          |   |   |   | name        | string  |          | open   |
          |   |   |   | language    | ref     | Language | open   |
          |   |   |   | language0   | ref     | Language | open   |
        ''', backend=postgresql)
