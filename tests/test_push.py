import datetime
import hashlib
import json
import textwrap
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple

import pytest
import requests
import sqlalchemy as sa
from pprintpp import pformat
from requests import PreparedRequest
from responses import POST
from responses import RequestsMock

from spinta.cli.push import _PushRow, _reset_pushed, _get_deleted_rows, _ErrorCounter
from spinta.cli.push import _get_row_for_error
from spinta.cli.push import _map_sent_and_recv
from spinta.cli.push import _init_push_state
from spinta.cli.push import _send_request
from spinta.cli.push import _push
from spinta.cli.push import _State
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest
from spinta.testing.manifest import load_manifest_and_context


@pytest.mark.skip('datasets')
@pytest.mark.models(
    'backends/postgres/report/:dataset/test',
)
def test_push_same_model(model, app):
    app.authmodel(model, ['insert'])
    data = [
        {'_op': 'insert', '_type': model, 'status': 'ok'},
        {'_op': 'insert', '_type': model, 'status': 'warning'},
        {'_op': 'insert', '_type': model, 'status': 'critical'},
        {'_op': 'insert', '_type': model, 'status': 'blocker'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post('/', headers=headers, content=payload)
    resp = resp.json()
    data = resp.pop('_data')
    assert resp == {
        '_transaction': resp['_transaction'],
        '_status': 'ok',
    }
    assert len(data) == 4
    assert data[0] == {
        '_id': data[0]['_id'],
        '_revision': data[0]['_revision'],
        '_type': 'backends/postgres/report/:dataset/test',
        'count': None,
        'notes': [],
        'operating_licenses': [],
        'report_type': None,
        'revision': None,
        'status': 'ok',
        'update_time': None,
        'valid_from_date': None,
    }


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('datasets')
def test_push_different_models(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['insert'])
    app.authmodel('backends/postgres/report/:dataset/test', ['insert'])
    data = [
        {'_op': 'insert', '_type': 'country/:dataset/csv', '_id': sha1('lt'), 'code': 'lt'},
        {'_op': 'insert', '_type': 'backends/postgres/report/:dataset/test', 'status': 'ok'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post('/', headers=headers, data=payload)
    resp = resp.json()
    assert '_data' in resp, resp
    data = resp.pop('_data')
    assert resp == {
        '_transaction': resp.get('_transaction'),
        '_status': 'ok',
    }
    assert len(data) == 2

    d = data[0]
    assert d == {
        '_id': d['_id'],
        '_revision': d['_revision'],
        '_type': 'country/:dataset/csv/:resource/countries',
        'code': 'lt',
        'title': None,
    }

    d = data[1]
    assert d == {
        '_id': d['_id'],
        '_revision': d['_revision'],
        '_type': 'backends/postgres/report/:dataset/test',
        'count': None,
        'notes': [],
        'operating_licenses': [],
        'report_type': None,
        'revision': None,
        'status': 'ok',
        'update_time': None,
        'valid_from_date': None,
    }


def test__map_sent_and_recv__no_recv(rc: RawConfig):
    manifest = load_manifest(rc, '''
    d | r | b | m | property | type   | access
    datasets/gov/example     |        |
      |   |   | Country      |        |
      |   |   |   | name     | string | open
    ''')

    model = manifest.models['datasets/gov/example/Country']
    sent = [
        _PushRow(model, {'name': 'Vilnius'}),
    ]
    recv = None
    assert list(_map_sent_and_recv(sent, recv)) == sent


def test__get_row_for_error__errors(rc: RawConfig):
    manifest = load_manifest(rc, '''
    d | r | b | m | property | type   | access
    datasets/gov/example     |        |
      |   |   | Country      |        |
      |   |   |   | name     | string | open
    ''')

    model = manifest.models['datasets/gov/example/Country']
    rows = [
        _PushRow(model, {
            '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            'name': 'Vilnius',
        }),
    ]
    errors = [
        {
            'context': {
                'id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            }
        }
    ]
    assert _get_row_for_error(rows, errors).splitlines() == [
        ' Model datasets/gov/example/Country, data:',
        " {'_id': '4d741843-4e94-4890-81d9-5af7c5b5989a', 'name': 'Vilnius'}",
    ]


def test__send_data__json_error(rc: RawConfig, responses: RequestsMock):
    model = 'example/City'
    url = f'https://example.com/{model}'
    responses.add(POST, url, status=500, body='{INVALID JSON}')
    rows = [
        _PushRow(model, {'name': 'Vilnius'}),
    ]
    data = '{"name": "Vilnius"}'
    session = requests.Session()
    _, resp = _send_request(session, url, "POST", rows, data)
    assert resp is None


def _match_dict(d: Dict[str, Any], m: Dict[str, Any]) -> bool:
    for k, v in m.items():
        if k not in d or d[k] != v:
            return False
    return True


def _matcher(match: Dict[str, Any]) -> Callable[..., Any]:
    def _match(request: PreparedRequest) -> Tuple[bool, str]:
        reason = ""
        body = request.body
        try:
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            data = json.loads(body) if body else {}
            if '_data' in data:
                valid = all(_match_dict(d, match) for d in data['_data'])
            else:
                valid = False
            if not valid:
                expected = textwrap.indent(pformat(match), '    ')
                received = textwrap.indent(pformat(data), '    ')
                reason = (
                    "request.body:\n"
                    f"{received}\n"
                    "  doesn't match\n"
                    f"{expected}"
                )
        except json.JSONDecodeError:
            valid = False
            reason = (
                "request.body doesn't match: JSONDecodeError: "
                "Cannot parse request.body"
            )
        return valid, reason
    return _match


def test_push_state__create(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
    m | property | type   | access
    City         |        |
      | name     | string | open
    ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rows = [
        _PushRow(model, {
            '_type': model.name,
            '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            'name': 'Vilnius',
        }),
    ]

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(
        POST, server,
        json={
            '_data': [{
                '_type': model.name,
                '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
                '_revision': 'f91adeea-3bb8-41b0-8049-ce47c7530bdc',
                'name': 'Vilnius',
            }],
        },
        match=[_matcher({
            '_op': 'insert',
            '_type': model.name,
            '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
        })],
    )

    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
    )

    table = state.metadata.tables[model.name]
    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [(
        '4d741843-4e94-4890-81d9-5af7c5b5989a',
        'f91adeea-3bb8-41b0-8049-ce47c7530bdc',
        False,
    )]


def test_push_state__create_error(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
    m | property | type   | access
    City         |        |
      | name     | string | open
    ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rows = [
        _PushRow(model, {
            '_type': model.name,
            '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            'name': 'Vilnius',
        })
    ]

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(POST, server, status=500, body='ERROR!')

    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
    )

    table = state.metadata.tables[model.name]
    query = sa.select([table.c.id, table.c.error])
    assert list(conn.execute(query)) == [
        ('4d741843-4e94-4890-81d9-5af7c5b5989a', True),
    ]


def test_push_state__update(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
    m | property | type   | access
    City         |        |
      | name     | string | open
    ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rev_before = 'f91adeea-3bb8-41b0-8049-ce47c7530bdc'
    rev_after = '45e8d4d6-bb6c-42cd-8ad8-09049bbed6bd'

    table = state.metadata.tables[model.name]
    conn.execute(table.insert().values(
        id='4d741843-4e94-4890-81d9-5af7c5b5989a',
        revision=rev_before,
        checksum='CHANGED',
        pushed=datetime.datetime.now(),
        error=False,
    ))

    rows = [
        _PushRow(model, {
            '_type': model.name,
            '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            'name': 'Vilnius',
        }),
    ]

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(
        POST, server,
        json={
            '_data': [{
                '_type': model.name,
                '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
                '_revision': rev_after,
                'name': 'Vilnius',
            }],
        },
        match=[_matcher({
            '_op': 'patch',
            '_type': model.name,
            '_where': "eq(_id, '4d741843-4e94-4890-81d9-5af7c5b5989a')",
            '_revision': rev_before,
        })],
    )

    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [(
        '4d741843-4e94-4890-81d9-5af7c5b5989a',
        rev_after,
        False,
    )]


def test_push_state__update_error(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
    m | property | type   | access
    City         |        |
      | name     | string | open
    ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rev_before = 'f91adeea-3bb8-41b0-8049-ce47c7530bdc'

    table = state.metadata.tables[model.name]
    conn.execute(table.insert().values(
        id='4d741843-4e94-4890-81d9-5af7c5b5989a',
        revision=rev_before,
        checksum='CHANGED',
        pushed=datetime.datetime.now(),
        error=False,
    ))

    rows = [
        _PushRow(model, {
            '_type': model.name,
            '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            'name': 'Vilnius',
        }),
    ]

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(POST, server, status=500, body='ERROR!')

    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [(
        '4d741843-4e94-4890-81d9-5af7c5b5989a',
        rev_before,
        True,
    )]


def test_push_state__delete(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
    m | property | type   | access
    City         |        |
      | name     | string | open
    ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rev_before = 'f91adeea-3bb8-41b0-8049-ce47c7530bdc'
    rev_after = '45e8d4d6-bb6c-42cd-8ad8-09049bbed6bd'

    table = state.metadata.tables[model.name]
    conn.execute(table.insert().values(
        id='4d741843-4e94-4890-81d9-5af7c5b5989a',
        revision=rev_before,
        checksum='DELETED',
        pushed=datetime.datetime.now(),
        error=False,
    ))

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(
        POST, server,
        json={
            '_data': [{
                '_type': model.name,
                '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
                '_revision': rev_after,
            }],
        },
        match=[_matcher({
            '_op': 'delete',
            '_type': model.name,
            '_where': "eq(_id, '4d741843-4e94-4890-81d9-5af7c5b5989a')"
        })],
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [(
        '4d741843-4e94-4890-81d9-5af7c5b5989a',
        rev_before,
        False,
    )]

    _reset_pushed(context, models, state.metadata)

    rows = [
        _PushRow(model, {
            '_op': 'delete',
            '_type': 'City',
            '_where': "eq(_id, '4d741843-4e94-4890-81d9-5af7c5b5989a')",
        }, op="delete", saved=True),
    ]

    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == []


def test_push_state__retry(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
       m | property | type   | access
       City         |        |
         | name     | string | open
       ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rev = 'f91adeea-3bb8-41b0-8049-ce47c7530bdc'
    _id = '4d741843-4e94-4890-81d9-5af7c5b5989a'

    table = state.metadata.tables[model.name]
    conn.execute(table.insert().values(
        id=_id,
        revision=None,
        checksum='CREATED',
        pushed=datetime.datetime.now(),
        error=True,
    ))

    rows = [
        _PushRow(model, {
            '_type': model.name,
            '_id': _id,
            'name': 'Vilnius',
        }, op="insert", error=True, saved=True),
    ]

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(
        POST, server,
        json={
            '_data': [{
                '_type': model.name,
                '_id': _id,
                '_revision': rev,
                'name': 'Vilnius',
            }],
        },
        match=[_matcher({
            '_op': 'insert',
            '_type': model.name,
            '_id': _id,
            'name': 'Vilnius',
        })],
    )

    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [(_id, rev, False)]


def test_push_state__max_errors(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
       m | property | type   | access
       City         |        |
         | name     | string | open
       ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rev = 'f91adeea-3bb8-41b0-8049-ce47c7530bdc'
    conflicting_rev = 'f91adeea-3bb8-41b0-8049-ce47c7530bdc'
    _id1 = '4d741843-4e94-4890-81d9-5af7c5b5989a'
    _id2 = '21ef6792-0315-4e86-9c39-b1b8f04b1f53'

    table = state.metadata.tables[model.name]
    conn.execute(table.insert().values(
        id=_id1,
        revision=rev,
        checksum='CREATED',
        pushed=datetime.datetime.now(),
        error=False,
    ))

    rows = [
        _PushRow(model, {
            '_type': model.name,
            '_id': _id1,
            '_revision': conflicting_rev,
            'name': 'Vilnius',
        }),
        _PushRow(model, {
            '_type': model.name,
            '_id': _id2,
            'name': 'Vilnius',
        }),
    ]

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(POST, server, status=409, body='Conflicting value')

    error_counter = _ErrorCounter(1)
    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
        chunk_size=1,
        error_counter=error_counter
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [(_id1, rev, True)]

    error_counter = _ErrorCounter(2)
    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
        chunk_size=1,
        error_counter=error_counter
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [
        (_id1, rev, True),
        (_id2, None, True)
    ]


def test_push_state__paginate(rc: RawConfig, responses: RequestsMock):
    context, manifest = load_manifest_and_context(rc, '''
       m | property | type   | prepare             |access
       City         |        | page(name, size: 1) |
         | name     | string |                     | open
       ''')

    model = manifest.models['City']
    models = [model]

    state = _State(*_init_push_state('sqlite://', models))
    conn = state.engine.connect()
    context.set('push.state.conn', conn)

    rev = 'f91adeea-3bb8-41b0-8049-ce47c7530bdc'
    _id = '4d741843-4e94-4890-81d9-5af7c5b5989a'

    table = state.metadata.tables[model.name]
    page_table = state.metadata.tables['_page']

    rows = [
        _PushRow(model, {
            '_type': model.name,
            '_id': _id,
            'name': 'Vilnius',
        }, op="insert"),
    ]

    client = requests.Session()
    server = 'https://example.com/'
    responses.add(
        POST, server,
        json={
            '_data': [{
                '_type': model.name,
                '_id': _id,
                '_revision': rev,
                'name': 'Vilnius',
            }],
        },
        match=[_matcher({
            '_op': 'insert',
            '_type': model.name,
            '_id': _id,
            'name': 'Vilnius',
        },)],
    )

    _push(
        context,
        client,
        server,
        models,
        rows,
        state=state,
    )

    query = sa.select([table.c.id, table.c.revision, table.c.error])
    assert list(conn.execute(query)) == [(_id, rev, False)]

    query = sa.select([page_table.c.model, page_table.c.property, page_table.c.value])
    assert list(conn.execute(query)) == [(model.name, 'name', 'Vilnius')]
