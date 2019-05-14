from datetime import datetime

import pytest


def test_schema_loader(context):
    result = context.push([
        {
            'type': 'country',
            '<id>': '1',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'org',
            '<id>': '1',
            'title': 'My Org',
            'govid': '0042',
            'country': {'type': 'country', '<id>': '1'},
        },
    ])

    # Add few objects to database.
    result = {x.pop('type'): x for x in result}
    assert result == {
        'country': {
            '<id>': '1',
            'id': result['country']['id'],
            'code': 'lt',
            'title': 'Lithuania',
        },
        'org': {
            '<id>': '1',
            'id': result['org']['id'],
            'country': result['country']['id'],
            'govid': '0042',
            'title': 'My Org',
        },
    }

    # Read those objects from database.
    assert context.getone('org', result['org']['id']) == {
        'id': result['org']['id'],
        'govid': '0042',
        'title': 'My Org',
        'country': int(result['country']['id']),
        'type': 'org',
    }

    assert context.getone('country', result['country']['id']) == {
        'id': result['country']['id'],
        'code': 'lt',
        'title': 'Lithuania',
        'type': 'country',
    }


def test_nested(context):
    result = list(context.push([
        {
            'type': 'nested',
            'some': [{'nested': {'structure': 'here'}}]
        }
    ]))
    assert context.getone('nested', result[0]['id']) == {
        'type': 'nested',
        'id': result[0]['id'],
    }


def test_report(context, app):
    app.authorize([
        'spinta_report_insert',
        'spinta_report_getone',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': '42',
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{'note': 'hello report', 'note_type': 'test'}]
    })
    assert resp.status_code == 201

    data = resp.json()
    id = data['id']
    resp = app.get(f'/report/{id}')
    assert resp.status_code == 200

    # FIXME: should return date/datetime strings instead of unix timestamps?
    assert resp.json() == {
        'id': id,
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': 1555718400,
        'update_time': 1555730055,
        'notes': [{'note': 'hello report', 'note_type': 'test'}],
    }


def test_invalid_report_int(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'count': 'c0unt',  # invalid conversion to int
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid value for int type: c0unt",
    }


def test_invalid_report_date(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'valid_from_date': '2019-04',  # invalid conversion to date
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid value for date type: 2019-04",
    }


def test_invalid_report_datetime(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'update_time': '2019-04',  # invalid conversion to datetime
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid value for datetime type: 2019-04",
    }


def test_invalid_report_array(context, app):
    app.authorize([
        'spinta_report_insert',
    ])

    resp = app.post('/report', json={
        'type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'notes': {'foo': 'bar'},  # invalid conversion to array
    })

    assert resp.status_code == 400
    assert resp.json() == {
        "error": "Invalid value for array type: {'foo': 'bar'}",
    }
