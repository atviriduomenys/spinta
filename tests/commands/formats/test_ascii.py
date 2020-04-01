import datetime
import hashlib

import pytest


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('datasets')
def test_export_ascii(app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['upsert', 'getall', 'search', 'changes'])

    resp = app.post('/country/:dataset/csv/:resource/countries', json={'_data': [
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': sha1('1'),
            '_where': '_id="' + sha1('1') + '"',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': sha1('2'),
            '_where': '_id="' + sha1('2') + '"',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': sha1('2'),
            '_where': '_id="' + sha1('2') + '"',
            'code': 'lv',
            'title': 'Latvia',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    assert app.get('/country/:dataset/csv/:resource/countries/:format/ascii?select(code,title)&sort(+code)&format(colwidth(42))').text == (
        'code     title  \n'
        '================\n'
        'lt     Lithuania\n'
        'lv     Latvia   '
    )

    resp = app.get('/country/:dataset/csv/:resource/countries/:changes')
    changes = resp.json()['_data']
    changes = [{k: str(v) for k, v in row.items()} for row in changes]
    res = app.get('country/:dataset/csv/:resource/countries/:changes/:format/ascii?format(colwidth(42))').text
    lines = res.splitlines()
    cols = lines[0].split()
    data = [dict(zip(cols, [v.strip() for v in row.split()])) for row in lines[2:]]
    data = [{k: v for k, v in row.items() if v != 'None'} for row in data]
    assert data == changes


@pytest.mark.asyncio
async def test_export_multiple_types(context):
    rows = [
        {'_type': 'a', 'value': 1},
        {'_type': 'a', 'value': 2},
        {'_type': 'a', 'value': 3},
        {'_type': 'b', 'value': 1},
        {'_type': 'b', 'value': 2},
        {'_type': 'b', 'value': 3},
        {'_type': 'c', 'value': 1},
        {'_type': 'c', 'value': 2},
        {'_type': 'c', 'value': 3},
    ]

    config = context.get('config')
    exporter = config.exporters['ascii']
    assert ''.join(exporter(rows)) == (
        '\n\n'
        'Table: a\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    \n'
        '\n'
        'Table: b\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    \n'
        '\n'
        'Table: c\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    '
    )


@pytest.mark.skip('datasets')
def test_export_ascii_params(app, mocker):
    app.authmodel('country/:dataset/csv/:resource/countries', ['insert', 'search'])
    resp = app.post('/country/:dataset/csv/:resource/countries', json={'_data': [
        {
            '_op': 'insert',
            '_type': 'country/:dataset/csv/:resource/countries',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': 'country/:dataset/csv/:resource/countries',
            'code': 'lv',
            'title': 'Latvia',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    assert app.get('/country/:dataset/csv/:resource/countries/:format/ascii?select(code,title)&sort(+code)&format(width(50))').text == (
        'code     title  \n'
        '================\n'
        'lt     Lithuania\n'
        'lv     Latvia   '
    )
