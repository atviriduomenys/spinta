import datetime


test_data = [
    {
        'type': 'report',
        'status': 'ok',
        'report_type': 'stv',
        'count': 10,
        'notes': [{
            'note': 'hello',
            'note_type': 'simple',
            'create_date': datetime.datetime(2019, 3, 14),
        }],
    },
    {
        'type': 'report',
        'status': 'invalid',
        'report_type': 'vmi',
        'count': 42,
        'notes': [{
            'note': 'world',
            'note_type': 'daily',
            'create_date': datetime.datetime(2019, 4, 20),
        }],
    },
    {
        'type': 'report',
        'status': 'invalid',
        'report_type': 'stv',
        'count': 13,
        'notes': [{
            'note': 'foo bar',
            'note_type': 'important',
            'create_date': datetime.datetime(2019, 2, 1),
        }],
    },
]


def test_search_exact(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:exact/status/ok')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']

    # single field search, case-insensitive
    resp = app.get('/reports/:exact/status/OK')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']

    # single field search, non string type
    resp = app.get('/reports/:exact/count/13')
    data = resp.json()['data']

    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # single non-existing field value search
    resp = app.get('/reports/:exact/status/o')
    data = resp.json()['data']
    assert len(data) == 0

    # single non-existing field search
    resp = app.get('/reports/:exact/state/o')
    assert resp.json()['error'] == "Unknown property 'state'."

    # multple field search
    resp = app.get('/reports/:exact/status/invalid/:exact/report_type/stv')
    data = resp.json()['data']

    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # same field searched multiple times is joined with AND operation by default
    resp = app.get('/reports/:exact/status/invalid/:exact/status/ok')
    data = resp.json()['data']
    assert len(data) == 0


def test_search_gt(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:gt/count/40')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:gt/count/40/:gt/count/10')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:gt/count/40/:exact/report_type/vmi')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # test `greater_than` works as expected
    resp = app.get('/reports/:gt/count/42')
    data = resp.json()['data']
    assert len(data) == 0


def test_search_gte(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:gte/count/40')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:gte/count/40/:gt/count/10')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:gte/count/40/:exact/report_type/vmi')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # test `greater_than` works as expected
    resp = app.get('/reports/:gte/count/42')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']


def test_search_lt(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:lt/count/12')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:lt/count/20/:gt/count/10')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:lt/count/50/:exact/report_type/vmi')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # test `lower_than` works as expected
    resp = app.get('/reports/:lt/count/10')
    data = resp.json()['data']
    assert len(data) == 0


def test_search_lte(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:lte/count/12')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:lte/count/20/:gt/count/10')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:lte/count/50/:exact/report_type/vmi')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # test `lower_than` works as expected
    resp = app.get('/reports/:lte/count/10')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']


def test_search_ne(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:ne/status/invalid')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']

    # single field search, case insensitive
    resp = app.get('/reports/:ne/status/invAlID')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:ne/count/10/:ne/count/42')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:ne/status/ok/:exact/report_type/vmi')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']


def test_search_contains(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:contains/report_type/vm')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # single field search, case insensitive
    resp = app.get('/reports/:contains/report_type/vM')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:contains/status/valid/:contains/report_type/tv')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # multi field search, case insensitive
    # test if operators are joined with AND logic
    resp = app.get('/reports/:contains/status/vAlId/:contains/report_type/TV')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # multi field search
    # test if operators are joined with AND logic for same field
    resp = app.get('/reports/:contains/report_type/vm/:contains/report_type/mi')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:contains/status/valid/:exact/report_type/vmi')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']


def test_search_startswith(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # single field search
    resp = app.get('/reports/:startswith/report_type/vm')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # single field search, case insensitive
    resp = app.get('/reports/:startswith/report_type/Vm')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:startswith/status/in/:startswith/report_type/vm')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get('/reports/:startswith/report_type/st/:exact/status/ok')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r1['id']

    # sanity check that `startswith` searches from the start
    resp = app.get('/reports/:startswith/status/valid')
    data = resp.json()['data']
    assert len(data) == 0


def test_search_nested(context, app):
    r1, r2, r3, = context.push(test_data)

    app.authorize(['spinta_report_search'])

    # nested `exact` search
    resp = app.get('/reports/:exact/notes.note/foo bar')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # nested `exact` search, case insensitive
    resp = app.get('/reports/:exact/notes.note/foo BAR')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r3['id']

    # nested `gt` search
    resp = app.get('/reports/:gt/notes.create_date/2019-04-01')
    data = resp.json()['data']
    assert len(data) == 1
    assert data[0]['id'] == r2['id']

    # nested non existant field
    resp = app.get('/reports/:exact/notes.foo.bar/baz')
    assert resp.json()['error'] == "Unknown property 'notes.foo.bar'."
