import pytest
import requests

from spinta.testing.utils import get_error_codes, get_error_context


test_data = [
    {
        '_type': 'report',
        'status': 'ok',
        'report_type': 'stv',
        'count': 10,
        'notes': [{
            'note': 'hello',
            'note_type': 'simple',
            'create_date': '2019-03-14',
        }],
        'operating_licenses': [{
            'license_types': ['valid', 'invalid'],
        }],
    },
    {
        '_type': 'report',
        'status': 'invalid',
        'report_type': 'vmi',
        'count': 42,
        'notes': [{
            'note': 'world',
            'note_type': 'daily',
            'create_date': '2019-04-20',
        }],
        'operating_licenses': [{
            'license_types': ['expired'],
        }],
    },
    {
        '_type': 'report',
        'status': 'invalid',
        'report_type': 'stv',
        'count': 13,
        'notes': [{
            'note': 'foo bar',
            'note_type': 'important',
            'create_date': '2019-02-01',
        }],
    },
]


def _push_test_data(app, model, data=None):
    app.authmodel(model, ['insert'])
    resp = app.post('/', json={'_data': [
        {
            **res,
            '_op': 'insert',
            '_type': model,
        }
        for res in data or test_data
    ]})
    assert resp.status_code == 200, resp.json()
    resp = resp.json()
    assert '_data' in resp, resp
    return resp['_data']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
    'backends/postgres/report/:dataset/test'
)
def test_search_exact(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?status=ok')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # single field search, case-insensitive
    resp = app.get(f'/{model}?status=OK')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # single field search, non string type
    resp = app.get(f'/{model}?count=13')
    data = resp.json()['_data']

    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # single field fsearch, non string type
    resp = app.get(f'/{model}?count=abc')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # single non-existing field value search
    resp = app.get(f'/{model}?status=o')
    data = resp.json()['_data']
    assert len(data) == 0

    # single non-existing field search
    resp = app.get(f'/{model}?state=o')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]

    # multple field search
    resp = app.get(f'/{model}?status=invalid&report_type=stv')
    data = resp.json()['_data']

    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # same field searched multiple times is joined with AND operation by default
    resp = app.get(f'/{model}?status=invalid&status=ok')
    data = resp.json()['_data']
    assert len(data) == 0


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_gt(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?count=gt=40')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # search for string value
    resp = app.get(f'/{model}?status=gt=ok')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidOperandValue"]
    assert get_error_context(
        resp.json(), "InvalidOperandValue", ["operator", "model", "property"]
    ) == {
        "operator": "gt",
        "model": model,
        "property": "status",
    }

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=gt=40&count=gt=10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=gt=40&report_type=vmi')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `greater_than` works as expected
    resp = app.get(f'/{model}?count=gt=42')
    data = resp.json()['_data']
    assert len(data) == 0


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_gt_with_nested_date(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?gt(recurse(create_date),2019-04-19)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_gte(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?count=ge=40')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # search for string value
    resp = app.get(f'/{model}?status=ge=ok')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidOperandValue"]
    assert get_error_context(
        resp.json(), "InvalidOperandValue", ["operator", "model", "property"]
    ) == {
        "operator": "ge",
        "model": model,
        "property": "status",
    }

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=ge=40&count=gt=10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=ge=40&report_type=vmi')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `greater_than` works as expected
    resp = app.get(f'/{model}?count=ge=42')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ge_with_nested_date(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?ge(recurse(create_date),2019-04-20)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_lt(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?count=lt=12')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # search for string value
    resp = app.get(f'/{model}?status=lt=ok')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidOperandValue"]
    assert get_error_context(
        resp.json(), "InvalidOperandValue", ["operator", "model", "property"]
    ) == {
        "operator": "lt",
        "model": model,
        "property": "status",
    }

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=lt=20&count=gt=10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=lt=50&report_type=vmi')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `lower_than` works as expected
    resp = app.get(f'/{model}?count=lt=10')
    data = resp.json()['_data']
    assert len(data) == 0


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_lt_with_nested_date(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?lt(recurse(create_date),2019-02-02)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_lte(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?count=le=12')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # search for string value
    resp = app.get(f'/{model}?status=le=ok')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidOperandValue"]
    assert get_error_context(
        resp.json(), "InvalidOperandValue", ["operator", "model", "property"]
    ) == {
        "operator": "le",
        "model": model,
        "property": "status",
    }

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=le=20&count=gt=10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=le=50&report_type=vmi')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `lower_than` works as expected
    resp = app.get(f'/{model}?count=le=10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_le_with_nested_date(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?le(recurse(create_date),2019-02-01)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']


@pytest.mark.skip('NotImplementedError')
@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?status=ne=invalid')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # single field search, case insensitive
    resp = app.get(f'/{model}?status=ne=invAlID')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=ne=10&count=ne=42')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?status=ne=ok&report_type=vmi')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `ne` with nested structure
    resp = app.get(f'/{model}?ne(notes.create_date,2019-02-01)&status=ne=invalid')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # test `ne` with nested structures and not full data in all resources
    resp = app.get(f'/{model}?ne(operating_licenses.license_types,valid)&sort(+count)')
    data = resp.json()['_data']
    assert len(data) == 2
    assert data[0]['_id'] == r3['_id']
    assert data[1]['_id'] == r2['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains(model, context, app, mocker):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?contains(report_type,vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # single field search, case insensitive
    resp = app.get(f'/{model}?contains(report_type,vM)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?contains(status,valid)&contains(report_type,tv)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field search, case insensitive
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?contains(status,vAlId)&contains(report_type,TV)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field search
    # test if operators are joined with AND logic for same field
    resp = app.get(f'/{model}?contains(report_type,vm)&contains(report_type,mi)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?contains(status,valid)&report_type=vmi')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains_type_check(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?contains(recurse(create_date),2019-04-20)')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidOperandValue"]
    assert get_error_context(
        resp.json(), "InvalidOperandValue", ["operator", "model", "property"]
    ) == {
        "operator": "contains",
        "model": model,
        "property": "notes.create_date",
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains_with_select(model, context, app, mocker):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])

    # `contains` with select
    resp = app.get(f'/{model}?contains(report_type,vm)&select(count)')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0] == {
        'count': 42,
    }

    # `contains` with select and always_show_id
    mocker.patch.object(context.get('config'), 'always_show_id', True)
    resp = app.get(f'/{model}?contains(report_type,vm)&select(count)')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0] == {
        '_id': r2['_id'],
        'count': 42,
    }

    # `contains` with always_show_id should return just id
    resp = app.get(f'/{model}?contains(report_type,vm)')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0] == {
        '_id': r2['_id'],
    }


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_startswith(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?startswith(report_type,vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # single field search, case insensitive
    resp = app.get(f'/{model}?startswith(report_type,Vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?startswith(status,in)&startswith(report_type,vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?startswith(report_type,st)&status=ok')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # sanity check that `startswith` searches from the start
    resp = app.get(f'/{model}?startswith(status,valid)')
    data = resp.json()['_data']
    assert len(data) == 0

    # `startswith` type check
    resp = app.get(f'/{model}?startswith(notes.create_date,2019-04-20)')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidOperandValue"]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # nested `exact` search
    resp = app.get(f'/{model}?(notes,note)=foo bar')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # nested `exact` search, case insensitive
    resp = app.get(f'/{model}?(notes,note)=foo BAR')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # nested `exact` search with dates
    resp = app.get(f'/{model}?(notes,create_date)=2019-03-14')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # nested `gt` search
    resp = app.get(f'/{model}?(notes,create_date)=gt=2019-04-01')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # nested non existant field
    resp = app.get(f'/{model}?(notes,foo,bar)=baz')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]
    assert get_error_context(
        resp.json(),
        "FieldNotInResource",
        ["property", "model"]
    ) == {
        "property": "notes.foo.bar",
        "model": model,
    }

    # nested `contains` search
    resp = app.get(f'/{model}?contains(notes.note,bar)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    resp = app.get(f'/{model}?contains(operating_licenses.license_types,lid)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # nested `startswith` search
    resp = app.get(f'/{model}?startswith(notes.note,fo)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    resp = app.get(f'/{model}?startswith(operating_licenses.license_types,exp)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']


def ids(resources):
    if isinstance(resources, requests.models.Response):
        resp = resources
        assert resp.status_code == 200, resp.json()
        resources = resp.json()['_data']
    return [r['_id'] for r in resources]


@pytest.mark.models(
    # TODO: add or support for mongo
    # 'backends/mongo/report',
    'backends/postgres/report',
)
def test_or(model, context, app):
    r1, r2, r3, = ids(_push_test_data(app, model))
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?or(eq(count,42),eq(status,ok))')
    assert ids(resp) == [r1, r2]

    resp = app.get(f'/{model}?or(le(count,10),eq(count,13))')
    assert ids(resp) == [r1, r3]


@pytest.mark.skip('recurse operator')
@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_recurse(model, context, app):
    r1, r2, r3, = ids(_push_test_data(app, model))

    app.authmodel(model, ['search'])

    resp = app.get(f'/{model}?eq(,hello)')
    assert ids(resp) == [r1]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested_recurse(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?eq(recurse(note),foo bar)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']


@pytest.mark.models(
    # TODO: add OR support for mongo
    # 'backends/mongo/report',
    'backends/postgres/recurse',
)
def test_search_nested_recurse_multiple_props(model, context, app):
    r1, r2, = ids(_push_test_data(app, model, [
        {
            'title': "Org",
            'country': 'fi',
            'govids': [
                {'govid': '1', 'country': 'fi'},
                {'govid': '2', 'country': 'se'},
            ]
        },
        {
            'title': "Org",
            'country': 'no',
            'govids': [
                {'govid': '3', 'country': 'no'},
            ]
        },
    ]))
    app.authmodel(model, ['search'])

    resp = app.get(f'/{model}?eq(recurse(country),se)')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?eq(recurse(country),fi)')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?eq(recurse(country),no)')
    assert ids(resp) == [r2]
