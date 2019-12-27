import pytest
import requests

from spinta.testing.utils import get_error_codes, get_error_context


test_data = [
    {
        '_type': 'report',
        'status': 'OK',
        'report_type': 'STV',
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
        'report_type': 'VMI',
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
        'report_type': 'STV',
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


class RowIds:

    def __init__(self, ids):
        self.ids = {k: v for v, k in enumerate(self._cast(ids))}

    def __call__(self, ids):
        return [self.ids.get(i, i) for i in self._cast(ids)]

    def _cast(self, ids):
        if isinstance(ids, requests.models.Response):
            resp = ids
            assert resp.status_code == 200, resp.json()
            ids = resp.json()
        if isinstance(ids, dict):
            ids = ids['_data']
        if isinstance(ids, list) and len(ids) > 0 and isinstance(ids[0], dict):
            ids = [r['_id'] for r in ids]
        return ids


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
    'backends/postgres/report/:dataset/test'
)
def test_search_exact(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?status=OK')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_exact_lower(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?eq(lower(status),ok)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
    'backends/postgres/report/:dataset/test'
)
def test_search_exact_non_string(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

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


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_exact_multiple_props(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?eq(lower(status),invalid)&eq(lower(report_type),stv)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_exact_same_prop_multiple_times(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?eq(lower(status),invalid)&eq(lower(status),ok)')
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
    resp = app.get(f'/{model}?count=gt=40&eq(lower(report_type),vmi)')
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
    ids = RowIds(_push_test_data(app, model))
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?gt(recurse(create_date),2019-04-19)')
    assert ids(resp) == [1]


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
    resp = app.get(f'/{model}?count=ge=40&eq(lower(report_type),vmi)')
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
    resp = app.get(f'/{model}?count=lt=50&eq(lower(report_type),vmi)')
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
    resp = app.get(f'/{model}?count=le=50&eq(lower(report_type),vmi)')
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


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))

    # single field search
    resp = app.get(f'/{model}?status=ne=invalid')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_lower(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # single field search, case insensitive
    resp = app.get(f'/{model}?ne(lower(status),ok)')
    assert ids(resp) == [1, 2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_multiple_props(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count=ne=10&count=ne=42')
    assert ids(resp) == [2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_multiple_props_and_logic(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?ne(lower(status),ok)&eq(lower(report_type),stv)')
    assert ids(resp) == [2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_nested(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # test `ne` with nested structure
    resp = app.get(f'/{model}?ne(notes.create_date,2019-02-01)&status=ne=invalid')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_nested_missing_data(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # test `ne` with nested structures and not full data in all resources
    resp = app.get(f'/{model}?ne(operating_licenses.license_types,valid)')
    assert ids(resp) == [1]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains(model, context, app, mocker):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?contains(lower(report_type),vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains_case_insensitive(model, context, app, mocker):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    # single field search, case insensitive
    resp = app.get(f'/{model}?contains(lower(report_type),vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains_multi_field(model, context, app, mocker):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?contains(status,valid)&contains(lower(report_type),tv)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?contains(status,valid)&contains(report_type,TV)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field search
    # test if operators are joined with AND logic for same field
    resp = app.get(f'/{model}?contains(lower(report_type),vm)&contains(lower(report_type),mi)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?contains(status,valid)&eq(lower(report_type),vmi)')
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
    resp = app.get(f'/{model}?contains(lower(report_type),vm)&select(count)')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0] == {
        'count': 42,
    }

    # `contains` with select and always_show_id
    mocker.patch.object(context.get('config'), 'always_show_id', True)
    resp = app.get(f'/{model}?contains(lower(report_type),vm)&select(count)')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0] == {
        '_id': r2['_id'],
        'count': 42,
    }

    # `contains` with always_show_id should return just id
    resp = app.get(f'/{model}?contains(lower(report_type),vm)')
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
    resp = app.get(f'/{model}?startswith(report_type,VM)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # single field search, case insensitive
    resp = app.get(f'/{model}?startswith(lower(report_type),vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?startswith(status,in)&startswith(lower(report_type),vm)')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?startswith(lower(report_type),st)&eq(lower(status),ok)')
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
    resp = app.get(f'/{model}?eq(lower((notes,note)),foo bar)')
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


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested_contains(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?contains(operating_licenses.license_types,lid)')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested_startswith(model, context, app):
    app.authmodel(model, ['search'])
    r1, r2, r3, = _push_test_data(app, model)

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
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_or(model, context, app):
    r1, r2, r3, = ids(_push_test_data(app, model))
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?or(eq(count,42),eq(lower(status),ok))')
    assert ids(resp) == [r1, r2]

    resp = app.get(f'/{model}?or(le(count,10),eq(count,13))')
    assert ids(resp) == [r1, r3]


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
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested_recurse_lower(model, context, app):
    r1, r2, r3, = ids(_push_test_data(app, model))
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?eq(lower(recurse(status)),ok)')
    assert ids(resp) == [r1]


@pytest.mark.models(
    'backends/mongo/recurse',
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


@pytest.mark.models(
    'backends/mongo/recurse',
    'backends/postgres/recurse',
)
def test_search_recurse_multiple_props_lower(model, app):
    r1, r2, = ids(_push_test_data(app, model, [
        {
            'title': "Org",
            'country': 'fi',
            'govids': [
                {'govid': '1', 'country': 'FI'},
                {'govid': '2', 'country': 'SE'},
            ]
        },
        {
            'title': "Org",
            'country': 'no',
            'govids': [
                {'govid': '3', 'country': 'NO'},
            ]
        },
    ]))
    app.authmodel(model, ['search'])

    resp = app.get(f'/{model}?eq(lower(recurse(country)),se)')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?eq(lower(recurse(country)),fi)')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?eq(lower(recurse(country)),no)')
    assert ids(resp) == [r2]


# TODO: add mongo
def test_search_any(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(eq,count,10,42)')
    assert ids(resp) == [0, 1]

    resp = app.get(f'/{model}?any(ne,count,42)')
    assert ids(resp) == [0, 2]


# TODO: add mongo
def test_search_any_in_list(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(eq,notes.note,hello,world)')
    assert sorted(ids(resp)) == [0, 1]

    resp = app.get(f'/{model}?any(ne,notes.note,foo bar)')
    assert sorted(ids(resp)) == [0, 1]


# TODO: add mongo
def test_search_any_in_list_of_scalars(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(eq,operating_licenses.license_types,valid,invalid,expired)')
    assert sorted(ids(resp)) == [0, 1]

    resp = app.get(f'/{model}?any(ne,operating_licenses.license_types,expired)')
    assert sorted(ids(resp)) == [0]


# TODO: add mongo
def test_search_any_recurse(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(eq,recurse(status),OK,none)')
    assert ids(resp) == [0]


# TODO: add mongo
def test_search_any_recurse_lower(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(eq,lower(recurse(status)),ok,none)')
    assert ids(resp) == [0]


# TODO: add mongo
def test_search_any_contains(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(contains,status,inv,val,lid)')
    assert sorted(ids(resp)) == [1, 2]


# TODO: add mongo
def test_search_any_contains_nested(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(contains,notes.note,hel,wor)')
    assert sorted(ids(resp)) == [0, 1]


# TODO: add mongo
def test_search_any_contains_recurse_lower(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any(contains,lower(recurse(status)),o,k)')
    assert sorted(ids(resp)) == [0]
