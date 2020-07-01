import pytest
import requests

from spinta.utils.data import take
from spinta.testing.utils import error
from spinta.testing.utils import get_error_codes, RowIds
from spinta.testing.context import create_test_context
from spinta.testing.client import create_test_client
from spinta.testing.tabular import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.data import listdata


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


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_exact(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?status="OK"')
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
    resp = app.get(f'/{model}?status.lower()="ok"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
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
    resp = app.get(f'/{model}?count="abc"')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # single non-existing field value search
    resp = app.get(f'/{model}?status="o"')
    data = resp.json()['_data']
    assert len(data) == 0

    # single non-existing field search
    resp = app.get(f'/{model}?state="o"')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_exact_multiple_props(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?status.lower()="invalid"&report_type.lower()="stv"')
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
    resp = app.get(f'/{model}?status.lower()="invalid"&status.lower()="ok"')
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
    resp = app.get(f'/{model}?count>40')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # search for string value
    resp = app.get(f'/{model}?status>"ok"')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count>40&count>10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count>40&report_type.lower()="vmi"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `greater_than` works as expected
    resp = app.get(f'/{model}?count>42')
    data = resp.json()['_data']
    assert len(data) == 0


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_gt_with_nested_date(model, context, app):
    ids = RowIds(_push_test_data(app, model))
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?recurse(create_date)>"2019-04-19"')
    assert ids(resp) == [1]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_gte(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?count>=40')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # search for string value
    resp = app.get(f'/{model}?status>="ok"')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count>=40&count>10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count>=40&report_type.lower()="vmi"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `greater_than` works as expected
    resp = app.get(f'/{model}?count>=42')
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
    resp = app.get(f'/{model}?recurse(create_date)>="2019-04-20"')
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
    resp = app.get(f'/{model}?count<12')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # search for string value
    resp = app.get(f'/{model}?status<"ok"')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count<20&count>10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count<50&report_type.lower()="vmi"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `lower_than` works as expected
    resp = app.get(f'/{model}?count<10')
    data = resp.json()['_data']
    assert len(data) == 0


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_lt_with_nested_date(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?recurse(create_date)<"2019-02-02"')
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
    resp = app.get(f'/{model}?count<=12')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # search for string value
    resp = app.get(f'/{model}?status<="ok"')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count<=20&count>10')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?count<=50&report_type.lower()="vmi"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # test `lower_than` works as expected
    resp = app.get(f'/{model}?count<=10')
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
    resp = app.get(f'/{model}?recurse(create_date)<="2019-02-01"')
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
    resp = app.get(f'/{model}?status!="invalid"')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_lower(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # single field search, case insensitive
    resp = app.get(f'/{model}?status.lower()!="ok"')
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
    resp = app.get(f'/{model}?count!=10&count!=42')
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
    resp = app.get(f'/{model}?status.lower()!="ok"&report_type.lower()="stv"')
    assert ids(resp) == [2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_nested(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # test `ne` with nested structure
    resp = app.get(f'/{model}?notes.create_date!="2019-02-01"&status!="invalid"')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_ne_nested_missing_data(model, context, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    # test `ne` with nested structures and not full data in all resources
    resp = app.get(f'/{model}?operating_licenses.license_types!="valid"')
    assert ids(resp) == [1]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains(model, context, app, mocker):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?report_type.lower().contains("vm")')
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
    resp = app.get(f'/{model}?report_type.lower().contains("vm")')
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
    resp = app.get(f'/{model}?status.contains("valid")&report_type.lower().contains("tv")')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?status.contains("valid")&report_type.contains("TV")')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # multi field search
    # test if operators are joined with AND logic for same field
    resp = app.get(f'/{model}?report_type.lower().contains("vm")&report_type.lower().contains("mi")')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?status.contains("valid")&report_type.lower()="vmi"')
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
    resp = app.get(f'/{model}?recurse(create_date).contains("2019-04-20")')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_contains_with_select(model, context, app, mocker):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])

    # `contains` with select
    resp = app.get(f'/{model}?report_type.lower().contains("vm")&select(count)')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0] == {
        'count': 42,
    }

    # `contains` with select and always_show_id
    mocker.patch.object(context.get('config'), 'always_show_id', True)
    resp = app.get(f'/{model}?report_type.lower().contains("vm")&select(count)')
    assert resp.status_code == 200
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0] == {
        '_id': r2['_id'],
        'count': 42,
    }

    # `contains` with always_show_id should return just id
    resp = app.get(f'/{model}?report_type.lower().contains("vm")')
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
def test_select_unknown_property(model, context, app, mocker):
    _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?select(nothere)')
    assert error(resp) == 'FieldNotInResource'


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_select_unknown_property_in_object(model, context, app, mocker):
    _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?select(notes.nothere)')
    assert error(resp) == 'FieldNotInResource'


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_startswith(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # single field search
    resp = app.get(f'/{model}?report_type.startswith("VM")')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # single field search, case insensitive
    resp = app.get(f'/{model}?report_type.lower().startswith("vm")')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?status.startswith("in")&report_type.lower().startswith("vm")')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # multi field and multi operator search
    # test if operators are joined with AND logic
    resp = app.get(f'/{model}?report_type.lower().startswith("st")&status.lower()="ok"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # sanity check that `startswith` searches from the start
    resp = app.get(f'/{model}?status.startswith("valid")')
    data = resp.json()['_data']
    assert len(data) == 0

    # `startswith` type check
    resp = app.get(f'/{model}?notes.create_date.startswith("2019-04-20")')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)

    app.authmodel(model, ['search'])

    # nested `exact` search
    resp = app.get(f'/{model}?notes.note="foo bar"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # nested `exact` search, case insensitive
    resp = app.get(f'/{model}?notes.note.lower()="foo bar"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    # nested `exact` search with dates
    resp = app.get(f'/{model}?notes.create_date="2019-03-14"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r1['_id']

    # nested `gt` search
    resp = app.get(f'/{model}?notes.create_date>"2019-04-01"')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r2['_id']

    # nested non existant field
    resp = app.get(f'/{model}?notes.foo.bar="baz"')
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]

    # nested `contains` search
    resp = app.get(f'/{model}?notes.note.contains("bar")')
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
    resp = app.get(f'/{model}?operating_licenses.license_types.contains("lid")')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested_startswith(model, context, app):
    app.authmodel(model, ['search'])
    r1, r2, r3, = _push_test_data(app, model)

    # nested `startswith` search
    resp = app.get(f'/{model}?notes.note.startswith("fo")')
    data = resp.json()['_data']
    assert len(data) == 1
    assert data[0]['_id'] == r3['_id']

    resp = app.get(f'/{model}?operating_licenses.license_types.startswith("exp")')
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
    ids = RowIds(_push_test_data(app, model))
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?count=42|status.lower()="ok"')
    assert ids(resp) == [0, 1]

    resp = app.get(f'/{model}?count<=10|count=13')
    assert ids(resp) == [0, 2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_nested_recurse(model, context, app):
    r1, r2, r3, = _push_test_data(app, model)
    app.authmodel(model, ['search'])
    resp = app.get(f'/{model}?recurse(note)="foo bar"')
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
    resp = app.get(f'/{model}?recurse(status).lower()="ok"')
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

    resp = app.get(f'/{model}?recurse(country)="se"')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?recurse(country)="fi"')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?recurse(country)="no"')
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

    resp = app.get(f'/{model}?recurse(country).lower()="se"')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?recurse(country).lower()="fi"')
    assert ids(resp) == [r1]

    resp = app.get(f'/{model}?recurse(country).lower()="no"')
    assert ids(resp) == [r2]


# TODO: add mongo
def test_search_any(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("eq",count,10,42)')
    assert ids(resp) == [0, 1]

    resp = app.get(f'/{model}?any("ne",count,42)')
    assert ids(resp) == [0, 2]


# TODO: add mongo
def test_search_any_in_list(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("eq",notes.note,"hello","world")')
    assert sorted(ids(resp)) == [0, 1]

    resp = app.get(f'/{model}?any("ne",notes.note,"foo bar")')
    assert sorted(ids(resp)) == [0, 1]


# TODO: add mongo
def test_search_any_in_list_of_scalars(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("eq",operating_licenses.license_types,"valid","invalid","expired")')
    assert sorted(ids(resp)) == [0, 1]

    resp = app.get(f'/{model}?any("ne",operating_licenses.license_types,"expired")')
    assert sorted(ids(resp)) == [0]


# TODO: add mongo
def test_search_any_recurse(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("eq",recurse(status),"OK","none")')
    assert ids(resp) == [0]


# TODO: add mongo
def test_search_any_recurse_lower(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("eq",recurse(status).lower(),"ok","none")')
    assert ids(resp) == [0]


# TODO: add mongo
def test_search_any_contains(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("contains",status,"inv","val","lid")')
    assert sorted(ids(resp)) == [1, 2]


# TODO: add mongo
def test_search_any_contains_nested(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("contains",notes.note,"hel","wor")')
    assert sorted(ids(resp)) == [0, 1]


# TODO: add mongo
def test_search_any_contains_recurse_lower(app):
    model = 'backends/postgres/report'
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?any("contains",recurse(status).lower(),"o","k")')
    assert sorted(ids(resp)) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_id_contains(model, app):
    app.authmodel(model, ['search', 'getall'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?_id.contains("-")')
    assert sorted(ids(resp)) == [0, 1, 2]

    subid = ids[0][5:10]
    resp = app.get(f'/{model}?_id.contains("{subid}")')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_id_not_contains(model, app):
    app.authmodel(model, ['search', 'getall'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?_id.contains("AAAAA")')
    assert ids(resp) == []


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_id_startswith(model, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    subid = ids[0][:5]
    resp = app.get(f'/{model}?_id.startswith("{subid}")')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_id_not_startswith(model, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    subid = ids[0][5:10]
    resp = app.get(f'/{model}?_id.startswith("{subid}")')
    assert ids(resp) == []


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_revision_contains(model, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?_revision.contains("-")')
    assert sorted(ids(resp)) == [0, 1, 2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_revision_startswith(model, app):
    app.authmodel(model, ['search', 'getone'])
    ids = RowIds(_push_test_data(app, model))
    id0 = ids[0]
    resp = app.get(f'/{model}/{id0}')
    revision = resp.json()['_revision'][:5]
    resp = app.get(f'/{model}?_revision.startswith("{revision}")')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_group(model, app):
    app.authmodel(model, ['search', 'getone'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?(report_type="STV"&status="OK")')
    assert ids(resp) == [0]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_select_in_or(model, app):
    app.authmodel(model, ['search', 'getone'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?(report_type="STV"|status="OK")&select(_id)')
    # XXX: Flaky test, some times it gives [2, 0], don't know why.
    assert ids(resp) == [0, 2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_lower_contains(model, app):
    app.authmodel(model, ['search', 'getone'])
    ids = RowIds(_push_test_data(app, model))
    resp = app.get(f'/{model}?report_type.lower().contains("st")')
    assert ids(resp) == [0, 2]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_null(model, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model, [
        {'status': 'OK'},
        {},
    ]))
    resp = app.get(f'/{model}?status=null')
    assert ids(resp) == [1]


@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_search_not_null(model, app):
    app.authmodel(model, ['search'])
    ids = RowIds(_push_test_data(app, model, [
        {'status': 'OK'},
        {},
    ]))
    resp = app.get(f'/{model}?status!=null')
    assert ids(resp) == [0]


@pytest.mark.parametrize('backend', ['default', 'mongo'])
def test_extra_fields(postgresql, mongo, backend, rc, tmpdir, request):
    rc = rc.fork({
        'backends': [backend],
        'manifests.default': {
            'type': 'tabular',
            'path': str(tmpdir / 'manifest.csv'),
            'backend': backend,
        },
    })

    # Create data into a extrafields model with code and name properties.
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    m | property | type
    extrafields  |
      | code     | string
      | name     | string
    '''))
    context = create_test_context(rc)
    request.addfinalizer(context.wipe_all)
    app = create_test_client(context)
    app.authmodel('extrafields', ['insert'])
    resp = app.post('/extrafields', json={'_data': [
        {'_op': 'insert', 'code': 'lt', 'name': 'Lietuva'},
        {'_op': 'insert', 'code': 'lv', 'name': 'Latvija'},
        {'_op': 'insert', 'code': 'ee', 'name': 'Estija'},
    ]})
    assert resp.status_code == 200, resp.json()

    # Now try to read from same model, but loaded with just one property.
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    m | property | type
    extrafields  |
      | name     | string
    '''))
    context = create_test_context(rc)
    app = create_test_client(context)
    app.authmodel('extrafields', ['getall', 'getone'])
    resp = app.get('/extrafields')
    assert listdata(resp, sort=True) == [
        "Estija",
        "Latvija",
        "Lietuva",
    ]

    pk = resp.json()['_data'][0]['_id']
    resp = app.get(f'/extrafields/{pk}')
    data = resp.json()
    assert resp.status_code == 200, data
    assert take(data) == {'name': 'Lietuva'}


@pytest.mark.parametrize('backend', ['mongo'])
def test_missing_fields(postgresql, mongo, backend, rc, tmpdir):
    rc = rc.fork({
        'backends': [backend],
        'manifests.default': {
            'type': 'tabular',
            'path': str(tmpdir / 'manifest.csv'),
            'backend': backend,
        },
    })

    # Create data into a extrafields model with code and name properties.
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    m | property  | type
    missingfields |
      | code      | string
    '''))
    context = create_test_context(rc)
    app = create_test_client(context)
    app.authmodel('missingfields', ['insert'])
    resp = app.post('/missingfields', json={'_data': [
        {'_op': 'insert', 'code': 'lt'},
        {'_op': 'insert', 'code': 'lv'},
        {'_op': 'insert', 'code': 'ee'},
    ]})
    assert resp.status_code == 200, resp.json()

    # Now try to read from same model, but loaded with just one property.
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    m | property  | type
    missingfields |
      | code      | string
      | name      | string
    '''))
    context = create_test_context(rc)
    app = create_test_client(context)
    app.authmodel('missingfields', ['search', 'getone'])
    resp = app.get('/missingfields?select(_id,code,name)')
    assert listdata(resp, sort=True) == [
        ('ee', None),
        ('lt', None),
        ('lv', None),
    ]

    pk = resp.json()['_data'][0]['_id']
    resp = app.get(f'/missingfields/{pk}')
    data = resp.json()
    assert resp.status_code == 200, data
    assert take(data) == {'code': 'lt'}
