import pytest

from spinta.testing.utils import get_error_codes, get_error_context


# FIXME: fix postgres
# @pytest.mark.models(
#     'backends/postgres/report',
#     'backends/mongo/report',
# )
@pytest.mark.models(
    'backends/mongo/report',
    'backends/postgres/report',
)
def test_report(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',
            'note_type': 'test',
            'create_date': '2019-04-20',
        }],
        'operating_licenses': [],
    })
    assert resp.status_code == 201

    data = resp.json()
    id = data['_id']
    resp = app.get(f'/{model}/{id}')
    assert resp.status_code == 200

    data = resp.json()
    assert data == {
        '_id': id,
        '_revision': data['_revision'],
        '_type': model,
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20T03:14:15',
        'notes': [{
            'note': 'hello report',
            'note_type': 'test',
            'create_date': '2019-04-20',
        }],
        'operating_licenses': [],
    }


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_invalid_report_int(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'report_type': 'simple',
        'status': 'valid',
        'count': 'c0unt',  # invalid conversion to int
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # ignore schema manifest path in error context as
    # it's problematic to get path in the CI tests
    assert get_error_context(
        resp.json(),
        "InvalidValue",
        ["manifest", "model", "property", "type"],
    ) == {
        'manifest': 'default',
        'model': model,
        'property': 'count',
        'type': 'integer',
    }


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_invalid_report_date(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'report_type': 'simple',
        'status': 'valid',
        'valid_from_date': '2019-04',  # invalid conversion to date
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_non_string_report_date(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'valid_from_date': 42,  # invalid conversion to date
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_invalid_report_datetime(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'update_time': '2019-04',  # invalid conversion to datetime
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_non_string_report_datetime(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'update_time': 42,  # invalid conversion to datetime
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_invalid_report_array(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'notes': {'foo': 'bar'},  # invalid conversion to array
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_invalid_report_array_object(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'report_type': 'simple',
        'status': 'valid',
        'notes': ['hello', 'world'],  # invalid array item type
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_invalid_nested_object_property(model, app):
    app.authmodel(model, ['insert'])

    resp = app.post(f'/{model}', json={
        'notes': [{
            'note': 42  # invalid object property type
        }]
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_missing_report_object_property(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',  # missing object properties
        }],
    })

    assert resp.status_code == 201


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_unknown_report_property(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'report_type': 'simple',
        'status': 'valid',
        'count': '42',
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',
            'note_type': 'test',
            'create_date': '2014-20',
        }],
        'random_prop': 'foo',
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]
    assert get_error_context(
        resp.json(),
        "FieldNotInResource",
        ["property", "model"]
    ) == {
        'property': 'random_prop',
        'model': model,
    }


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_unknown_report_object_property(model, app):
    app.authmodel(model, ['insert', 'getone'])

    resp = app.post(f'/{model}', json={
        '_type': model,
        'report_type': 'simple',
        'status': 'valid',
        'count': 42,
        'valid_from_date': '2019-04-20',
        'update_time': '2019-04-20 03:14:15',
        'notes': [{
            'note': 'hello report',
            'rand_prop': 42,  # unknown object properties
        }],
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["FieldNotInResource"]
    assert get_error_context(
        resp.json(),
        "FieldNotInResource",
        ["property", "model"]
    ) == {
        'property': 'notes.rand_prop',
        'model': model,
    }
