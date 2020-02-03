import pytest

from spinta.utils.schema import NA
from spinta.testing.dtypes import path, post, put, patch, get, search


@pytest.mark.models(
    'backends/mongo/dtypes/object',
    'backends/postgres/dtypes/object',
    'backends/mongo/dtypes/array/object',
    'backends/postgres/dtypes/array/object',
)
def test_update_empty(model, app):
    app.authmodel(model, ['insert', 'update', 'getone', 'search'])
    pk, rev0, val = post(app, model, {'string': 'old'})
    rev1, val = put(app, model, pk, rev0, {})
    assert val == {'string': None}
    assert rev0 != rev1
    assert get(app, model, pk, rev1) == val
    name = path(model + '/string')
    assert search(app, model, pk, rev1, 'old', by=name) == []
    assert search(app, model, pk, rev1, None, by=name) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/object',
    'backends/postgres/dtypes/object',
)
def test_patch_empty(model, app):
    app.authmodel(model, ['insert', 'patch', 'getone', 'search'])
    pk, rev0, val = post(app, model, {'string': 'old'})
    rev1, val = patch(app, model, pk, rev0, {})
    assert val is NA
    assert rev0 == rev1
    assert get(app, model, pk, rev1) == {'string': 'old'}
    name = path(model + '/string')
    assert search(app, model, pk, rev1, 'old', by=name) == [{'string': 'old'}]
    assert search(app, model, pk, rev1, None, by=name) == []


@pytest.mark.models(
    'backends/mongo/dtypes/array/object',
    'backends/postgres/dtypes/array/object',
)
def test_patch_empty_array(model, app):
    app.authmodel(model, ['insert', 'patch', 'getone', 'search'])
    pk, rev0, val = post(app, model, {'string': 'old'})
    rev1, val = patch(app, model, pk, rev0, {})
    assert val == {'string': None}
    assert rev0 != rev1
    assert get(app, model, pk, rev1) == {'string': None}
    name = path(model + '/string')
    assert search(app, model, pk, rev1, 'old', by=name) == []
    assert search(app, model, pk, rev1, None, by=name) == [{'string': None}]
